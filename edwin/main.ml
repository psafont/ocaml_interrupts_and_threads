let defer f = Fun.protect ~finally:f

let with_mutex lock f =
  Mutex.lock lock;
  defer (fun () -> Mutex.unlock lock) f

module Xenops_task = struct
  let with_cancel _t _cancel_fn f = f ()

  let check_cancelling _t = ()
end

exception Ioemu_failed of (string * string)

type domid = int

type service = {
  name : string;
  domid : domid;
  exec_path : string;
  pid_path : string;
  cancel_path : string;
  timeout_seconds : float;
  args : string list;
  execute : path:string -> args:string list -> domid:domid -> unit -> string;
}

let service_alive _service = true

type watch_trigger = Finished | Cancelled | Empty

let pp_trigger fmt = function
  | Finished -> Format.pp_print_string fmt "Finished"
  | Cancelled -> Format.pp_print_string fmt "Cancelled"
  | Empty -> Format.pp_print_string fmt "Empty"

exception ECancelled of int

let raise_e = function e -> raise e

(** File-descriptor event monitor implementation for the epoll library *)
module Monitor = struct
  let create () = Polly.create ()

  let add m fd = Polly.add m fd Polly.Events.inp

  let remove m fd = Polly.del m fd

  let close m = Polly.close m
end

let with_inotify f =
  let fd = Inotify.create () in
  defer (fun () -> Unix.close fd) (fun () -> f fd)

let with_watch notifd dir f =
  let open Inotify in
  let flags = [ S_Create; S_Delete_self; S_Onlydir ] in
  let watch = Inotify.add_watch notifd dir flags in
  defer
    (fun () ->
      try Inotify.rm_watch notifd watch
      with e ->
        Printf.printf "Error when closing watch for %s: %s\n" dir
          (Printexc.to_string e))
    (fun () -> f watch)

let with_monitor watch_fd f =
  let fd = Monitor.create () in
  Monitor.add fd watch_fd;
  defer
    (fun () ->
      Monitor.remove fd watch_fd;
      Monitor.close fd)
    (fun () -> f fd)

let fold_events ~init f events =
  events |> List.to_seq
  |> ( Seq.flat_map @@ fun (_, events, _, fnameopt) ->
       List.to_seq events |> Seq.map @@ fun event -> (event, fnameopt) )
  |> Seq.fold_left f init

let start_service_and_wait_for_readyness ~task ~service =
  with_inotify @@ fun notifd ->
  with_monitor notifd @@ fun pollfd ->
  with_watch notifd (Filename.dirname service.pid_path) @@ fun _ ->
  let wait ~task ~pid_path ~cancel_path ~for_s ~service_name =
    let start_time = Mtime_clock.elapsed () in
    let poll_period_ms = 1000 in
    let event = ref Empty in
    let cancel () =
      let fd =
        Unix.openfile pid_path [ O_WRONLY; O_CREAT; O_EXCL; O_SYNC ] 0o200
      in
      let _ = Unix.write fd (Bytes.of_string "") 0 0 in
      Unix.close fd;
      Unix.unlink pid_path
    in
    let collect_watches acc (event, file) =
      match (acc, event, file) with
      | Cancelled, _, _ | _, Inotify.Delete_self, _ -> Cancelled
      | _, Inotify.Create, Some file when file = Filename.basename cancel_path
        ->
          Cancelled
      | _, Inotify.Create, Some file when file = Filename.basename pid_path ->
          Finished
      | Finished, _, _ -> Finished
      | Empty, _, _ -> Empty
    in

    let cancellable_watch () =
      let rec poll_loop () =
        Printf.printf "Polling...\n%!";
        try
          ignore
          @@ Polly.wait pollfd 1 poll_period_ms (fun _ fd events ->
                 if Polly.Events.(test events inp) then
                   event :=
                     fold_events ~init:!event collect_watches (Inotify.read fd));

          let current_time = Mtime_clock.elapsed () in
          let elapsed_time =
            Mtime.Span.(to_s (abs_diff start_time current_time))
          in

          Printf.printf "elapsed %f\n" elapsed_time;
          Format.printf "created: %a\n%!" pp_trigger !event;

          match !event with
          | Empty when elapsed_time < for_s -> poll_loop ()
          | Finished -> Ok ()
          | Cancelled -> Error (ECancelled task)
          | Empty ->
              let err_msg =
                if service_alive service_name then
                  "Timeout reached while starting service"
                else "Service exited unexpectedly"
              in
              Error (Ioemu_failed (service_name, err_msg))
        with e ->
          let err_msg =
            Printf.sprintf
              "Exception while waiting for service %s to be ready: %s"
              service_name (Printexc.to_string e)
          in
          Error (Failure err_msg)
      in

      Xenops_task.with_cancel task cancel poll_loop
    in
    cancellable_watch ()
  in

  (* start systemd service *)
  let syslog_key =
    service.execute ~path:service.exec_path ~args:service.args
      ~domid:service.domid ()
  in

  Xenops_task.check_cancelling task;

  (* wait for pidfile to appear *)
  Result.iter_error raise_e
    (wait ~task ~pid_path:service.pid_path ~cancel_path:service.cancel_path
       ~for_s:service.timeout_seconds ~service_name:syslog_key);

  Printf.sprintf "Service %s initialized" syslog_key

let () =
  let pid_path = "./pid" in
  let cancel_path = "./cancel" in
  let execute ~path:_ ~args:_ ~domid:_ () =
    (*
    let fd =
      Unix.openfile cancel_path [ O_WRONLY; O_CREAT; O_EXCL; O_SYNC ] 0o200
    in
    let _ = Unix.write fd (Bytes.of_string "") 0 0 in
    Unix.close fd;
    Unix.unlink cancel_path;
    *)
    "dummy"
  in
  let service =
    {
      name = "test";
      domid = 0;
      exec_path = "/opt/bin/exec";
      pid_path;
      cancel_path;
      timeout_seconds = 10.;
      args = [];
      execute;
    }
  in
  ignore @@ start_service_and_wait_for_readyness ~task:0 ~service
