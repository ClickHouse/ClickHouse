from ci.praktika.info import Info
from ci.praktika.utils import Shell, Utils


def kill_leftover_server_processes():
    """Kill ClickHouse server processes leaked by a previous CI job.

    Runners are reused across jobs, and a job that is cancelled or times out
    can leave its clickhouse-server running or slowly shutting down. Such a
    leftover keeps the server ports, so the next job's server fails every
    listen with `Address already in use` and its `wait_ready` times out with
    an opaque `Connection refused`.

    Call this from every entry point that starts the job's own server on the
    host, right before the first start. At that moment the current job has not
    started any server yet, so anything matching here is a leftover from a
    previous job and is stale by definition. Never runs locally: a local run
    may share the machine with a developer's own server that must not be
    touched.
    """
    if Info().is_local_run:
        return
    # The bracket in the pattern also keeps pkill from matching the shell that
    # runs it: the literal `clickhouse[- ]server` in that shell's own cmdline
    # does not match the regex it denotes.
    pattern = "clickhouse[- ]server"
    leftovers = Shell.get_output(f"pgrep -a -f '{pattern}'").strip()
    if not leftovers:
        return
    print(
        "WARNING: killing leftover clickhouse server processes"
        f" from a previous job:\n{leftovers}"
    )
    Shell.check(f"pkill -9 -f '{pattern}'", verbose=True)
    for _ in range(10):
        if not Shell.get_output(f"pgrep -f '{pattern}'").strip():
            break
        Utils.sleep(1)
    else:
        print("WARNING: leftover clickhouse server processes survived SIGKILL")
    Info().add_workflow_warning(
        "Leftover clickhouse server processes from a previous job"
        " were found and killed, see job.log"
    )
