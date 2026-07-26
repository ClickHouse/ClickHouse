"""
Regression test for ClickHouseProc.stop_server (ci/jobs/scripts/clickhouse_proc.py)
stopping the server process itself rather than the `sh -c ...` wrapper above it.

Background
----------
The servers are started with `subprocess.Popen(command, shell=True)`, so
`Popen.pid` is the `/bin/sh -c ...` wrapper, and `BaseDaemon::setupWatchdog`
forks once more below it - the server is the wrapper's grandchild, and the only
handle on it is the pid it writes to its own `--pid-file`.

When `clickhouse stop` failed to stop a server gracefully, `stop_server` sent
`SIGTRAP` to `proc` (the wrapper) and then, on timeout, `proc.kill()`. Both hit
the shell:

* the TRAP dumped a useless `core.sh.<pid>` instead of a core of the wedged
  server, defeating the whole point of the "send TRAP signal to generate core
  file" branch and uploading a junk core as a job artifact;
* the shell died at once, so `proc.wait(timeout=10)` returned and `stop_server`
  moved on while the *server* kept running.

The orphaned server keeps its exclusive lock on `<run_path>/status`, so the
"Collect logs" phase, which scrapes `system.*_log` with `clickhouse local --path
<run_path>`, fails for that replica with `Code: 76 ... Cannot lock file
<run_path>/status. Another server instance in same directory is already running.
(CANNOT_OPEN_FILE)`, and every system table of that replica is lost from the
report. Observed on master 3401bc9cdfa1187d229dc4f84ba3aa380bec4f0b, `Stateless
tests (amd_llvm_coverage, old analyzer, s3 storage, DBReplicated, WasmEdge,
parallel, 2/3)`: replicas 1 and 2 (pids 802 and 952) survived `stop_server`,
which had TRAPped their wrappers (pids 792 and 943 - hence the `core.sh.792-*`
and `core.sh.943-*` artifacts), and all 11 scraped system tables failed for both
replicas.

The tests drive the real `stop_server` against a fake server built to have the
same process topology, and assert what the fix has to guarantee: the process
named by the pid file is gone and the data-directory lock is free by the time
`stop_server` returns. Both would fail on the pre-fix code, which left the
server running.
"""

import os
import signal
import subprocess
import time

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.utils import Shell

# The fake server must be named `clickhouse-server`: `_server_process_alive`
# identifies the server by its command line so the harness never signals a pid
# the kernel has already recycled for something else.
#
# Invoked without `--server` it plays the watchdog half of
# `BaseDaemon::setupWatchdog`: fork, and let the child be the real server. So
# `Popen(shell=True)` on it produces the same three-process chain as in CI -
# `sh -c ...` wrapper -> watchdog -> server - with only the server's pid in the
# pid file.
#
# `$1` is the pid file, `$2` the `status` file to lock, `$3` the extra signals to
# ignore on top of TERM (which a server wedged past `clickhouse stop` ignores in
# effect); passing TRAP forces stop_server's SIGKILL escalation.
_FAKE_SERVER = """#!/bin/sh
if [ "$1" = "--server" ]; then
    shift
    trap '' TERM $3
    # Hold the data directory lock that the scraping `clickhouse local` needs.
    # The pid file is written only once the lock is held, so a test that sees
    # the pid file can rely on the lock being taken.
    exec 9>>"$2"
    flock --exclusive 9
    echo $$ > "$1"
    # `9>&-` keeps each `sleep` from inheriting the locked descriptor: an flock
    # lives on the open file description, so a child holding a copy would keep
    # the lock after this process dies and make the assertions race. The real
    # server has no such child - `setupWatchdog` forks before the status file is
    # created - so this only removes an artifact of the fake.
    while true; do sleep 0.1 9>&-; done
fi
"$0" --server "$1" "$2" "$3" &
wait
"""


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _start_fake_server(tmp_path, name="clickhouse-server", ignore_signals="TERM"):
    """Start wrapper shell -> watchdog -> "server", as the CI harness does.

    Returns `(proc, pid)`, the pair `stop_server` iterates over: `proc` is the
    `Popen` handle on the wrapper, `pid` is what the server wrote to its pid file.
    """
    server = tmp_path / name
    server.write_text(_FAKE_SERVER)
    server.chmod(0o755)
    # Name the pid file after the binary: `_server_process_alive` matches the
    # whole command line, so a hardcoded `clickhouse-server.pid` argument would
    # make even a non-ClickHouse process look like the server.
    pid_file = tmp_path / f"{name}.pid"
    status_file = tmp_path / "status"
    proc = subprocess.Popen(
        f"{server} {pid_file} {status_file} {ignore_signals}",
        shell=True,
        cwd=tmp_path,
    )
    deadline = time.monotonic() + 60
    while not pid_file.exists() or not pid_file.read_text().strip():
        assert time.monotonic() < deadline, "fake server did not write its pid file"
        time.sleep(0.05)
    pid = int(pid_file.read_text().strip())
    # Guard the premise of the whole fix: the handle the harness keeps is not the
    # server. If a future shell exec'd the command instead of forking, signalling
    # `proc` would reach the server and these tests would pass vacuously.
    assert proc.pid != pid, (
        f"Popen.pid ({proc.pid}) equals the server pid ({pid}); the topology "
        "under test no longer reproduces the bug"
    )
    return proc, pid


def _cleanup(proc, pid):
    for target in (pid, proc.pid):
        try:
            os.kill(target, signal.SIGKILL)
        except OSError:
            pass
    try:
        proc.wait(timeout=30)
    except subprocess.TimeoutExpired:
        pass


def _make_proc(monkeypatch, tmp_path, proc, pid, trap_timeout=10):
    """A ClickHouseProc wired to one fake server, with the graceful stop failing.

    `stop_server` only reaches the branch under test when `clickhouse stop` could
    not stop the server, so that call is stubbed out to fail - which is also what
    keeps the test off the real `clickhouse` binary and its 300-try wait.
    """
    original_check = Shell.check

    def check(command, *args, **kwargs):
        if "clickhouse stop" in command:
            return False
        return original_check(command, *args, **kwargs)

    monkeypatch.setattr(Shell, "check", staticmethod(check))

    ch = object.__new__(ClickHouseProc)
    ch.proc, ch.pid_0, ch.run_path0 = proc, pid, tmp_path
    ch.pid_file = tmp_path / "clickhouse-server.pid"
    ch.proc_1 = ch.proc_2 = None
    ch.pid_1 = ch.pid_2 = 0
    ch.pid_file_replica_1 = ch.pid_file_replica_2 = None
    ch.run_path1 = ch.run_path2 = None
    # Keep the core-dump grace period short: the fake server has no core to write.
    ch.TRAP_CORE_DUMP_TIMEOUT = trap_timeout
    return ch


def _status_lock_is_free(tmp_path):
    return (
        subprocess.run(
            f"flock --exclusive --nonblock {tmp_path}/status -c true", shell=True
        ).returncode
        == 0
    )


def test_stop_server_stops_the_server_not_the_wrapper(monkeypatch, tmp_path):
    # The fix: stop_server must leave no server behind holding the data
    # directory. Pre-fix it TRAPped the wrapper, the server survived, and the
    # `status` lock stayed held - which is what broke "Scraping system tables".
    proc, pid = _start_fake_server(tmp_path)
    try:
        assert not _status_lock_is_free(tmp_path), (
            "the fake server is not holding the status lock"
        )
        _make_proc(monkeypatch, tmp_path, proc, pid).stop_server()
        assert not ClickHouseProc._server_process_alive(pid), (
            "the server outlived stop_server"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
        assert not _pid_alive(proc.pid), "the wrapper shell was left behind"
    finally:
        _cleanup(proc, pid)


def test_stop_server_escalates_to_kill_when_the_server_ignores_trap(
    monkeypatch, tmp_path
):
    # A server that does not die from the TRAP (no core dump configured, a
    # handler installed, or one still dumping past the grace period) must be
    # SIGKILLed rather than left running, or the scraping loses this replica.
    proc, pid = _start_fake_server(tmp_path, ignore_signals="TERM TRAP")
    try:
        start = time.monotonic()
        _make_proc(monkeypatch, tmp_path, proc, pid, trap_timeout=3).stop_server()
        elapsed = time.monotonic() - start
        assert not ClickHouseProc._server_process_alive(pid), (
            "a TRAP-ignoring server outlived stop_server"
        )
        assert _status_lock_is_free(tmp_path)
        assert elapsed < 60, (
            f"stop_server took {elapsed:.1f}s; the TRAP grace period is not bounded"
        )
    finally:
        _cleanup(proc, pid)


def test_stop_server_leaves_an_unrelated_process_alone(monkeypatch, tmp_path):
    # The pid file is read once at startup, so by teardown its pid may belong to
    # an unrelated process the kernel has since given it to. stop_server must not
    # kill it - the command-line check in `_server_process_alive` is what stops a
    # `SIGKILL` from going to a random process on the runner.
    proc, pid = _start_fake_server(tmp_path, name="not-a-clickhouse-binary")
    try:
        _make_proc(monkeypatch, tmp_path, proc, pid).stop_server()
        assert _pid_alive(pid), "stop_server killed an unrelated process"
    finally:
        _cleanup(proc, pid)


def test_server_liveness_check_survives_a_long_command_line(tmp_path):
    # `ps` truncates the command line to 80 columns when its output is not a
    # terminal, so a plain `ps -p <pid> -o command=` misses the match for a
    # server started from a long path - and a liveness check that wrongly
    # answers "already gone" skips the KILL and leaves the server running.
    deep = tmp_path / ("d" * 60) / ("e" * 60)
    deep.mkdir(parents=True)
    proc, pid = _start_fake_server(deep)
    try:
        assert len(f"{deep}") > 80, "the path under test is not long enough"
        assert ClickHouseProc._server_process_alive(pid), (
            "the liveness check lost the server to ps command-line truncation"
        )
    finally:
        _cleanup(proc, pid)
