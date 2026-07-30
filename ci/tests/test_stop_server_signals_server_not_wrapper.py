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
import sys
import time

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc
from ci.praktika.utils import Shell

# The fake server first plays the watchdog half of `BaseDaemon::setupWatchdog`:
# fork, and let the child be the real server. So `Popen(shell=True)` on it
# produces the same three-process chain as in CI - `sh -c ...` wrapper ->
# watchdog -> server - with only the server's pid in the pid file.
#
# `argv[1]` is the pid file, `argv[2]` the `status` file to lock, `argv[3]` the
# extra signals to ignore on top of TERM (which a server wedged past `clickhouse
# stop` ignores in effect); passing TRAP forces stop_server's SIGKILL escalation.
#
# It is a Python script rather than a shell one because it has to run under an
# `argv[0]` of our choosing: `_server_process_alive` identifies the server by
# `argv[0]` so the harness never signals a pid the kernel has already recycled,
# and a `#!` script cannot control its own `argv[0]` - the kernel replaces it
# with the interpreter. Running the interpreter itself through a symlink named
# `clickhouse-server` gives the fake exactly the identity of the real thing.
_FAKE_SERVER = """
import fcntl
import os
import signal
import sys
import time

pid_file, status_file, ignore_signals = sys.argv[1], sys.argv[2], sys.argv[3]
# When given, the server exits as soon as this file appears, so that the
# watchdog restarts it under a new pid - the `CLICKHOUSE_WATCHDOG_RESTART=1`
# shape, driven by the test instead of by a signal so that the drift is
# deterministic.
restart_file = sys.argv[4]
# "once" hands the pid over a single time; "always" is the full
# `CLICKHOUSE_WATCHDOG_RESTART=1` watchdog, which brings a new server up after
# every abnormal exit - so a teardown that only kills servers never wins.
restart_mode = sys.argv[5]
# Bounded well above `RESPAWN_STOP_ATTEMPTS`, so that a teardown killing only
# servers loses, yet a failing test cannot leave a process respawning forever.
RESTART_ALWAYS_LIMIT = 20


def serve(hand_over):
    for name in ignore_signals.split():
        signal.signal(getattr(signal, "SIG" + name), signal.SIG_IGN)
    # Hold the data directory lock that the scraping `clickhouse local` needs.
    # The pid file is written only once the lock is held, so a test that sees
    # the pid file can rely on the lock being taken.
    fd = os.open(status_file, os.O_CREAT | os.O_WRONLY, 0o644)
    fcntl.flock(fd, fcntl.LOCK_EX)
    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))
    while True:
        if hand_over and os.path.exists(restart_file):
            os._exit(0)
        time.sleep(0.1)


if restart_mode == "always":
    # Detach the watchdog from the `sh -c ...` wrapper - fork it, and let the
    # process the wrapper is waiting for exit at once - so that the only thing
    # bringing servers back is the watchdog itself, whether or not the shell
    # exec'd this process instead of forking it.
    if os.fork() != 0:
        os._exit(0)
    for _ in range(RESTART_ALWAYS_LIMIT):
        if os.fork() == 0:
            serve(hand_over=False)
        os.wait()
    sys.exit(0)

if os.fork() == 0:
    serve(hand_over=bool(restart_file))
# The watchdog half: outlive the server, as `setupWatchdog` does. `argv[0]`
# survives the fork, so the server below is indistinguishable from the real one
# to `_server_process_alive`, just as in CI.
os.wait()
if restart_file:
    # Restart the server once, keeping the watchdog's and the wrapper's own
    # pids - which is exactly what makes the pid snapshot taken at startup
    # stale.
    if os.fork() == 0:
        serve(hand_over=False)
    os.wait()
sys.exit(0)
"""


def _pid_alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _start_fake_server(
    tmp_path,
    name="clickhouse-server",
    ignore_signals="TERM",
    restart_file="",
    restart_mode="once",
):
    """Start wrapper shell -> watchdog -> "server", as the CI harness does.

    `name` is the `argv[0]` the fake server runs under - the sole thing that
    makes it the server as far as `_server_process_alive` is concerned.

    With `restart_file`, the watchdog restarts the server under a new pid once
    that file appears, modelling `CLICKHOUSE_WATCHDOG_RESTART=1`. With
    `restart_mode="always"` it keeps doing so after every abnormal exit, as the
    real watchdog in that mode does.

    Returns `(proc, pid)`, the pair `stop_server` iterates over: `proc` is the
    `Popen` handle on the wrapper, `pid` is what the server wrote to its pid file.
    """
    script = tmp_path / "fake_server.py"
    script.write_text(_FAKE_SERVER)
    # A symlink to the interpreter, so the process runs with `argv[0]` ending in
    # `name` while `/proc/<pid>/exe` points at the real Python binary - the same
    # split as the real `clickhouse-server`, which is a symlink to `clickhouse`.
    server = tmp_path / name
    server.symlink_to(sys.executable)
    # Always the production pid-file name, whatever the binary is called: an
    # unrelated process must not be taken for the server just because
    # `clickhouse-server.pid` appears somewhere in its command line.
    pid_file = tmp_path / "clickhouse-server.pid"
    status_file = tmp_path / "status"
    proc = subprocess.Popen(
        f"{server} {script} {pid_file} {status_file} '{ignore_signals}' "
        f"'{restart_file}' '{restart_mode}'",
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


def test_stop_server_stops_the_server_the_pid_file_names_now(monkeypatch, tmp_path):
    # `CLICKHOUSE_WATCHDOG_RESTART=1` lets `BaseDaemon::setupWatchdog` respawn
    # the server under a new pid while the wrapper and the watchdog above it keep
    # theirs, and the new server rewrites the pid file. The pid `stop_server`
    # snapshotted at startup is then stale: signalling it does nothing, and
    # killing the wrapper does not stop the server - which keeps
    # `<run_path>/status` locked and takes this replica's system tables down with
    # it. So `stop_server` must signal whoever the pid file names at teardown
    # time, not who it named at startup.
    restart_file = tmp_path / "restart"
    pid_file = tmp_path / "clickhouse-server.pid"
    proc, startup_pid = _start_fake_server(tmp_path, restart_file=restart_file)
    respawned_pid = 0
    try:
        restart_file.touch()
        deadline = time.monotonic() + 60
        while True:
            current = pid_file.read_text().strip()
            if (
                current
                and int(current) != startup_pid
                and not _status_lock_is_free(tmp_path)
            ):
                respawned_pid = int(current)
                break
            assert time.monotonic() < deadline, (
                "the fake watchdog did not bring the server back under a new pid"
            )
            time.sleep(0.05)
        _make_proc(monkeypatch, tmp_path, proc, startup_pid).stop_server()
        assert not ClickHouseProc._server_process_alive(respawned_pid), (
            "the server outlived stop_server because its pid had drifted from "
            "the startup snapshot"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
    finally:
        _cleanup(proc, startup_pid)
        if respawned_pid:
            _cleanup(proc, respawned_pid)


def test_stop_server_stops_the_watchdog_that_keeps_restarting_the_server(
    monkeypatch, tmp_path
):
    # A watchdog in `CLICKHOUSE_WATCHDOG_RESTART=1` mode brings a new server up
    # after *every* abnormal exit, so killing servers alone can never win: each
    # `SIGKILL` only triggers the next restart, and a teardown that gives up
    # after a bounded number of them returns with yet another server alive and
    # holding `<run_path>/status` - losing this replica's system tables just the
    # same. `stop_server` therefore has to take out the watchdog itself.
    pid_file = tmp_path / "clickhouse-server.pid"
    proc, startup_pid = _start_fake_server(tmp_path, restart_mode="always")
    watchdogs = ClickHouseProc._server_watchdog_pids(startup_pid)
    try:
        assert watchdogs, "the fake watchdog is not above the fake server"
        _make_proc(monkeypatch, tmp_path, proc, startup_pid).stop_server()
        # Whoever the pid file names by now - the first server or the n-th one
        # the watchdog brought back - must be gone, and the lock free.
        assert not ClickHouseProc._current_server_pid(pid_file), (
            f"a server the watchdog restarted ({pid_file.read_text().strip()}) "
            "outlived stop_server"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
        for watchdog in watchdogs:
            assert not _pid_alive(watchdog), (
                f"the watchdog {watchdog} outlived stop_server and can start "
                "yet another server"
            )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, ClickHouseProc._current_server_pid(pid_file) or startup_pid)


def test_stop_server_leaves_an_unrelated_process_alone(monkeypatch, tmp_path):
    # The pid file is read once at startup, so by teardown its pid may belong to
    # an unrelated process the kernel has since given it to. stop_server must not
    # kill it - the `argv[0]` check in `_server_process_alive` is what stops a
    # `SIGKILL` from going to a random process on the runner. Note that this
    # process does carry `.../clickhouse-server.pid` in its arguments, the shape
    # a substring search over the whole command line would wrongly accept.
    proc, pid = _start_fake_server(tmp_path, name="not-a-clickhouse-binary")
    try:
        _make_proc(monkeypatch, tmp_path, proc, pid).stop_server()
        assert _pid_alive(pid), "stop_server killed an unrelated process"
    finally:
        _cleanup(proc, pid)


def test_server_liveness_check_survives_a_long_command_line(tmp_path):
    # A liveness check that wrongly answers "already gone" skips the KILL and
    # leaves the server running, so it must not depend on how much of the
    # command line it can see: `ps` truncates it to 80 columns when its output
    # is not a terminal, which is why `_server_process_alive` reads
    # `/proc/<pid>/cmdline` instead.
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
