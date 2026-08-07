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

from ci.jobs.scripts.clickhouse_proc import ClickHouseProc, ProcessIdentityUnknown
from ci.praktika.utils import Shell, Utils

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
#
# The real watchdog does not keep the server's name: `setupWatchdog` writes
# `clickhouse-watchdog` over its own `argv[0]` in place, truncated to the
# length of the original - `clickhouse-server` leaves `clickhouse-watchd` - and
# restores the original name just before each respawn fork, so the next server
# inherits it. The fake models both renames with `execv` through differently
# named symlinks (`argv[7]` the server one, `argv[8]` the watchdog one), which
# keeps the pid while changing the identity, exactly like the in-place `memcpy`.
# An empty `argv[8]` keeps the watchdog under the server's own name - the shape
# of the rename window, which the harness must accept as a watchdog too.
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
# every abnormal exit - so a teardown that only kills servers never wins;
# "delayed" brings one back exactly once, `restart_delay` seconds after the
# previous server died, so the pid file is empty at teardown time.
restart_mode = sys.argv[5]
restart_delay = float(sys.argv[6])
server_exe, watchdog_exe = sys.argv[7], sys.argv[8]
role = sys.argv[9] if len(sys.argv) > 9 else "entry"
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


def spawn_server_and_wait(hand_over=False):
    # Fork the server below the watchdog. When the watchdog runs renamed, the
    # child re-execs through the server symlink - restoring the server's own
    # name for the child, as the real watchdog's `memcpy` of the original
    # `argv[0]` before the fork does - keeping the pid the fork gave it.
    if os.fork() == 0:
        if watchdog_exe:
            os.execv(
                server_exe,
                [server_exe]
                + sys.argv[:9]
                + ["serve-handover" if hand_over else "serve"],
            )
        serve(hand_over)
    os.wait()


def watch():
    if restart_mode == "delayed":
        spawn_server_and_wait()
        time.sleep(restart_delay)
        spawn_server_and_wait()
    elif restart_mode == "always":
        for _ in range(RESTART_ALWAYS_LIMIT):
            spawn_server_and_wait()
    else:
        spawn_server_and_wait(hand_over=bool(restart_file))
        if restart_file:
            # Restart the server once, keeping the watchdog's and the wrapper's
            # own pids - which is exactly what makes the pid snapshot taken at
            # startup stale.
            spawn_server_and_wait()
    sys.exit(0)


if role == "serve":
    serve(hand_over=False)
if role == "serve-handover":
    serve(hand_over=True)
if role == "watch":
    watch()

# The entry process, forked by the `sh -c ...` wrapper.
if restart_mode in ("delayed", "always"):
    # Detach the watchdog from the `sh -c ...` wrapper - fork it, and let the
    # process the wrapper is waiting for exit at once - so that the only thing
    # bringing servers back is the watchdog itself, whether or not the shell
    # exec'd this process instead of forking it. For "delayed" this also makes
    # the wrapper already gone when the teardown reaps it and the pid file is
    # looked at immediately after the server died - before this watchdog
    # publishes the replacement.
    if os.fork() != 0:
        os._exit(0)
# Become the watchdog: outlive the server, as `setupWatchdog` does. The exec
# through the watchdog symlink is `setupWatchdog`'s in-place rename to
# `clickhouse-watchdog` - same pid, new `argv[0]`.
if watchdog_exe:
    os.execv(watchdog_exe, [watchdog_exe] + sys.argv[:9] + ["watch"])
watch()
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
    restart_delay=0,
    watchdog_name="clickhouse-watchd",
):
    """Start wrapper shell -> watchdog -> "server", as the CI harness does.

    `name` is the `argv[0]` the fake server runs under - the sole thing that
    makes it the server as far as `_server_process_alive` is concerned.
    `watchdog_name` is the `argv[0]` the watchdog renames itself to; the
    default is what `BaseDaemon::setupWatchdog`'s in-place rename leaves for a
    server started as `clickhouse-server` - `clickhouse-watchdog` truncated to
    the original name's length. Pass "" to keep the watchdog under the server's
    own name, the shape of the moments around the rename.

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
    watchdog = ""
    if watchdog_name:
        watchdog = tmp_path / watchdog_name
        watchdog.symlink_to(sys.executable)
    # Always the production pid-file name, whatever the binary is called: an
    # unrelated process must not be taken for the server just because
    # `clickhouse-server.pid` appears somewhere in its command line.
    pid_file = tmp_path / "clickhouse-server.pid"
    status_file = tmp_path / "status"
    proc = subprocess.Popen(
        f"{server} {script} {pid_file} {status_file} '{ignore_signals}' "
        f"'{restart_file}' '{restart_mode}' '{restart_delay}' "
        f"'{server}' '{watchdog}'",
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
    # As `start` does, snapshot the watchdog chain while the startup pid is
    # known to be alive: it is what `stop_server` falls back to when the
    # teardown begins inside the watchdog's restart gap and the walk from the
    # (then dead) pid finds nothing.
    ch.watchdogs_0 = ClickHouseProc._startup_watchdog_snapshot(pid)
    ch.proc_1 = ch.proc_2 = None
    ch.pid_1 = ch.pid_2 = 0
    ch.watchdogs_1 = ch.watchdogs_2 = ()
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


def test_stop_server_waits_out_a_watchdog_that_respawns_late(monkeypatch, tmp_path):
    # An empty pid file at teardown time does not mean nothing is coming back:
    # the replacement server publishes its pid only once it is up, and `start`
    # itself budgets 15 seconds for that. A teardown that reads the pid file
    # once, sees no live server and returns therefore loses the race - the
    # watchdog brings a server up afterwards, and that server holds
    # `<run_path>/status` while `clickhouse local` tries to scrape
    # `system.*_log`, which is the very failure this teardown exists to prevent.
    # So nothing that can still start a server may outlive `stop_server`.
    pid_file = tmp_path / "clickhouse-server.pid"
    respawn_delay = 5
    proc, startup_pid = _start_fake_server(
        tmp_path, restart_mode="delayed", restart_delay=respawn_delay
    )
    watchdogs = ClickHouseProc._server_watchdog_pids(startup_pid)
    try:
        assert watchdogs, "the fake watchdog is not above the fake server"
        _make_proc(monkeypatch, tmp_path, proc, startup_pid).stop_server()
        for watchdog in watchdogs:
            assert not _pid_alive(watchdog), (
                f"the watchdog {watchdog} outlived stop_server and is still free "
                "to bring a server back"
            )
        # And it did not manage to leave one behind on its way out either, now
        # or once its delay has fully elapsed.
        time.sleep(respawn_delay + 1)
        assert not ClickHouseProc._current_server_pid(pid_file), (
            f"a server ({pid_file.read_text().strip()}) came up after "
            "stop_server returned"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, ClickHouseProc._current_server_pid(pid_file) or startup_pid)


def test_stop_server_wins_when_teardown_starts_inside_the_restart_gap(
    monkeypatch, tmp_path
):
    # The teardown can begin *inside* the watchdog's restart gap: one server
    # has exited, the replacement has not yet rewritten the pid file. The pid
    # file then names a dead pid, and the startup pid is that same dead pid, so
    # a watchdog discovery that only walks up from either finds nothing - and a
    # teardown with no watchdogs to wait for returns at once, while the still
    # live watchdog publishes a replacement server a few seconds later, holding
    # `<run_path>/status` against the scraping `clickhouse local`. The startup
    # snapshot of the watchdog chain is what has to close this: it was taken
    # while the first server was alive, and the watchdog's own pid does not
    # change across the respawns it performs.
    pid_file = tmp_path / "clickhouse-server.pid"
    respawn_delay = 5
    proc, startup_pid = _start_fake_server(
        tmp_path, restart_mode="delayed", restart_delay=respawn_delay
    )
    watchdogs = ClickHouseProc._server_watchdog_pids(startup_pid)
    try:
        assert watchdogs, "the fake watchdog is not above the fake server"
        # `_make_proc` snapshots the watchdogs while the server is alive, as
        # `start` does - before the server dies below.
        ch = _make_proc(monkeypatch, tmp_path, proc, startup_pid)
        os.kill(startup_pid, signal.SIGKILL)
        deadline = time.monotonic() + 60
        while _pid_alive(startup_pid):
            assert time.monotonic() < deadline, "the fake server did not die"
            time.sleep(0.05)
        # The teardown now starts with a dead pid in the pid file, a dead
        # startup pid, and the replacement `respawn_delay` seconds away.
        ch.stop_server()
        for watchdog in watchdogs:
            assert not _pid_alive(watchdog), (
                f"the watchdog {watchdog} outlived a teardown that began in "
                "its restart gap and is still free to bring a server back"
            )
        time.sleep(respawn_delay + 1)
        assert not ClickHouseProc._current_server_pid(pid_file), (
            f"a server ({pid_file.read_text().strip()}) came up after "
            "stop_server returned"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, ClickHouseProc._current_server_pid(pid_file) or startup_pid)


def test_stop_respawned_server_survives_a_walk_failure_with_the_startup_snapshot(
    monkeypatch, tmp_path
):
    # `_stop_respawned_server` walks the watchdogs up from whatever server came
    # back, and on the `HAS_PROC = False` path a transient `ps` failure turns
    # that walk into `[]` (see `_parent_pid`). Killing only the server then
    # cannot win - the watchdog respawns another one on every attempt until the
    # loop runs out and `stop_server` returns with a live server holding
    # `<run_path>/status`. So that branch has to fall back to the `watchdogs`
    # snapshot taken before the teardown, exactly as `_stop_one_server` does
    # for its own walk.
    pid_file = tmp_path / "clickhouse-server.pid"
    proc, startup_pid = _start_fake_server(tmp_path, restart_mode="always")
    watchdogs = ClickHouseProc._server_watchdog_pids(startup_pid)
    try:
        assert watchdogs, "the fake watchdog is not above the fake server"
        ch = _make_proc(monkeypatch, tmp_path, proc, startup_pid)
        # The startup snapshot above and the walk inside `_stop_one_server`
        # stay real; the walk fails only while `_stop_respawned_server` runs,
        # the shape of a `ps` outage that begins mid-teardown.
        real_walk = ClickHouseProc._server_watchdog_pids.__func__
        real_stop_respawned = ClickHouseProc._stop_respawned_server.__func__

        def stop_respawned_with_failing_walk(
            cls, pid_file, run_path, watchdogs=(), watchdogs_definitive=True
        ):
            monkeypatch.setattr(
                ClickHouseProc,
                "_server_watchdog_pids",
                classmethod(lambda cls, pid: []),
            )
            try:
                return real_stop_respawned(
                    cls, pid_file, run_path, watchdogs, watchdogs_definitive
                )
            finally:
                monkeypatch.setattr(
                    ClickHouseProc, "_server_watchdog_pids", classmethod(real_walk)
                )

        monkeypatch.setattr(
            ClickHouseProc,
            "_stop_respawned_server",
            classmethod(stop_respawned_with_failing_walk),
        )
        ch.stop_server()
        for watchdog in watchdogs:
            assert not _pid_alive(watchdog), (
                f"the watchdog {watchdog} outlived a teardown whose watchdog "
                "walk failed and is still free to bring a server back"
            )
        assert not ClickHouseProc._current_server_pid(pid_file), (
            f"a server ({pid_file.read_text().strip()}) outlived stop_server"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, ClickHouseProc._current_server_pid(pid_file) or startup_pid)


def test_stop_server_wins_the_restart_gap_even_when_startup_discovery_failed(
    monkeypatch, tmp_path
):
    # The startup snapshot itself can be lost to a `ps` outage: `start` then
    # records the watchdogs as unknown (None, see `_startup_watchdog_snapshot`)
    # rather than as a definitive empty snapshot. A teardown that begins inside
    # the watchdog's restart gap now has a dead pid in the pid file, a dead
    # startup pid, *and* no watchdog pids to wait on - so the only proof left
    # that no server is coming back is the pid file staying empty for the whole
    # respawn grace period. Trusting the empty set immediately loses: the
    # teardown returns in milliseconds, the watchdog publishes a replacement a
    # few seconds later, and that server holds `<run_path>/status` against the
    # scraping `clickhouse local`.
    pid_file = tmp_path / "clickhouse-server.pid"
    respawn_delay = 5
    proc, startup_pid = _start_fake_server(
        tmp_path, restart_mode="delayed", restart_delay=respawn_delay
    )
    watchdogs = ClickHouseProc._server_watchdog_pids(startup_pid)
    try:
        assert watchdogs, "the fake watchdog is not above the fake server"
        ch = _make_proc(monkeypatch, tmp_path, proc, startup_pid)
        # What `start` records when the watchdog discovery keeps failing.
        real_checked = ClickHouseProc._server_watchdog_pids_checked.__func__
        monkeypatch.setattr(
            ClickHouseProc,
            "_server_watchdog_pids_checked",
            classmethod(lambda cls, pid: ([], False)),
        )
        ch.watchdogs_0 = ClickHouseProc._startup_watchdog_snapshot(startup_pid)
        monkeypatch.setattr(
            ClickHouseProc, "_server_watchdog_pids_checked", classmethod(real_checked)
        )
        assert ch.watchdogs_0 is None, (
            "a failed startup discovery must be recorded as unknown"
        )
        # Well above the fake watchdog's respawn delay, well below the test's
        # patience: the wait for a republished server is the whole point here,
        # not the production-sized grace period.
        monkeypatch.setattr(ClickHouseProc, "RESPAWN_GRACE_TIMEOUT", 10)
        os.kill(startup_pid, signal.SIGKILL)
        deadline = time.monotonic() + 60
        while _pid_alive(startup_pid):
            assert time.monotonic() < deadline, "the fake server did not die"
            time.sleep(0.05)
        # The teardown now starts with a dead pid in the pid file, a dead
        # startup pid, an unknown watchdog set, and the replacement
        # `respawn_delay` seconds away.
        ch.stop_server()
        for watchdog in watchdogs:
            assert not _pid_alive(watchdog), (
                f"the watchdog {watchdog} outlived a teardown that had no "
                "startup snapshot to fall back on"
            )
        time.sleep(respawn_delay + 1)
        assert not ClickHouseProc._current_server_pid(pid_file), (
            f"a server ({pid_file.read_text().strip()}) came up after "
            "stop_server returned"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, ClickHouseProc._current_server_pid(pid_file) or startup_pid)


def test_startup_snapshot_over_a_dead_server_is_not_proof_of_no_watchdog(
    monkeypatch, tmp_path
):
    # The first server can die between writing its pid file and `start` taking
    # the watchdog snapshot - its watchdog already counting down to the
    # respawn. The walk then runs over a corpse: a dead pid has no readable
    # parent, so the chain comes back empty and definitive-looking, and
    # recording that `()` reopens the leak this fix exists for - a teardown
    # that begins inside the restart gap trusts the snapshot, believes the
    # empty pid file immediately, and returns milliseconds before the still
    # live watchdog republishes a server that holds `<run_path>/status`
    # against the scraping `clickhouse local`. A walk over a dead pid must be
    # recorded as unknown (None), exactly like a walk that failed.
    pid_file = tmp_path / "clickhouse-server.pid"
    respawn_delay = 5
    proc, startup_pid = _start_fake_server(
        tmp_path, restart_mode="delayed", restart_delay=respawn_delay
    )
    watchdogs = ClickHouseProc._server_watchdog_pids(startup_pid)
    try:
        assert watchdogs, "the fake watchdog is not above the fake server"
        # The server dies *before* the snapshot this time, unlike in
        # `test_stop_server_wins_when_teardown_starts_inside_the_restart_gap`
        # where the snapshot catches the live server.
        os.kill(startup_pid, signal.SIGKILL)
        deadline = time.monotonic() + 60
        while _pid_alive(startup_pid):
            assert time.monotonic() < deadline, "the fake server did not die"
            time.sleep(0.05)
        # `_make_proc` takes the startup snapshot, as `start` does - here from
        # a pid that is already dead.
        ch = _make_proc(monkeypatch, tmp_path, proc, startup_pid)
        assert ch.watchdogs_0 is None, (
            "a startup snapshot walked over a dead server must be recorded as "
            f"unknown, not as {ch.watchdogs_0!r}"
        )
        # Well above the fake watchdog's respawn delay, well below the test's
        # patience (see the same override in the restart-gap tests above).
        monkeypatch.setattr(ClickHouseProc, "RESPAWN_GRACE_TIMEOUT", 10)
        ch.stop_server()
        for watchdog in watchdogs:
            assert not _pid_alive(watchdog), (
                f"the watchdog {watchdog} outlived a teardown whose startup "
                "snapshot was walked over a dead server"
            )
        time.sleep(respawn_delay + 1)
        assert not ClickHouseProc._current_server_pid(pid_file), (
            f"a server ({pid_file.read_text().strip()}) came up after "
            "stop_server returned"
        )
        assert _status_lock_is_free(tmp_path), (
            "the status lock was still held after stop_server returned; "
            "`clickhouse local` cannot scrape this replica's system tables"
        )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, ClickHouseProc._current_server_pid(pid_file) or startup_pid)


def test_watchdog_discovery_sees_the_renamed_watchdog(tmp_path):
    # `BaseDaemon::setupWatchdog` renames the watchdog to `clickhouse-watchdog`
    # in place, so a server started as `clickhouse-server` leaves it named
    # `clickhouse-watchd` - not a server as far as `_server_process_alive` is
    # concerned. A watchdog discovery that only accepts the server's own name
    # therefore returns nothing in `CLICKHOUSE_WATCHDOG_RESTART=1` mode: the
    # grace-period wait in `_stop_respawned_server` has no watchdog to wait
    # for, and an empty pid file is taken as final while the renamed parent is
    # still free to publish a replacement server - the very race the wait
    # exists to close.
    proc, pid = _start_fake_server(tmp_path)
    watchdogs = []
    try:
        watchdogs = ClickHouseProc._server_watchdog_pids(pid)
        assert watchdogs, (
            "the watchdog discovery does not see a watchdog renamed to "
            "`clickhouse-watchd`"
        )
        for watchdog in watchdogs:
            assert ClickHouseProc._watchdog_process_alive(watchdog), (
                f"the renamed watchdog {watchdog} is not accepted as a watchdog"
            )
            # And it is exactly the identity split that used to hide it: the
            # renamed watchdog no longer passes for the server itself.
            assert not ClickHouseProc._server_process_alive(watchdog), (
                f"the renamed watchdog {watchdog} passes the *server* identity "
                "check; this test no longer exercises the rename"
            )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, pid)


def test_watchdog_discovery_accepts_the_untruncated_watchdog_name(tmp_path):
    # A server started under a name at least as long as `clickhouse-watchdog`
    # (a path, say) leaves the rename untruncated, so the full name must be
    # accepted just like the `clickhouse-watchd` truncation.
    proc, pid = _start_fake_server(tmp_path, watchdog_name="clickhouse-watchdog")
    watchdogs = []
    try:
        watchdogs = ClickHouseProc._server_watchdog_pids(pid)
        assert watchdogs, (
            "the watchdog discovery does not see a watchdog named "
            "`clickhouse-watchdog`"
        )
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, pid)


def test_watchdog_discovery_accepts_a_watchdog_still_under_the_server_name(tmp_path):
    # Around the rename the watchdog still carries the server's own `argv[0]`:
    # between the fork and the rename, and again around each respawn, when
    # `setupWatchdog` restores the original name for the next server to
    # inherit. Both names must count as the watchdog.
    proc, pid = _start_fake_server(tmp_path, watchdog_name="")
    watchdogs = []
    try:
        watchdogs = ClickHouseProc._server_watchdog_pids(pid)
        assert watchdogs, (
            "the watchdog discovery does not see a watchdog that still runs "
            "under the server's own name"
        )
        for watchdog in watchdogs:
            assert ClickHouseProc._watchdog_process_alive(watchdog)
    finally:
        for watchdog in watchdogs:
            _cleanup(proc, watchdog)
        _cleanup(proc, pid)


def test_stop_server_leaves_an_unrelated_process_alone(monkeypatch, tmp_path):
    # The pid file is read once at startup, so by teardown its pid may belong to
    # an unrelated process the kernel has since given it to. stop_server must not
    # kill it - the `argv[0]` check in `_server_process_alive` is what stops a
    # `SIGKILL` from going to a random process on the runner. Note that this
    # process does carry `.../clickhouse-server.pid` in its arguments, the shape
    # a substring search over the whole command line would wrongly accept.
    proc, pid = _start_fake_server(
        tmp_path, name="not-a-clickhouse-binary", watchdog_name=""
    )
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


# The macOS half. `ClickHouseProc.HAS_PROC` is probed once at import, so setting
# it to False here drives the exact code path the `macos_m2` runners take while
# the tests keep running on Linux CI. The original change had no such coverage:
# every test above passes on a host with `/proc`, and the Darwin path - where
# `_server_process_alive` answered "already gone" for every pid - shipped
# untested and wedged the whole macOS pool.


def test_liveness_check_without_proc_sees_a_live_server(monkeypatch, tmp_path):
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
    proc, pid = _start_fake_server(tmp_path)
    try:
        assert ClickHouseProc._server_process_alive(pid), (
            "the liveness check reports a live server gone when there is no /proc"
        )
    finally:
        _cleanup(proc, pid)


def test_liveness_check_without_proc_rejects_an_unrelated_process(
    monkeypatch, tmp_path
):
    # The `argv[0]` identity guard has to hold on the `ps` path too, or a
    # recycled pid gets a SIGKILL meant for the server.
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
    proc, pid = _start_fake_server(
        tmp_path, name="not-a-clickhouse-binary", watchdog_name=""
    )
    try:
        assert not ClickHouseProc._server_process_alive(pid), (
            "the liveness check took an unrelated process for the server"
        )
    finally:
        _cleanup(proc, pid)


def test_liveness_check_without_proc_sees_a_dead_pid_as_gone(monkeypatch, tmp_path):
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
    proc, pid = _start_fake_server(tmp_path)
    _cleanup(proc, pid)
    deadline = time.monotonic() + 60
    while _pid_alive(pid):
        assert time.monotonic() < deadline, "the fake server did not exit"
        time.sleep(0.05)
    assert not ClickHouseProc._server_process_alive(pid), (
        "the liveness check reports a dead pid alive, so stop_server would spin"
    )


def test_liveness_check_without_proc_survives_a_long_command_line(
    monkeypatch, tmp_path
):
    # The reason `/proc` was preferred in the first place: `ps` truncates the
    # command line to the terminal width when its output is not a terminal, and
    # a truncated one no longer ends in `clickhouse-server`. `_ps_field` passes
    # `-ww` to switch that off; without it this test fails and the `ps` path
    # regresses into the same wrong "already gone" the revert was about.
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
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


def test_parent_pid_without_proc_matches_the_proc_answer(monkeypatch, tmp_path):
    # `_server_watchdog_pids` walks up the process tree to find the watchdog
    # that keeps restarting the server; the walk has to work without `/proc` too.
    proc, pid = _start_fake_server(tmp_path)
    try:
        monkeypatch.setattr(ClickHouseProc, "HAS_PROC", True)
        from_proc = ClickHouseProc._parent_pid(pid)
        monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
        from_ps = ClickHouseProc._parent_pid(pid)
        assert from_proc > 1, "the /proc reference answer is not a real pid"
        assert from_ps == from_proc, (
            f"ps reports the parent of {pid} as {from_ps}, /proc as {from_proc}"
        )
    finally:
        _cleanup(proc, pid)


def test_watchdog_walk_survives_a_ps_failure_without_proc(monkeypatch):
    # A `ps` failure inside the watchdog walk is "no parent information", not
    # an exception: raised out of `start` it reported success without ever
    # recording `self.pid_*`, so `stop_server` skipped the replica entirely;
    # raised out of `_stop_one_server` it aborted the teardown before any
    # signal was sent, leaving the server holding the status lock.
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)

    def unavailable(*args, **kwargs):
        raise ProcessIdentityUnknown("ps is not available")

    monkeypatch.setattr(ClickHouseProc, "_ps_field", classmethod(unavailable))
    assert ClickHouseProc._parent_pid(os.getpid()) == 0, (
        "a ps failure must read as an unknown parent, not break the caller"
    )
    assert ClickHouseProc._server_watchdog_pids(os.getpid()) == [], (
        "the watchdog walk must degrade to no watchdogs when ps fails"
    )


def test_startup_snapshot_records_a_failed_discovery_as_unknown(monkeypatch):
    # `start` snapshots the watchdog chain from a pid that is known to be alive
    # at that moment, and the empty snapshot is what later lets
    # `_stop_respawned_server` take an empty pid file as proof that no server
    # is coming back. A `ps` outage during `start` must therefore be recorded
    # as "unknown" (None) - collapsing it to an empty snapshot reopens the
    # leaked-server / held-`status` race for the whole run (the teardown would
    # trust the snapshot and return inside the watchdog's restart gap).
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
    monkeypatch.setattr(Utils, "sleep", staticmethod(lambda *a, **k: None))

    def unavailable(*args, **kwargs):
        raise ProcessIdentityUnknown("ps is not available")

    monkeypatch.setattr(ClickHouseProc, "_ps_field", classmethod(unavailable))
    assert ClickHouseProc._startup_watchdog_snapshot(os.getpid()) is None, (
        "a startup watchdog discovery that keeps failing must be recorded as "
        "unknown, not as a definitive empty snapshot"
    )


def test_startup_snapshot_retries_a_transient_discovery_failure(monkeypatch):
    # The pid the startup snapshot is walked from is known to name a live
    # server, so a failed walk is a transient `ps` hiccup, not an answer:
    # `_startup_watchdog_snapshot` retries before settling for "unknown".
    monkeypatch.setattr(Utils, "sleep", staticmethod(lambda *a, **k: None))
    answers = iter([([], False), ([42], True)])
    monkeypatch.setattr(
        ClickHouseProc,
        "_server_watchdog_pids_checked",
        classmethod(lambda cls, pid: next(answers)),
    )
    # The snapshot confirms the walked pid is still the server before trusting
    # a completed walk; this test's stand-in pid is the test runner, not a
    # `clickhouse-server`, so answer that confirmation directly.
    monkeypatch.setattr(
        ClickHouseProc,
        "_server_process_alive",
        classmethod(lambda cls, pid, unknown_alive=True: True),
    )
    assert ClickHouseProc._startup_watchdog_snapshot(os.getpid()) == (42,), (
        "a transient walk failure at startup must be retried, not recorded"
    )


def test_watchdog_discovery_does_not_promote_unreadable_ancestors(monkeypatch):
    # `ps -o command=` failing while `ps -o ppid=` still works: an ancestor
    # whose identity cannot be read must end the walk. The fail-close "unknown
    # means alive" of the server liveness check is wrong here - it would turn
    # the `sh -c` wrapper and every live ancestor above it into watchdogs, and
    # `_kill_watchdogs` would then aim SIGKILL at unrelated processes.
    parents = {10: 20, 20: 30, 30: 1}
    monkeypatch.setattr(
        ClickHouseProc,
        "_parent_pid",
        classmethod(lambda cls, pid, unknown_raises=False: parents.get(pid, 0)),
    )

    def unreadable(cls, pid):
        raise ProcessIdentityUnknown(f"cannot read the command of process {pid}")

    monkeypatch.setattr(ClickHouseProc, "_process_argv0", classmethod(unreadable))
    monkeypatch.setattr(ClickHouseProc, "_pid_exists", staticmethod(lambda pid: True))
    assert ClickHouseProc._server_watchdog_pids(10) == [], (
        "ancestors with an unreadable identity were promoted into kill targets"
    )


def test_liveness_check_reports_alive_when_the_identity_cannot_be_read(
    monkeypatch, tmp_path
):
    # Neither source can answer. Reporting "gone" is what leaves an orphan
    # holding the job's stdout pipe and wedges an ephemeral runner for good, so
    # an existing pid is reported alive and stop_server goes on to stop it.
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)

    def unavailable(*args, **kwargs):
        raise ProcessIdentityUnknown("ps is not available")

    monkeypatch.setattr(ClickHouseProc, "_ps_field", classmethod(unavailable))
    assert ClickHouseProc._server_process_alive(os.getpid()), (
        "an existing pid must not be reported gone when it cannot be identified"
    )
    proc, pid = _start_fake_server(tmp_path)
    _cleanup(proc, pid)
    deadline = time.monotonic() + 60
    while _pid_alive(pid):
        assert time.monotonic() < deadline, "the fake server did not exit"
        time.sleep(0.05)
    assert not ClickHouseProc._server_process_alive(pid), (
        "a pid that does not exist must still be reported gone"
    )


def test_stop_server_stops_the_server_without_proc(monkeypatch, tmp_path):
    # The incident itself, as an end-to-end test: on a host without `/proc` the
    # teardown concluded "already gone - TERM not sent" for every server, killed
    # only the `sh -c` wrapper and returned. The orphaned server kept the write
    # end of praktika's stdout pipe open, so `TeePopen.wait` never saw EOF and
    # `praktika run` hung after an otherwise-green run - burning one ephemeral
    # macOS host per job until the pool ran out.
    monkeypatch.setattr(ClickHouseProc, "HAS_PROC", False)
    proc, pid = _start_fake_server(tmp_path)
    try:
        _make_proc(monkeypatch, tmp_path, proc, pid).stop_server()
        assert not _pid_alive(pid), (
            "stop_server left the server running on a host without /proc"
        )
        assert _status_lock_is_free(tmp_path), (
            "the data directory is still locked after stop_server"
        )
    finally:
        _cleanup(proc, pid)
