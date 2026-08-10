"""
Regression tests for `ClickHouseProc.stop_server` releasing `<run_path>/status`.

`stop_server` must leave that file lockable: the `clickhouse local --path
<run_path>` dump that follows it takes the same `flock` through `StatusFile`, and
fails with `Code: 76` and loses every `system.*_log` table of the replica if the
server still holds it.

The fake servers reproduce the production topology, which is what makes the
signalling non-trivial: they are launched through `Popen(shell=True)`, so `proc`
is a `sh -c` wrapper rather than the server, they hold an `flock` on `status`,
and they write `PID: <pid>` into it exactly like `StatusFile::write_full_info`.
The oracle is the lock itself, the very predicate `clickhouse local` evaluates,
plus a spy on signal delivery so an arm named for one leg of the escalation
ladder cannot pass on the other leg's behaviour.

`test_prefix_fallback_leaves_lock_held` is the negative control: it runs the
pre-fix fallback against the same fixture and requires the lock to stay held.
Without it, the assertions below could pass for reasons unrelated to the fix.
"""

import fcntl
import os
import signal
import subprocess
import sys
import tempfile
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.scripts import clickhouse_proc as clickhouse_proc_module
from ci.jobs.scripts.clickhouse_proc import ClickHouseProc

# Scaled-down budgets: the TRAP leg must still outlast a server that takes
# TRAP_EXIT_DELAY to die, while the TRAP-ignoring arm escalates in seconds.
TRAP_WAIT = 5
KILL_WAIT = 1
TRAP_EXIT_DELAY = 2
# Comfortably longer than any arm, so a fake server that is never signalled is
# unambiguously still alive at the end of the test.
SERVER_SLEEP = 120

FAKE_SERVER = r"""
import fcntl, os, signal, sys, time
run_path, pid_file, trap_dies, trap_delay, marker, sleep_for = sys.argv[1:7]
signal.signal(signal.SIGTERM, signal.SIG_IGN)
if trap_dies == "1":
    def _on_trap(signum, frame):
        # Hold the lock for the delay first: that is the window in which the
        # kernel would be writing the core this signal exists to produce.
        time.sleep(float(trap_delay))
        open(marker, "w").write("trap\n")
        os._exit(0)
    signal.signal(signal.SIGTRAP, _on_trap)
else:
    signal.signal(signal.SIGTRAP, signal.SIG_IGN)
fd = os.open(os.path.join(run_path, "status"), os.O_WRONLY | os.O_CREAT, 0o666)
fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
# Byte-for-byte the layout of StatusFile::write_full_info, whose first line is
# the identity the fix checks before signalling.
os.write(fd, ("PID: " + str(os.getpid()) + "\nStarted at: 2026-08-10 00:00:00\nRevision: 1\n").encode())
os.fsync(fd)
open(pid_file, "w").write(str(os.getpid()))
time.sleep(int(sleep_for))
"""


def _lock_is_free(run_path):
    """The predicate `clickhouse local` evaluates when it opens its StatusFile."""
    status = os.path.join(run_path, "status")
    if not os.path.exists(status):
        return True
    fd = os.open(status, os.O_RDONLY)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        fcntl.flock(fd, fcntl.LOCK_UN)
        return True
    except OSError:
        return False
    finally:
        os.close(fd)


def _alive(pid):
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    return True


def _proc_state(pid):
    """The process state letter from `/proc/<pid>/stat`, or "gone" once reaped.

    A zombie keeps its `stat` entry (only `cmdline` empties), which is what makes
    this the oracle for "was the wrapper waited for". The state is read after the
    last `") "` because `comm` may itself contain spaces and parentheses.
    """
    try:
        with open(f"/proc/{pid}/stat") as file:
            return file.read().split(") ", 1)[1].split()[0]
    except OSError:
        return "gone"


class _SignalSpy:
    """Every signal delivered, so the escalation ladder itself can be asserted
    and not just its end state.

    `clickhouse_proc` does `import os`, so patching its `os.kill` patches the one
    module object every caller shares: this records the test's own signals too.
    Call `reset()` immediately before the `stop_server` under test so `calls`
    holds only what production delivered.
    """

    def __init__(self, real_kill):
        # Public: an arm that has to signal something itself during the call under
        # test must not have that recorded as production's doing.
        self.real_kill = real_kill
        self.calls = []

    def __call__(self, pid, sig):
        # Signal 0 is a liveness probe, not a delivery.
        if sig:
            self.calls.append((pid, sig))
        return self.real_kill(pid, sig)

    def reset(self):
        self.calls.clear()

    def sent(self, sig):
        return [pid for pid, got in self.calls if got == sig]


class _WarningSpy:
    """Stands in for `Info()` so the suite records the workflow warnings the
    fail-closed branch reports instead of writing them into the real job."""

    def __init__(self):
        self.warnings = []

    def __call__(self):
        return self

    def add_workflow_warning(self, message):
        self.warnings.append(message)


class _Fixture:
    def __init__(self, tmp_path, signals, warnings):
        self.tmp_path = tmp_path
        self.signals = signals
        self.warnings = warnings
        self.script = os.path.join(tmp_path, "fake_server.py")
        with open(self.script, "w") as file:
            file.write(FAKE_SERVER)
        self.procs = []

    def start(self, name, trap_dies=False, trap_delay=0):
        run_path = os.path.join(self.tmp_path, name)
        os.makedirs(run_path, exist_ok=True)
        pid_file = os.path.join(self.tmp_path, f"{name}.pid")
        marker = os.path.join(self.tmp_path, f"{name}.trap")
        # `Popen(shell=True)` runs /bin/sh, which is dash on the CI image and
        # forks rather than exec's, so `proc` is the wrapper and not the server:
        # the production topology. The trailing options mirror production too.
        command = (
            f"{sys.executable} {self.script} {run_path} {pid_file}"
            f" {'1' if trap_dies else '0'} {trap_delay} {marker} {SERVER_SLEEP}"
            f" -- --path {run_path} --logger.stderr {run_path}/stderr.log"
        )
        proc = subprocess.Popen(
            command, stderr=subprocess.STDOUT, shell=True, cwd=run_path
        )
        self.procs.append(proc)
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            if os.path.exists(pid_file) and not _lock_is_free(run_path):
                break
            time.sleep(0.2)
        else:
            raise AssertionError(f"fake server {name} did not take the lock")
        pid = int(open(pid_file).read().strip())
        assert pid != proc.pid, "fixture must reproduce the shell-wrapper topology"
        return run_path, pid_file, pid, proc, marker

    def cleanup(self):
        for proc in self.procs:
            try:
                os.kill(proc.pid, signal.SIGKILL)
            except OSError:
                pass
        # Kill any surviving fake server (arms that deliberately leave one alive).
        subprocess.run(
            ["pkill", "-9", "-f", self.script], check=False, capture_output=True
        )


@pytest.fixture
def fixture(monkeypatch):
    # raising=False so that running this file against a tree without the fix
    # exercises the old fallback and fails on the lock oracle, rather than
    # erroring out here and proving nothing.
    monkeypatch.setattr(
        ClickHouseProc, "STOP_LOCK_WAIT_TIMEOUT_TRAP", TRAP_WAIT, raising=False
    )
    monkeypatch.setattr(
        ClickHouseProc, "STOP_LOCK_WAIT_TIMEOUT_KILL", KILL_WAIT, raising=False
    )
    signals = _SignalSpy(os.kill)
    monkeypatch.setattr(clickhouse_proc_module.os, "kill", signals)
    warnings = _WarningSpy()
    monkeypatch.setattr(clickhouse_proc_module, "Info", warnings)
    with tempfile.TemporaryDirectory(prefix="stop-server-lock-") as tmp_path:
        # `clickhouse stop --do-not-kill` returns 1 without killing when SIGTERM
        # does not land; stub that exit code so every arm reaches the fallback
        # deterministically and without waiting out --max-tries.
        bin_dir = os.path.join(tmp_path, "bin")
        os.makedirs(bin_dir)
        stub = os.path.join(bin_dir, "clickhouse")
        with open(stub, "w") as file:
            file.write("#!/bin/sh\nexit 1\n")
        os.chmod(stub, 0o755)
        monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")
        fix = _Fixture(tmp_path, signals, warnings)
        try:
            yield fix
        finally:
            fix.cleanup()


def _proc_for(servers):
    """A ClickHouseProc whose replica slots point at the fake servers.

    Built without `__init__`, which wipes the shared server log directory, mutates
    `CLICKHOUSE_*` in this process and needs a real config tree. `stop_server`
    reads only the attributes set below.
    """
    proc = ClickHouseProc.__new__(ClickHouseProc)
    names = (
        ("proc", "pid_file", "pid_0", "run_path0"),
        ("proc_1", "pid_file_replica_1", "pid_1", "run_path1"),
        ("proc_2", "pid_file_replica_2", "pid_2", "run_path2"),
    )
    for slot, server in zip(names, servers):
        proc_attr, pid_file_attr, pid_attr, run_path_attr = slot
        if server is None:
            setattr(proc, proc_attr, None)
            setattr(proc, pid_attr, 0)
            setattr(proc, pid_file_attr, None)
            setattr(proc, run_path_attr, None)
            continue
        run_path, pid_file, pid, popen, _ = server
        setattr(proc, proc_attr, popen)
        setattr(proc, pid_file_attr, pid_file)
        setattr(proc, pid_attr, pid)
        setattr(proc, run_path_attr, run_path)
    return proc


def test_wedged_server_is_killed_and_lock_released(fixture):
    """A server that ignores SIGTERM and SIGTRAP is escalated to SIGKILL."""
    server = fixture.start("run_r0")
    run_path, _, pid, _, _ = server
    fixture.signals.reset()
    _proc_for([server, None, None]).stop_server()
    assert _lock_is_free(run_path), "clickhouse local would still fail with Code 76"
    assert not _alive(pid)
    # Pin the whole ladder, not just its end state.
    assert fixture.signals.sent(signal.SIGTRAP) == [pid]
    assert fixture.signals.sent(signal.SIGKILL) == [pid]
    assert fixture.warnings.warnings == []


def test_wrapper_is_reaped(fixture):
    """The sh -c wrapper is waited for, so stop_server leaves no zombie behind."""
    server = fixture.start("run_r0")
    run_path, _, pid, popen, _ = server
    fixture.signals.reset()
    _proc_for([server, None, None]).stop_server()
    assert _lock_is_free(run_path), "clickhouse local would still fail with Code 76"
    assert popen.returncode is not None, "the wrapper was never waited for"
    assert _proc_state(popen.pid) != "Z", "the wrapper was left as a zombie"


def test_trap_honoured_needs_no_sigkill(fixture):
    """A server that dies on SIGTRAP releases the lock without escalation."""
    server = fixture.start("run_r0", trap_dies=True)
    run_path, _, pid, _, marker = server
    fixture.signals.reset()
    _proc_for([server, None, None]).stop_server()
    assert _lock_is_free(run_path)
    assert not _alive(pid)
    assert os.path.exists(marker), "server should have exited via its SIGTRAP handler"
    assert fixture.signals.sent(signal.SIGTRAP) == [pid]
    assert fixture.signals.sent(signal.SIGKILL) == []
    assert fixture.warnings.warnings == []


def test_trap_leg_waits_for_a_slow_core_instead_of_escalating(fixture):
    """The TRAP wait outlasts a server still writing its core.

    In scaled time TRAP_EXIT_DELAY stands for the fault handler's pre-core
    prologue plus core writing; escalating inside it would truncate the core.
    """
    server = fixture.start("run_r0", trap_dies=True, trap_delay=TRAP_EXIT_DELAY)
    run_path, _, pid, _, marker = server
    fixture.signals.reset()
    _proc_for([server, None, None]).stop_server()
    assert os.path.exists(marker), "the server was not left to finish on its own"
    assert _lock_is_free(run_path)
    assert fixture.signals.sent(signal.SIGTRAP) == [pid]
    assert fixture.signals.sent(signal.SIGKILL) == []
    assert fixture.warnings.warnings == []


def test_pid_not_named_by_status_file_is_not_signalled(fixture):
    """Fail closed on pid reuse: a pid the status file disowns is left alone."""
    server = fixture.start("run_r0")
    run_path, pid_file, pid, popen, _ = server
    # Simulate the pid having been reused: `status` still names the live holder,
    # while stop_server is told a different (also live) pid.
    other = subprocess.Popen(
        [sys.executable, "-c", f"import time; time.sleep({SERVER_SLEEP})"]
    )
    try:
        proc = _proc_for([(run_path, pid_file, other.pid, popen, None), None, None])
        fixture.signals.reset()
        proc.stop_server()
        # poll() rather than kill(pid, 0): `other` is this process's own child, so
        # once signalled it becomes a zombie that kill(pid, 0) still reports as
        # alive. poll() reaps it and returns the negated signal number.
        assert other.poll() is None, (
            f"an unrelated process must never be signalled (exit {other.poll()})"
        )
        assert _alive(pid), "the real holder is not named, so it is not signalled either"
        assert not _lock_is_free(run_path)
        assert fixture.signals.calls == [], (
            "nothing may be signalled once the identity check fails"
        )
        # The lost dump must be reported, not swallowed.
        assert len(fixture.warnings.warnings) == 1, fixture.warnings.warnings
        assert run_path in fixture.warnings.warnings[0]
    finally:
        other.kill()
        other.wait()


def test_lock_released_between_probe_and_identity_read_is_not_reported_lost(
    fixture, monkeypatch
):
    """A holder that exits inside the probe/identity window is not reported as lost.

    The window is made deterministic rather than raced: the holder is retired from
    inside the `head -1` call, so the lock is provably busy at the probe and the
    file provably gone at the identity read.
    """
    server = fixture.start("run_r0")
    run_path, _, pid, _, _ = server
    status = os.path.join(run_path, "status")
    real_get_output = clickhouse_proc_module.Shell.get_output

    def get_output(command, *args, **kwargs):
        if "head -1" in command:
            # Retire the holder the way a server completing its shutdown does:
            # StatusFile's destructor closes and unlinks the file it locked.
            fixture.signals.real_kill(pid, signal.SIGKILL)
            deadline = time.monotonic() + 30
            while _alive(pid) and time.monotonic() < deadline:
                time.sleep(0.1)
            if os.path.exists(status):
                os.unlink(status)
        return real_get_output(command, *args, **kwargs)

    monkeypatch.setattr(clickhouse_proc_module.Shell, "get_output", get_output)
    fixture.signals.reset()
    _proc_for([server, None, None]).stop_server()
    assert _lock_is_free(run_path)
    assert fixture.warnings.warnings == [], "a free lock must not be reported lost"
    assert fixture.signals.calls == [], "the disowns branch signals nothing"


def test_stale_status_file_of_dead_holder_is_free(fixture):
    """A status file whose holder already died needs no signal at all."""
    server = fixture.start("run_r0")
    run_path, pid_file, pid, popen, _ = server
    os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert os.path.exists(
        os.path.join(run_path, "status")
    ), "file must survive its holder"
    fixture.signals.reset()
    _proc_for([server, None, None]).stop_server()
    assert _lock_is_free(run_path)
    assert fixture.signals.calls == []
    assert fixture.warnings.warnings == []


def test_missing_status_file_is_not_an_error(fixture):
    """No status file means nothing holds the lock; stop_server must not raise."""
    server = fixture.start("run_r0")
    run_path, pid_file, pid, popen, _ = server
    os.kill(pid, signal.SIGKILL)
    deadline = time.monotonic() + 30
    while _alive(pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    os.unlink(os.path.join(run_path, "status"))
    _proc_for([server, None, None]).stop_server()
    assert _lock_is_free(run_path)
    assert fixture.warnings.warnings == []


def test_all_three_replicas_are_released(fixture):
    """r0/r1/r2 all go through the same loop body, so all three must be freed."""
    servers = [fixture.start(name) for name in ("run_r0", "run_r1", "run_r2")]
    fixture.signals.reset()
    _proc_for(servers).stop_server()
    for run_path, _, pid, _, _ in servers:
        assert _lock_is_free(run_path), f"{run_path}/status still locked"
        assert not _alive(pid)
    assert fixture.warnings.warnings == []


def test_prefix_fallback_leaves_lock_held(fixture):
    """Negative control: the pre-fix fallback signalled the shell, not the server.

    Without this arm the assertions above could hold for reasons unrelated to the
    fix, since a fixture whose server died on its own would satisfy them too.
    """
    run_path, _, pid, popen, _ = fixture.start("run_r0")
    popen.send_signal(signal.SIGTRAP)
    try:
        popen.wait(timeout=10)
    except subprocess.TimeoutExpired:
        popen.kill()
    deadline = time.monotonic() + 5
    while _alive(popen.pid) and time.monotonic() < deadline:
        time.sleep(0.1)
    assert not _alive(popen.pid), "the shell wrapper is what the old fallback killed"
    assert _alive(pid), "the server outlives its wrapper"
    assert not _lock_is_free(run_path), "old fallback left the lock held: Code 76"
