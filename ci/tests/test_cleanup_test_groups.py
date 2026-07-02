"""
End-to-end test for the process-group orphan cleanup in tests/clickhouse-test.

Works on both Linux and macOS.  The ``pgrep()`` helper is imported directly
from ``tests/clickhouse-test`` so it uses ``ps -eo pid,ppid,pgid,command``
(POSIX) rather than the ``pgrep --pgroup`` system command (Linux-only).

Scenario
--------
1. ``clickhouse-test`` starts a subprocess (the "test process") in its own
   process group and writes the PGID to a file via ``write_text_atomic``
   right after ``Popen()``.  The test is ``01_parallel_sleep.sh``, which
   spawns 5 child ``sleep`` processes.
2. ``clickhouse-test`` is killed with ``SIGKILL``, leaving the test process
   (and its children) orphaned because the PGID file is never deleted.
3. We assert the test process and its 5 child processes are still alive: they
   live in their own process group, so the parent's ``SIGKILL`` cannot reach
   them.
4. We run ``clickhouse-test --cleanup``, which reads the file and kills all
   recorded process groups.
5. We assert all processes are now dead and the pid file is gone.
"""

import os
import runpy
import signal
import subprocess
import sys
import time
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")
_TEST = "01_parallel_sleep"

# Import helpers directly from clickhouse-test so path changes propagate
# automatically.  runpy.run_path handles the missing .py extension and the
# hyphen in the name.
_ct = runpy.run_path(_CLICKHOUSE_TEST)
pgrep = _ct["pgrep"]
_GROUP_PID_PATH = _ct["_GROUP_PID_PATH"]
_GROUP_PID_NAME = _ct["_GROUP_PID_NAME"]

# clickhouse-test uses --queries ci/tests, so per-test stdout files end up
# under ci/tests/0_stateless/ (args.tmp defaults to args.queries).
_STDOUT = _REPO_ROOT / "ci" / "tests" / "0_stateless" / "test.stdout"


def test_cleanup_kills_orphaned_test_process():
    """
    Verify that ``clickhouse-test --cleanup`` kills a test subprocess that was
    orphaned when its parent (clickhouse-test) was terminated with SIGKILL.
    """
    _GROUP_PID_PATH.mkdir(parents=True, exist_ok=True)
    for _f in _GROUP_PID_PATH.glob(f"{_GROUP_PID_NAME}.*"):
        _f.unlink(missing_ok=True)

    # Remove any leftover stdout files from a previous (possibly interrupted) run
    # so we get a clean signal when waiting for the file to appear below.
    _STDOUT.unlink(missing_ok=True)

    _ch_proc = subprocess.Popen(
        [sys.executable, _CLICKHOUSE_TEST, "--queries", "ci/tests", _TEST],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        try:
            # Wait for clickhouse-test to open the stdout file for the test, which
            # happens just before the test subprocess is launched.  This gives us
            # an early confirmation that the harness is actually running the test
            # rather than, e.g., still parsing options or connecting to the server.
            deadline_stdout = time.monotonic() + 15
            while time.monotonic() < deadline_stdout:
                if _STDOUT.exists() and _STDOUT.stat().st_size:
                    break
                time.sleep(0.5)
            else:
                assert False, f"{_STDOUT} is empty"

            # The PGID file is written by clickhouse-test synchronously right after
            # Popen(), before the bash script starts.  By the time the test script
            # writes its output and the stdout file has content, the file is likely to exist.
            p = list(_GROUP_PID_PATH.glob(f"{_GROUP_PID_NAME}.*"))[0]
            pgid = int(p.read_text())

            def got_procs(procs) -> str:
                return ", got\n" + '\n'.join(f"{p[0]} {p[3]}" for p in procs)

            # Poll until we see exactly 7 processes: two bash processes (the test
            # runner wrapper and the test script itself) plus 5 sleep subprocesses
            # spawned by the test script.
            deadline_procs = time.monotonic() + 5
            while time.monotonic() < deadline_procs:
                procs = pgrep(pgid=pgid)
                if len(procs) == 7:
                    break
                time.sleep(0.05)
            assert len(procs) == 7, "(Before kill) Expect 7 processes (two bash processes + 5 test processes)" + got_procs(procs)

            # Kill clickhouse-test with SIGKILL — simulates the OOM killer or an
            # external timeout killing the test runner.
        finally:
            os.kill(_ch_proc.pid, signal.SIGKILL)

        procs = pgrep(pgid=pgid)
        assert len(procs) == 7, "(After kill) Expect 7 processes" + got_procs(procs)
    finally:
        _ch_proc.wait()
        # Run clickhouse-test --cleanup to kill the orphaned process group.
        result = subprocess.run(
            [sys.executable, _CLICKHOUSE_TEST, "--cleanup"],
            capture_output=True,
            text=True,
            timeout=5,
        )

    assert result.returncode == 0, (
        f"clickhouse-test --cleanup failed (rc={result.returncode}):\n"
        f"{result.stdout}\n{result.stderr}"
    )

    # All test processes must now be dead.
    procs = pgrep(pgid=pgid)
    assert not procs, (
        "all test processes should be dead after clickhouse-test --cleanup" + got_procs(procs)
    )

