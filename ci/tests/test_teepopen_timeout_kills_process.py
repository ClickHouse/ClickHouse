"""
Regression test for the praktika watchdog (TeePopen._check_timeout).

Background
----------
`Stateless tests (amd_tsan, s3 storage, parallel, 2/2)` jobs intermittently
finished with a job-level ERROR (exit code 125) and `results: []`. The job.log
showed the 9000s watchdog firing, running a `docker rm -f <container>` that
removed nothing ("No such container"), and then the job hanging for another ~2h
until an unrelated docker-daemon event closed the pipe.

Root cause: when `timeout_shell_cleanup` was set, `_check_timeout` ran that
command and `return`ed early -- it never signalled the launched process group.
So if the cleanup command missed (which it did: the container was launched with
one name and `docker rm -f` targeted a different, stale name) the inner process
was never terminated and the whole job hung past its timeout. A second failure
mode: even when the signal was sent, running the cleanup synchronously first
meant a cleanup that wedged on a hung docker daemon still stalled the watchdog
before it could terminate the group.

The watchdog now signals the launched process group FIRST, then runs the
best-effort cleanup in a bounded daemon thread, so timeout enforcement never
depends on the cleanup returning.

These tests drive `_check_timeout` directly. The launched child sleeps far
longer than the watchdog timeout, so a working watchdog must terminate it
promptly regardless of what the cleanup command does.
"""

import os
import signal
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.praktika.utils import TeePopen

# Child sleeps much longer than the 1s watchdog timeout. A default `sleep`
# terminates on SIGTERM, so a working watchdog stops it in ~1s; a broken one
# (no signal sent) lets it run the full CHILD_SLEEP seconds.
CHILD_SLEEP = 30
# Generous ceiling: comfortably above the watchdog's ~1s reaction, far below
# CHILD_SLEEP so an un-killed child is unambiguously detected.
MAX_ELAPSED = 15


def _run_with_watchdog(timeout_shell_cleanup):
    start = time.monotonic()
    with TeePopen(
        f"sleep {CHILD_SLEEP}",
        timeout=1,
        timeout_shell_cleanup=timeout_shell_cleanup,
    ) as p:
        rc = p.wait()
    return p, rc, time.monotonic() - start


def test_timeout_terminates_process_when_cleanup_is_a_noop():
    """The exit-125 scenario: cleanup command removes nothing (no-op `true`)."""
    p, rc, elapsed = _run_with_watchdog(timeout_shell_cleanup="true")

    assert p.timeout_exceeded, "watchdog did not mark timeout_exceeded"
    # The launched process must have been terminated by the watchdog, not left
    # to run its full sleep. Pre-fix, the early return after the no-op cleanup
    # sent no signal, so the child ran all CHILD_SLEEP seconds (elapsed ~= 30,
    # rc == 0) -- the hang that ended in exit 125. We assert on the reaped exit
    # status and wall-clock, which are deterministic; the internal
    # `terminated_by_*` flags are set by the watchdog thread only after its
    # poll loop and lag behind the child's actual reaping, so are not asserted.
    assert rc == -signal.SIGTERM, (
        f"process was not SIGTERM-terminated (rc={rc}); the watchdog returned "
        "without signalling the launched process (the exit-125 hang)"
    )
    assert elapsed < MAX_ELAPSED, (
        f"watchdog took {elapsed:.1f}s to stop the process (>= {MAX_ELAPSED}s) -- "
        "it did not terminate promptly after the no-op cleanup"
    )


def test_timeout_terminates_process_without_cleanup_command():
    """The plain path (no cleanup command) must still terminate the group."""
    p, rc, elapsed = _run_with_watchdog(timeout_shell_cleanup=None)

    assert p.timeout_exceeded
    assert rc == -signal.SIGTERM, f"process was not SIGTERM-terminated (rc={rc})"
    assert elapsed < MAX_ELAPSED, f"watchdog took {elapsed:.1f}s (>= {MAX_ELAPSED}s)"


def test_slow_cleanup_does_not_gate_the_kill():
    """A cleanup that blocks (a wedged `docker rm -f`) must not delay the kill.

    The cleanup sleeps almost as long as the child. If the watchdog ran cleanup
    synchronously before signalling (the pre-fix ordering), the child would only
    be terminated after the cleanup returned -- elapsed >= SLOW_CLEANUP. Because
    the group is now signalled first and cleanup runs in a background thread, the
    child is stopped in ~1s while the cleanup keeps running detached.
    """
    slow_cleanup = f"sleep {CHILD_SLEEP - 5}"
    p, rc, elapsed = _run_with_watchdog(timeout_shell_cleanup=slow_cleanup)

    assert p.timeout_exceeded, "watchdog did not mark timeout_exceeded"
    assert rc == -signal.SIGTERM, (
        f"process was not SIGTERM-terminated (rc={rc}); a blocking cleanup "
        "gated the timeout kill"
    )
    assert elapsed < MAX_ELAPSED, (
        f"watchdog took {elapsed:.1f}s (>= {MAX_ELAPSED}s) -- the blocking "
        "cleanup delayed termination instead of running in the background"
    )


if __name__ == "__main__":
    t0 = time.monotonic()
    test_timeout_terminates_process_when_cleanup_is_a_noop()
    test_timeout_terminates_process_without_cleanup_command()
    test_slow_cleanup_does_not_gate_the_kill()
    print(f"ok in {time.monotonic() - t0:.1f}s")
