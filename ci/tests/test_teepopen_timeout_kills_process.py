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

A third failure mode: the watchdog slept the full timeout unconditionally, so a
child that exited early still tripped it -- it marked timeout_exceeded, ran the
cleanup, and, in a long-lived process, issued a late killpg against an
already-reaped (possibly PID-recycled) process group. The watchdog now waits on
an event set when the child is reaped and re-checks poll() before enforcing, so
an early-exiting child cancels it and no stray signal is sent.

These tests drive `_check_timeout` directly. Two of them launch a child that
sleeps far longer than the watchdog timeout (so a working watchdog must
terminate it promptly regardless of the cleanup); the fast-exit test launches a
child that finishes well before the timeout (so the watchdog must not fire).
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


def test_early_exit_does_not_trip_the_watchdog():
    """A child that finishes before the timeout must not trip the watchdog.

    The child exits in ~1s; the watchdog timeout is FAST_TIMEOUT. Pre-fix, the
    watchdog slept the full timeout and then unconditionally marked
    timeout_exceeded, ran the cleanup, and signalled the (already-reaped,
    possibly recycled) process group. Now it waits on an event set when the
    child is reaped and re-checks poll(), so it bails without enforcing.

    We wait past the timeout before asserting, giving a broken watchdog time to
    (wrongly) fire. cleanup_ran flips only if the watchdog reached the cleanup
    branch -- a direct probe that the watchdog did not act on a finished child.
    """
    cleanup_marker = f"/tmp/ch-slot-teepopen-cleanup-{os.getpid()}"
    if os.path.exists(cleanup_marker):
        os.remove(cleanup_marker)
    fast_timeout = 2
    child_sleep = 1

    with TeePopen(
        f"sleep {child_sleep}",
        timeout=fast_timeout,
        timeout_shell_cleanup=f"touch {cleanup_marker}",
    ) as p:
        rc = p.wait()

    # Give a broken (blind-sleep) watchdog time to wake and (wrongly) enforce.
    time.sleep(fast_timeout + 2)

    assert rc == 0, f"child should exit cleanly (rc={rc})"
    assert not p.timeout_exceeded, (
        "watchdog marked timeout_exceeded for a child that exited before the "
        "timeout -- the blind sleep fired instead of cancelling on early exit"
    )
    assert not os.path.exists(cleanup_marker), (
        "watchdog ran timeout_shell_cleanup for an early-exiting child -- it "
        "did not cancel when the process finished before the timeout"
    )
    assert not p.terminated_by_sigterm and not p.terminated_by_sigkill, (
        "watchdog signalled a process that had already exited"
    )


if __name__ == "__main__":
    t0 = time.monotonic()
    test_timeout_terminates_process_when_cleanup_is_a_noop()
    test_timeout_terminates_process_without_cleanup_command()
    test_slow_cleanup_does_not_gate_the_kill()
    test_early_exit_does_not_trip_the_watchdog()
    print(f"ok in {time.monotonic() - t0:.1f}s")
