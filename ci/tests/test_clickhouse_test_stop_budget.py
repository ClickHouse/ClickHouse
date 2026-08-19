"""
`tests/clickhouse-test`: the abnormal-stop wait must outlast the worker cleanup.

When the parent stops a run (server death, hung check, global time limit,
`--max-failures`) it SIGTERMs each worker. A worker's SIGTERM handler runs
`kill_process_group`, which on a sanitized build deliberately sleeps for
`SANITIZER_REPORT_GRACE_SECONDS` so the sanitizer can print its report. If the
parent's wait is shorter than that grace, every worker is SIGKILLed part-way
through its cleanup: its own children survive (`p.kill()` does not reach the
worker's process group, and its PGID file is already unlinked) and its sanitizer
report is truncated.

The assertions here are relationships between the two values, not literals, so
they keep holding if either delay is retuned. Needs no server and no build.
"""

import runpy
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")


def _load():
    """Load `tests/clickhouse-test` fresh.

    `runpy.run_path` returns a *copy* of the module globals, so mutating the
    returned mapping would not be seen by the functions under test. Take the
    real globals mapping from one of the loaded functions instead.
    """
    ns = runpy.run_path(_CLICKHOUSE_TEST)
    return ns["worker_cleanup_grace_seconds"].__globals__


@pytest.fixture(name="ct")
def _ct():
    return _load()


def _configure(ct, *, sanitized, capture=False, release_non_sanitized=False):
    ct["SANITIZED"] = sanitized
    ct["CAPTURE_CLIENT_STACKTRACE"] = capture
    ct["RELEASE_NON_SANITIZED"] = release_non_sanitized


class _FakeWorker:
    """Stands in for a `multiprocessing.Process` worker.

    `alive_for` is how long the worker keeps running after `terminate()`, in the
    fake clock's units; `None` means it never exits on its own.
    """

    def __init__(self, clock, alive_for, name="Process-1", pid=1234):
        self._clock = clock
        self._alive_for = alive_for
        self._terminated_at = None
        self.name = name
        self.pid = pid
        self.terminated = False
        self.killed = False
        self.killed_at = None
        self.joins = []

    def is_alive(self):
        if self.killed:
            return False
        if not self.terminated:
            return True
        if self._alive_for is None:
            return True
        return self._clock.now < self._terminated_at + self._alive_for

    def terminate(self):
        self.terminated = True
        self._terminated_at = self._clock.now

    def kill(self):
        self.killed = True
        self.killed_at = self._clock.now

    def join(self, timeout=None):
        """Advance the fake clock the way a real bounded join would: until the
        worker exits, or until the timeout expires - whichever comes first."""
        self.joins.append(timeout)
        assert timeout is not None, "an unbounded join can hang the run forever"
        deadline = self._clock.now + timeout
        while self._clock.now < deadline and self.is_alive():
            self._clock.now = min(deadline, self._clock.now + 0.1)


class _Clock:
    def __init__(self):
        self.now = 1000.0
        self.slept = []


def _run_stop(ct, workers, return_clock=False, wall_clock_factory=None):
    """Drive `terminate_workers` on a fake clock and return elapsed fake time.

    `sleep` is redirected into the fake clock too: the stop path must reach its
    decisions by waiting on the workers, never by sleeping a fixed budget. A
    real `sleep` here would also make this test take as long as the grace.

    Both clock names are redirected. `wall_clock_factory` replaces only the
    non-monotonic one, which is how the discontinuity arms tell the two apart: a
    steady `monotonic` and a jumping `time` diverge only if the code reads the
    wrong one.
    """
    clock = _Clock()
    for w in workers:
        w._clock = clock

    def _fake_sleep(seconds):
        clock.slept.append(seconds)
        clock.now += seconds

    ct["monotonic"] = lambda: clock.now
    ct["time"] = (
        wall_clock_factory(clock) if wall_clock_factory is not None else lambda: clock.now
    )
    ct["sleep"] = _fake_sleep
    started = clock.now
    ct["terminate_workers"](workers)
    if return_clock:
        return clock.now - started, clock
    return clock.now - started


class _JumpingWallClock:
    """A wall clock that steps by `jump` from its `at_call`-th reading onwards.

    Models an NTP correction or a VM resume landing in the middle of the stop
    loop. `monotonic` cannot do this, which is the whole point of using it.
    """

    def __init__(self, clock, jump, at_call):
        self._clock = clock
        self._jump = jump
        self._at_call = at_call
        self.calls = 0

    def __call__(self):
        self.calls += 1
        offset = self._jump if self.calls >= self._at_call else 0
        return self._clock.now + offset


# --- 1. the budget outlasts the grace the worker actually executes -----------


def test_sanitized_wait_outlasts_the_cleanup_grace(ct):
    """FAILS ON MASTER: the wait was a flat 10 s against a 60 s grace."""
    _configure(ct, sanitized=True)
    grace = ct["worker_cleanup_grace_seconds"]()
    assert grace > 0, "a sanitized build must have a non-zero cleanup grace"

    worker = _FakeWorker(_Clock(), alive_for=grace)
    _run_stop(ct, [worker])

    assert not worker.killed, (
        f"worker was SIGKILLed mid-cleanup: it needs {grace}s, "
        f"the wait allowed {worker.joins[0]}s"
    )
    assert worker.joins[0] > grace


def test_grace_matches_the_sleep_kill_process_group_performs(ct, monkeypatch):
    """Pins the grace to the delay the worker really sleeps for, so the two
    cannot drift apart again. Observes the sleep instead of trusting a constant."""
    _configure(ct, sanitized=True)
    slept = []
    ct["sleep"] = slept.append
    ct["pgrep"] = lambda **_: []
    # `ct["os"]` is the real `os` module, shared with the rest of the test
    # session, so this patch has to be undone - monkeypatch does that for us.
    monkeypatch.setattr(ct["os"], "killpg", lambda *_: None)

    ct["kill_process_group"](999999, None)

    # The last sleep is the fixed SIGTERM->SIGKILL delay the function ends with;
    # every sleep before it is grace the parent has to outlast. Identified by
    # position, so retuning any delay cannot drop it from the sum.
    assert len(slept) >= 2, f"expected a grace sleep and a final one, got {slept}"
    assert sum(slept[:-1]) == ct["worker_cleanup_grace_seconds"]()


def test_capture_client_stacktrace_delay_is_additive(ct):
    """`kill_process_group` sleeps for the sanitizer grace and then again for the
    SIGTSTP delay, so the budget must cover their sum."""
    _configure(ct, sanitized=True, capture=True)
    with_capture = ct["worker_cleanup_grace_seconds"]()
    _configure(ct, sanitized=True, capture=False)
    without = ct["worker_cleanup_grace_seconds"]()

    _configure(ct, sanitized=True, capture=True)
    assert with_capture == without + ct["client_stacktrace_delay_seconds"]()


# --- 2. non-sanitized builds are unchanged ----------------------------------


def test_non_sanitized_wait_is_unchanged(ct):
    """Without the sanitizer grace and without stacktrace capture there is nothing
    to outlast, so the wait stays what it was."""
    _configure(ct, sanitized=False)
    assert ct["worker_cleanup_grace_seconds"]() == 0

    worker = _FakeWorker(_Clock(), alive_for=None)
    _run_stop(ct, [worker])
    assert worker.joins[0] == ct["WORKER_STOP_MARGIN_SECONDS"]


def test_no_worker_waits_less_than_the_margin(ct):
    """FAILS ON A DEADLINE-ONLY BUDGET, and that failure is a regression against
    the sequence this replaces.

    Every worker is already terminated when the wait loop starts, so a purely
    shared deadline gives the first slow worker the whole budget and polls all the
    others with `timeout=0` - killing them mid-cleanup where the historical
    per-worker `join(timeout=10)` would have let them finish. The margin is a floor
    under every individual wait, which restores exactly that allowance: here the
    first worker is still killed (it outlives any single 10 s window, as it did
    before), and every later one survives, as it also did before.
    """
    _configure(ct, sanitized=False)
    margin = ct["WORKER_STOP_MARGIN_SECONDS"]
    assert ct["worker_cleanup_grace_seconds"]() == 0
    # Slower than one margin, so the first worker consumes the whole shared budget
    # and every later worker has to rely on the floor.
    workers = [
        _FakeWorker(_Clock(), alive_for=margin * 1.5, name=f"Process-{i}", pid=200 + i)
        for i in range(8)
    ]

    _run_stop(ct, workers)

    assert not any(w.killed for w in workers[1:]), (
        "workers were SIGKILLed without a wait once the shared deadline passed, "
        f"which the code this replaces did not do: waits={[w.joins[0] for w in workers]}"
    )
    assert workers[-1].joins[0] >= margin, (
        f"the last worker got {workers[-1].joins[0]}s, below the {margin}s floor"
    )


def test_non_sanitized_capture_build_covers_the_stacktrace_delay(ct):
    """A non-sanitized build still sleeps for the SIGTSTP delay when stacktrace
    capture is on, and the functional and Fast test lanes always turn it on. The
    budget has to cover that delay without picking up the sanitizer grace."""
    _configure(ct, sanitized=False, capture=True)
    grace = ct["worker_cleanup_grace_seconds"]()

    assert grace == ct["client_stacktrace_delay_seconds"]()
    assert grace > 0, "the capture delay must be part of the budget here"
    # The sanitizer grace belongs to sanitized builds only.
    assert grace < ct["SANITIZER_REPORT_GRACE_SECONDS"]

    worker = _FakeWorker(_Clock(), alive_for=grace)
    _run_stop(ct, [worker])
    assert not worker.killed, (
        f"worker was SIGKILLed mid-cleanup: it needs {grace}s, "
        f"the wait allowed {worker.joins[0]}s"
    )
    assert worker.joins[0] == grace + ct["WORKER_STOP_MARGIN_SECONDS"]


# --- 4/5. the hang fix (ddc6bcfb) is preserved ------------------------------


def test_wedged_worker_is_force_killed_and_the_stop_completes(ct):
    """A worker that never exits must still be SIGKILLed and the stop must
    return - the hours-long hang `ddc6bcfb` fixed must not come back."""
    _configure(ct, sanitized=True)
    worker = _FakeWorker(_Clock(), alive_for=None)

    elapsed = _run_stop(ct, [worker])

    assert worker.terminated and worker.killed
    budget = ct["worker_cleanup_grace_seconds"]() + ct["WORKER_STOP_MARGIN_SECONDS"]
    assert elapsed == pytest.approx(budget, abs=1), (
        "the stop must be bounded by the budget"
    )
    assert all(t is not None for t in worker.joins)


def test_escalation_order_is_terminate_join_kill_join(ct):
    _configure(ct, sanitized=True)
    worker = _FakeWorker(_Clock(), alive_for=None)
    _run_stop(ct, [worker])
    # one bounded wait before the kill, one after it
    assert len(worker.joins) == 2
    assert worker.joins[-1] == 5


# --- 6. the budget is a ceiling, not a sleep --------------------------------


def test_prompt_worker_is_not_delayed_by_the_larger_budget(ct):
    """Catches "fixed it by always waiting 70 s", which would add a minute to
    every sanitized abnormal stop."""
    _configure(ct, sanitized=True)
    worker = _FakeWorker(_Clock(), alive_for=0.5)

    elapsed, clock = _run_stop(ct, [worker], return_clock=True)

    assert not worker.killed
    assert elapsed < 5, f"stop took {elapsed}s for a worker that exited at once"
    # A `sleep` here would burn the budget whether or not the worker has exited,
    # so the stop path must not sleep at all - it waits on the workers.
    assert clock.slept == [], f"stop path slept {clock.slept} instead of waiting"


def test_many_workers_share_one_deadline(ct):
    """The grace is per stop, not per worker: 8 workers must not cost 8 budgets.

    Every worker uses its full grace and exits only at the deadline, so a
    per-worker budget shows up as N times the wall time.
    """
    _configure(ct, sanitized=True)
    grace = ct["worker_cleanup_grace_seconds"]()
    workers = [
        _FakeWorker(_Clock(), alive_for=grace, name=f"Process-{i}", pid=100 + i)
        for i in range(8)
    ]

    elapsed = _run_stop(ct, workers)

    assert not any(w.killed for w in workers)
    budget = grace + ct["WORKER_STOP_MARGIN_SECONDS"]
    assert elapsed < 2 * budget, (
        f"8 workers took {elapsed}s; a per-worker budget would cost ~{8 * budget}s"
    )
    # The waits must shrink as the shared deadline approaches. With a per-worker
    # budget every wait is the same length instead.
    first_waits = [w.joins[0] for w in workers]
    assert first_waits[-1] < first_waits[0], (
        f"each worker was given its own budget, not a shared deadline: {first_waits}"
    )


# --- the deadline survives a wall-clock adjustment --------------------------


def test_forward_clock_step_does_not_cut_a_wait_short(ct):
    """FAILS ON A `time()`-BASED DEADLINE: an NTP correction or a VM resume during
    the stop would push the wall clock past the deadline and SIGKILL workers that
    are still inside their own allowance - the very defect this change removes."""
    _configure(ct, sanitized=True)
    grace = ct["worker_cleanup_grace_seconds"]()
    workers = [
        _FakeWorker(_Clock(), alive_for=grace, name=f"Process-{i}", pid=300 + i)
        for i in range(3)
    ]

    # Jump far past the whole budget, from the second reading onwards.
    _run_stop(
        ct,
        workers,
        wall_clock_factory=lambda clock: _JumpingWallClock(
            clock, jump=grace + 3600, at_call=2
        ),
    )

    assert not any(w.killed for w in workers), (
        f"a wall-clock step killed workers mid-cleanup: {[w.killed for w in workers]}"
    )


def test_backward_clock_step_does_not_extend_the_budget(ct):
    """A backward wall-clock step must not enlarge any wait, or `ddc6bcfb`'s
    bounded escalation stops being bounded."""
    _configure(ct, sanitized=True)
    budget = ct["worker_cleanup_grace_seconds"]() + ct["WORKER_STOP_MARGIN_SECONDS"]
    worker = _FakeWorker(_Clock(), alive_for=None)

    elapsed = _run_stop(
        ct,
        [worker],
        wall_clock_factory=lambda clock: _JumpingWallClock(
            clock, jump=-(budget * 10), at_call=2
        ),
    )

    assert worker.killed, "the wedged worker must still be force-killed"
    assert elapsed == pytest.approx(budget, abs=1), (
        f"a backward wall-clock step stretched the stop to {elapsed}s (budget {budget}s)"
    )


# --- the production stop path uses the helper --------------------------------


def test_stop_path_calls_the_budgeted_helper(ct):
    """The arms above all drive `terminate_workers` directly, so none of them can
    see whether anything still calls it. Without this, restoring the historical
    inline `join(timeout=10)` sequence at the call site leaves them all green while
    the fix is entirely disabled. `do_run_tests` cannot be driven without a server
    and the property is purely "the stop branch delegates", so assert on its source.
    """
    import inspect

    source = inspect.getsource(ct["do_run_tests"])

    assert "terminate_workers(" in source, (
        "the stop path no longer calls terminate_workers, so the derived budget "
        "is dead code"
    )
    assert "join(timeout=10)" not in source, (
        "the stop path has an inline flat 10 s join again, bypassing the budget"
    )


# --- the harness itself must be able to fail --------------------------------


def test_fake_worker_reports_a_kill_when_the_wait_is_too_short(ct):
    """Negative control: with the pre-fix flat 10 s wait, a worker that needs the
    full sanitizer grace IS killed. If this ever passes without a kill, the fake
    worker has stopped modelling the defect and every arm above is vacuous."""
    _configure(ct, sanitized=True)
    grace = ct["worker_cleanup_grace_seconds"]()
    clock = _Clock()
    worker = _FakeWorker(clock, alive_for=grace)
    ct["time"] = lambda: clock.now

    worker.terminate()
    worker.join(timeout=10)  # the master budget
    if worker.is_alive():
        worker.kill()

    assert worker.killed, "the fake worker no longer reproduces the defect"


def test_loading_the_script_does_not_patch_the_shared_os_module():
    """Guard for this file, not for the fix: the loaded globals share the real
    `os`/`signal` modules with the whole pytest session, so a stray patch here
    would break unrelated tests that call `os.killpg` (it did once)."""
    import os as real_os

    before = real_os.killpg
    ct = _load()
    _configure(ct, sanitized=True)
    _run_stop(ct, [_FakeWorker(_Clock(), alive_for=0.1)])
    assert real_os.killpg is before, "a test leaked a patch into the real os module"
