"""
Tests for `HUNG_CHECK_EXIT_CODE` in `tests/clickhouse-test`.

A failed `check_server_liveness` probe used to raise the default
`STOP_TESTING_EXIT_CODE`, which the job side reports as "Server died" - so a
harness-detected stall and a real death were indistinguishable in the report and
in CIDB. The probe verdict now travels as its own exit code.

Two layers are covered:

* the stop-cause carrier and the four decision sites, in-process (fast, no
  server);
* the exit code the process actually returns, end-to-end through a real run -
  the only layer that can see `killpg` clobbering the code, which is how the
  original bug would silently return.

The end-to-end tests need a running ClickHouse server (provided by the CI Tests
job). They drive the probe with a stub instead of an unresponsive server, so they
do not wait out the probe's real 65-165 s retry budget.
"""

import ast
import contextlib
import http.client
import io
import multiprocessing
import os
import signal
import subprocess
import sys
import threading
import types
from pathlib import Path

# Repo root so `ci.*` resolves; the `ci` dir so the bare `from praktika...`
# imports inside the job modules resolve the same way they do under the praktika
# job runner.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = _REPO_ROOT / "tests" / "clickhouse-test"

_MAIN_GUARD = 'if __name__ == "__main__":'

# A `<Fatal>` line as the server actually logs it. The leading space matters: the
# site matches `" <Fatal> "`, and stderr is `.strip()`ed before the match, so a
# bare `" <Fatal> ..."` fixture would silently stop matching.
_FATAL_LINE = "2026.08.16 08:35:02.123456 [ 582 ] {} <Fatal> BaseDaemon: signal 11\n"


def _load_runner():
    """Import `tests/clickhouse-test` as a module, stopping at its main guard.

    The file is not importable as-is (no `.py` suffix, and its `__main__` block
    parses argv), so the definitions are executed into a fresh namespace. Patching
    a name in that namespace is visible to every function defined in it, which is
    how the liveness probe is stubbed below.
    """
    source = _CLICKHOUSE_TEST.read_text(encoding="utf-8")
    definitions = source.split(_MAIN_GUARD, 1)[0]
    module = types.ModuleType("clickhouse_test_under_test")
    module.__file__ = str(_CLICKHOUSE_TEST)
    exec(compile(definitions, str(_CLICKHOUSE_TEST), "exec"), module.__dict__)
    return module


_runner = _load_runner()

HUNG_CHECK_EXIT_CODE = _runner.HUNG_CHECK_EXIT_CODE
STOP_TESTING_EXIT_CODE = _runner.STOP_TESTING_EXIT_CODE
GLOBAL_TIME_LIMIT_EXIT_CODE = _runner.GLOBAL_TIME_LIMIT_EXIT_CODE
MAX_FAILURES_EXIT_CODE = _runner.MAX_FAILURES_EXIT_CODE
FailureReason = _runner.FailureReason


# --- The stop-cause carrier ---------------------------------------------------


def test_carrier_keeps_the_first_writer_for_every_ordered_pair():
    """First writer wins, in both directions for every pair of causes.

    A one-sided check would also pass on a "always keep the last write"
    implementation, so each pair is driven in both orders.
    """
    codes = (
        STOP_TESTING_EXIT_CODE,
        GLOBAL_TIME_LIMIT_EXIT_CODE,
        MAX_FAILURES_EXIT_CODE,
        HUNG_CHECK_EXIT_CODE,
    )
    for first in codes:
        for second in codes:
            if first == second:
                continue
            carrier = multiprocessing.Value("i", 0)
            assert _runner.try_claim_stop_cause(carrier, first) is True
            assert carrier.value == first
            assert _runner.try_claim_stop_cause(carrier, second) is False
            assert carrier.value == first, (first, second)


class _LockScopeProbe:
    """A carrier whose reads and writes assert the lock is held at that moment.

    Asserting only that the lock was entered passes on an empty critical section
    with the read-modify-write outside it, which is the shape a refactor
    produces and the shape the unlocked race lives in.
    """

    def __init__(self, real_lock):
        self._real_lock = real_lock
        self._value = 0
        self.lock_held = False
        self.entered = 0

    def _check(self, what):
        assert self.lock_held, f"{what} of the carrier happened outside the lock"

    @property
    def value(self):
        self._check("read")
        return self._value

    @value.setter
    def value(self, new_value):
        self._check("write")
        self._value = new_value

    def peek(self):
        """Read for the test's own assertions, bypassing the lock check."""
        return self._value

    def get_lock(self):
        outer = self

        class RecordingLock:
            def __enter__(self):
                outer.entered += 1
                outer.lock_held = True
                return outer._real_lock.__enter__()

            def __exit__(self, *exc):
                outer.lock_held = False
                return outer._real_lock.__exit__(*exc)

        return RecordingLock()


def test_carrier_reads_and_writes_inside_the_lock():
    """The read and the write must be one locked transition. Two workers can
    detect different causes at the same time, and an unlocked read-modify-write
    lets the later one displace the earlier."""
    probe = _LockScopeProbe(multiprocessing.Value("i", 0).get_lock())
    assert _runner.try_claim_stop_cause(probe, HUNG_CHECK_EXIT_CODE) is True
    assert probe.peek() == HUNG_CHECK_EXIT_CODE
    assert probe.entered == 1, "the claim did not take the carrier's lock"


def test_lock_scope_probe_rejects_an_unlocked_read_modify_write():
    """The probe above is only an oracle if it actually rejects the defect. A
    claim implemented with an empty critical section must not pass it."""

    def claim_outside_the_lock(carrier, exit_code):
        with carrier.get_lock():
            pass
        if carrier.value == 0:
            carrier.value = exit_code

    probe = _LockScopeProbe(multiprocessing.Value("i", 0).get_lock())
    try:
        claim_outside_the_lock(probe, HUNG_CHECK_EXIT_CODE)
    except AssertionError as e:
        assert "outside the lock" in str(e), e
    else:
        raise AssertionError("the probe accepted an unlocked read-modify-write")


def test_every_carrier_write_goes_through_the_locked_helper():
    """No site assigns the carrier directly.

    The arms above pin the helper's own semantics, but each site starts from an
    empty carrier and writes before anything competes, so a plain
    `stop_exit_code.value = CODE` at any of them produces the same values and
    keeps every test green. Only the write's spelling separates the two, so that
    is what this asserts, over the whole file rather than a listed set of lines.
    """
    tree = ast.parse(_CLICKHOUSE_TEST.read_text(encoding="utf-8"))
    helper = next(
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.name == "try_claim_stop_cause"
    )
    inside_helper = {n for n in ast.walk(helper)}

    direct = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Assign) and node not in inside_helper
        for target in node.targets
        if isinstance(target, ast.Attribute)
        and target.attr == "value"
        and isinstance(target.value, ast.Name)
        and target.value.id == "stop_exit_code"
    ]
    assert not direct, f"carrier assigned outside try_claim_stop_cause at {direct}"

    claims = [
        node.lineno
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "try_claim_stop_cause"
    ]
    # Positive control: zero direct assignments is also what a file with no
    # carrier writes at all would report.
    assert len(claims) >= 5, f"expected every decision site to claim, found {claims}"


# --- The four decision sites --------------------------------------------------


def _make_test_case(tmp_path, stop=True):
    class Args:
        pass

    args = Args()
    args.stop = stop
    args.debug_log_file = str(tmp_path / "debug.log")
    args.bash_tracing_file = str(tmp_path / "bash.log")
    args.testcase_database = "test_db"

    case = _runner.TestCase.__new__(_runner.TestCase)
    case.args = args
    case.testcase_args = args
    case.name = "00001_probe"
    case.stdout_file = str(tmp_path / "stdout")
    case.stderr_file = str(tmp_path / "stderr")
    case.fatal_sanitizer_prefix = str(tmp_path / "sanitizer_")
    case.tags = set()
    return case


class _FailedProc:
    returncode = 1


def _reason_for_stderr(tmp_path, stderr, liveness):
    case = _make_test_case(tmp_path)
    Path(case.stderr_file).write_text(stderr, encoding="utf-8")
    saved = _runner.check_server_liveness
    _runner.check_server_liveness = lambda *a, **k: liveness
    try:
        return case.process_result_impl(_FailedProc(), 1.0).reason
    finally:
        _runner.check_server_liveness = saved


def test_probe_only_failure_is_not_reported_as_a_death(tmp_path):
    """A connection error plus a failed probe establishes only that the probe
    failed."""
    reason = _reason_for_stderr(tmp_path, "Connection refused\n", liveness=False)
    assert reason == FailureReason.LIVENESS_CHECK_FAILED


def test_fatal_evidence_is_still_reported_as_a_death(tmp_path):
    """The narrowness anchor: a `<Fatal>` line is evidence of a death and keeps
    its own reason."""
    reason = _reason_for_stderr(tmp_path, _FATAL_LINE, liveness=False)
    assert reason == FailureReason.SERVER_DIED


def test_fatal_evidence_outranks_a_failed_probe(tmp_path):
    """Both signals in one stderr. The two tests are mutually exclusive branches,
    so fatal evidence is never downgraded to a probe failure - as two independent
    `if`s would do, the second overwriting the first."""
    reason = _reason_for_stderr(
        tmp_path, _FATAL_LINE + "Connection refused\n", liveness=False
    )
    assert reason == FailureReason.SERVER_DIED


def test_connection_error_with_a_live_server_is_neither(tmp_path):
    """Unchanged behaviour: a transient error against a responding server is an
    ordinary test failure."""
    reason = _reason_for_stderr(tmp_path, "Connection refused\n", liveness=True)
    assert reason not in (
        FailureReason.SERVER_DIED,
        FailureReason.LIVENESS_CHECK_FAILED,
    )


def _reason_for_failed_health_check(tmp_path, liveness):
    """Drive `run`'s health-check site: `send_test_name_failed` raises, and the
    reason is then decided by the probe."""
    case = _make_test_case(tmp_path)
    case.case = "00001_probe.sql"
    # `run`'s exception path restores the client environment on the way out.
    case.base_url_params = ""
    case.base_client_options = ""

    class Suite:
        suite = "0_stateless"

    class Args:
        pass

    args = Args()
    args.testname = True
    args.cloud = False

    def boom(self, suite, case_name):
        raise RuntimeError("health check send failed")

    saved = {
        "send": _runner.TestCase.send_test_name_failed,
        "skip": _runner.TestCase.should_skip_test,
        "liveness": _runner.check_server_liveness,
        "configure": _runner.TestCase.configure_testcase_args,
    }
    _runner.TestCase.send_test_name_failed = boom
    _runner.TestCase.should_skip_test = lambda self, suite: None
    # Reached only on the probe-passes arm, where the site under test is not
    # taken; it stops the run at a known point instead of a real test execution.
    _runner.TestCase.configure_testcase_args = lambda *a, **k: (_ for _ in ()).throw(
        RuntimeError("stop after the health-check site")
    )
    _runner.check_server_liveness = lambda *a, **k: liveness
    try:
        return case.run(args, Suite(), "").reason
    finally:
        _runner.TestCase.send_test_name_failed = saved["send"]
        _runner.TestCase.should_skip_test = saved["skip"]
        _runner.TestCase.configure_testcase_args = saved["configure"]
        _runner.check_server_liveness = saved["liveness"]


def test_health_check_send_failure_reports_the_probe(tmp_path):
    """`send_test_name_failed` plus a failed probe. Its own comment concedes the
    ambiguity: the check "may fail because of memory exhaustion, for example"."""
    assert (
        _reason_for_failed_health_check(tmp_path, liveness=False)
        == FailureReason.LIVENESS_CHECK_FAILED
    )


def test_health_check_send_failure_with_a_live_server_is_not_the_probe(tmp_path):
    """Negative control for the test above: the reason comes from the probe, not
    merely from `send_test_name_failed` raising. Without this arm, a fixture that
    reached the site by accident would look like a pass."""
    assert (
        _reason_for_failed_health_check(tmp_path, liveness=True)
        != FailureReason.LIVENESS_CHECK_FAILED
    )


def _reason_for_connection_error(tmp_path, liveness, exception):
    """Drive `run`'s `except (ConnectionError, ImproperConnectionState)` handler.

    The exception is raised from inside the try-block, past the health-check
    site, so the reason comes from this handler's own probe and not from the
    earlier one.
    """
    case = _make_test_case(tmp_path)
    case.case = "00001_probe.sql"
    case.case_file = str(tmp_path / "00001_probe.sql")
    case.base_url_params = ""
    case.base_client_options = ""
    case.effective_settings = None
    case.effective_merge_tree_settings = None
    case.runs_count = 0

    class Suite:
        suite = "0_stateless"
        suite_tmp_path = str(tmp_path)

    class Args:
        pass

    args = Args()
    args.testname = False
    args.cloud = False

    saved = {
        "skip": _runner.TestCase.should_skip_test,
        "configure": _runner.TestCase.configure_testcase_args,
        "liveness": _runner.check_server_liveness,
    }
    _runner.TestCase.should_skip_test = lambda self, suite: None

    def raise_connection_error(*a, **k):
        raise exception

    _runner.TestCase.configure_testcase_args = raise_connection_error
    _runner.check_server_liveness = lambda *a, **k: liveness
    try:
        return case.run(args, Suite(), "").reason
    finally:
        _runner.TestCase.should_skip_test = saved["skip"]
        _runner.TestCase.configure_testcase_args = saved["configure"]
        _runner.check_server_liveness = saved["liveness"]


def test_connection_error_with_a_failed_probe_reports_the_probe(tmp_path):
    """The third probe-only site. A dropped connection plus a failed probe
    establishes only that the probe failed."""
    assert (
        _reason_for_connection_error(
            tmp_path, liveness=False, exception=ConnectionError("connection dropped")
        )
        == FailureReason.LIVENESS_CHECK_FAILED
    )


def test_connection_error_with_a_live_server_is_a_connection_error(tmp_path):
    """Negative control: the fall-through arm is reached, so the reason above
    comes from the probe rather than from the handler being entered at all."""
    assert (
        _reason_for_connection_error(
            tmp_path, liveness=True, exception=ConnectionError("connection dropped")
        )
        == FailureReason.CONNECTION_ERROR
    )


def test_improper_connection_state_with_a_failed_probe_reports_the_probe(tmp_path):
    """The handler catches two exception types and must treat them alike."""
    assert (
        _reason_for_connection_error(
            tmp_path,
            liveness=False,
            exception=http.client.ImproperConnectionState("bad state"),
        )
        == FailureReason.LIVENESS_CHECK_FAILED
    )


# --- Consuming the carrier ----------------------------------------------------
#
# Claiming a cause is only half of it: both places that turn a claim into an exit
# code have to read the carrier. These drive the two consumers directly, with the
# interleaving forced rather than raced, so there are no sleeps and no flakiness.


class _StubSuite:
    suite = "0_stateless"
    parallel_tests = ["00001_x"]
    sequential_tests = []


def _runner_args(**overrides):
    class Args:
        pass

    args = Args()
    args.jobs = 1
    args.no_self_parallel = True
    args.stop_time = None
    args.hung_check = False
    args.max_failures = 0
    args.max_failures_chain = 10**9
    args.client_option = []
    args.force_color = False
    args.sequential = None
    args.timeout = 600
    args.memory_limit = 0
    args.no_random_settings = True
    args.no_random_merge_tree_settings = True
    for name, value in overrides.items():
        setattr(args, name, value)
    return args


def _drive_sequential_stop_arm(carrier_value):
    """Drive `run_tests_array`'s `stop_testing already set` arm as the sequential
    runner, which owns the main process and raises rather than breaking."""
    stop_testing = multiprocessing.Event()
    stop_testing.set()
    carrier = multiprocessing.Value("i", carrier_value)

    saved = _runner.stop_tests
    _runner.stop_tests = lambda: None
    try:
        _runner.run_tests_array(
            (
                ["00001_x"],
                1,
                _StubSuite(),
                False,
                _runner_args(),
                multiprocessing.Value("i", 0),
                stop_testing,
                multiprocessing.Value("i", 0),
                [],
                1,
                multiprocessing.Value("i", 0),
                multiprocessing.Value("i", 0),
                1,
                carrier,
            )
        )
        return None
    except _runner.StopTesting as e:
        return e.exit_code
    finally:
        _runner.stop_tests = saved


def test_sequential_runner_forwards_a_claimed_cause():
    """A suite with sequential tests reaches this arm after the parallel workers
    are reaped, so it is the last chance to consume the carrier. Raising the
    default here is what reports a liveness abort as `Server died`."""
    assert _drive_sequential_stop_arm(HUNG_CHECK_EXIT_CODE) == HUNG_CHECK_EXIT_CODE
    assert _drive_sequential_stop_arm(MAX_FAILURES_EXIT_CODE) == MAX_FAILURES_EXIT_CODE


def test_sequential_runner_keeps_the_default_when_no_cause_was_claimed():
    """Mirror arm. Without it the test above also passes on an implementation
    that always returns the liveness code, and 0 must never become an exit
    code."""
    assert _drive_sequential_stop_arm(0) == STOP_TESTING_EXIT_CODE


class _EventHiddenOnce:
    """A real event whose first observation after `set()` reports False.

    The parent checks `stop_testing` at the top of its monitor loop but reaps
    workers at the bottom, and the reap is the loop's exit condition. This forces
    the claim to land in that gap on every run instead of waiting for the parent
    to be descheduled there.

    Hiding one observation is necessary but not sufficient: the reap only ends
    the loop once every worker is dead, so the hidden observation must be spent
    on an iteration that reaps the last one. Waiting for the children here makes
    that a precondition rather than a scheduling accident.
    """

    def __init__(self, timeout=30.0):
        self._inner = multiprocessing.Event()
        self._observations_after_set = 0
        self._timeout = timeout

    def set(self):
        self._inner.set()

    def is_set(self):
        if not self._inner.is_set():
            return False
        if self._observations_after_set == 0:
            for child in multiprocessing.active_children():
                child.join(timeout=self._timeout)
                if child.is_alive():
                    raise AssertionError(
                        f"worker {child.name} outlived the {self._timeout}s join, "
                        "so the reap cannot end the loop on the hidden observation"
                    )
        self._observations_after_set += 1
        return self._observations_after_set > 1


class _EventVisibleOnFirstCheck:
    """A real event whose `is_set()` waits, bounded, for a worker to set it.

    The inverse of `_EventHiddenOnce`: the parent's check at the top of the
    monitor loop cannot answer False before a worker has signalled, so the
    in-loop consumer is reached on every run instead of whenever the parent
    happens to be scheduled before the reap. Every detector claims its cause
    before setting the event, so a set event means the claim is already visible.
    A missed handshake raises instead of hanging.
    """

    def __init__(self, timeout=30.0):
        self._inner = multiprocessing.Event()
        self._timeout = timeout

    def set(self):
        self._inner.set()

    def is_set(self):
        if not self._inner.wait(timeout=self._timeout):
            raise RuntimeError(
                f"handshake missed: no worker signalled a stop within {self._timeout}s"
            )
        return True


def _drive_parent_monitor_loop(
    worker, stop_testing, raise_sites=None, args=None, clock=None
):
    """Run the parent monitor loop against a fake worker.

    `raise_sites` collects the line number each `raise_stop_from_carrier` call was
    made from, which is what distinguishes the in-loop consumer from the post-loop
    one. The teardown banner cannot: it is printed before the in-loop raise, so it
    still appears when that raise is deleted.

    `clock` replaces the runner's `time`, which is how an arm can decide when the
    global time limit reads as expired instead of depending on how long the fixture
    took to get here.
    """
    saved = (
        _runner.run_tests_process,
        _runner.raise_stop_from_carrier,
        _runner.time,
    )
    _runner.run_tests_process = worker
    if clock is not None:
        _runner.time = clock
    # The time-limit branch installs SIG_IGN for the rest of the run. In-process
    # that would outlive the test and leave the whole pytest session unable to be
    # terminated.
    saved_sigterm = signal.getsignal(signal.SIGTERM)

    if raise_sites is not None:
        real_raise = _runner.raise_stop_from_carrier

        def recording_raise(carrier):
            raise_sites.append(sys._getframe(1).f_lineno)
            real_raise(carrier)

        _runner.raise_stop_from_carrier = recording_raise

    output = io.StringIO()
    try:
        with contextlib.redirect_stdout(output):
            _runner.do_run_tests(
                1,
                _StubSuite(),
                args if args is not None else _runner_args(),
                multiprocessing.Value("i", 0),
                [],
                stop_testing,
                multiprocessing.Event(),
            )
        exit_code = None
    except _runner.StopTesting as e:
        exit_code = e.exit_code
    finally:
        (
            _runner.run_tests_process,
            _runner.raise_stop_from_carrier,
            _runner.time,
        ) = saved
        signal.signal(signal.SIGTERM, saved_sigterm)
    took_teardown_path = "terminating all processes" in output.getvalue()
    return exit_code, took_teardown_path


def _carrier_consumer_lines():
    """The two `raise_stop_from_carrier` call sites in the monitor loop, in source
    order: the in-loop consumer first, the post-loop one second.

    Derived from the source rather than hardcoded, so a refactor that moves either
    site keeps working - and one that adds or removes a site fails here loudly
    instead of silently making the arms below compare against the wrong line.
    """
    lines = [
        number
        for number, text in enumerate(
            _CLICKHOUSE_TEST.read_text(encoding="utf-8").splitlines(), start=1
        )
        if text.strip() == "raise_stop_from_carrier(stop_exit_code)"
    ]
    assert len(lines) == 2, f"expected 2 carrier consumers, found {lines}"
    return lines


_IN_LOOP_CONSUMER, _POST_LOOP_CONSUMER = _carrier_consumer_lines()


def _worker_claiming(exit_code):
    def worker(params):
        _runner.try_claim_stop_cause(params[13], exit_code)
        params[6].set()

    return worker


def _worker_claiming_nothing(params):
    params[6].set()


def _worker_signalling_nothing(params):
    """Exits without requesting a stop, which leaves the handshake unsatisfiable."""


def test_parent_consumes_a_cause_claimed_after_its_last_check():
    """A worker that claims and exits in the gap between the parent's check and
    the reap that ends the loop. Without a post-loop consumer the run reports an
    ordinary set of failures and the verdict is lost."""
    sites = []
    exit_code, took_teardown_path = _drive_parent_monitor_loop(
        _worker_claiming(HUNG_CHECK_EXIT_CODE), _EventHiddenOnce(), raise_sites=sites
    )
    assert exit_code == HUNG_CHECK_EXIT_CODE
    assert sites == [_POST_LOOP_CONSUMER], sites
    assert not took_teardown_path, "the in-loop path ran, so the gap was not hit"


def test_parent_keeps_the_default_for_a_stop_with_no_claimed_cause():
    """Mirror arm for the test above."""
    sites = []
    exit_code, took_teardown_path = _drive_parent_monitor_loop(
        _worker_claiming_nothing, _EventHiddenOnce(), raise_sites=sites
    )
    assert exit_code == STOP_TESTING_EXIT_CODE
    assert sites == [_POST_LOOP_CONSUMER], sites
    assert not took_teardown_path


def test_parent_still_consumes_a_cause_seen_inside_the_loop():
    """The unhidden path, so the two tests above cannot both pass on an
    implementation that only ever consumes the carrier after the loop.

    The observable is which consumer raised, not the teardown banner: that banner
    is printed before the in-loop raise, so it survives deleting it. The event
    blocks until the worker has claimed, so the in-loop path is reached on every
    run rather than whenever the parent is scheduled first.
    """
    sites = []
    exit_code, took_teardown_path = _drive_parent_monitor_loop(
        _worker_claiming(HUNG_CHECK_EXIT_CODE),
        _EventVisibleOnFirstCheck(),
        raise_sites=sites,
    )
    assert exit_code == HUNG_CHECK_EXIT_CODE
    assert sites == [_IN_LOOP_CONSUMER], sites
    assert took_teardown_path


def test_the_in_loop_handshake_is_not_vacuous():
    """`_EventVisibleOnFirstCheck` is only a forcing mechanism if an unmet
    handshake fails loudly. With a worker that never signals a stop it must raise,
    not pass and not hang."""
    try:
        _drive_parent_monitor_loop(
            _worker_signalling_nothing, _EventVisibleOnFirstCheck(timeout=1.0)
        )
    except RuntimeError as e:
        assert "no worker signalled a stop" in str(e), e
    else:
        raise AssertionError("the driver accepted a run nothing signalled in")


# --- Cause selection against an expired time limit ----------------------------
#
# The time limit is the one cause whose trigger is a clock, so end to end it can
# only be ordered against another cause by a budget that is wide enough to reach
# and narrow enough to lose - and process startup, including the manager and worker
# forks, happens inside it, which is unbounded on a shared runner. These arms own
# the clock instead, so the ordering does not depend on how fast the runner is.


_DEADLINE = 1.0
_PAST_THE_DEADLINE = 2.0


class _ClockReadAfterAWorkerSignals:
    """A clock whose every reading is past `_DEADLINE`, taken only once a worker has
    signalled a stop.

    Every detector claims its cause before setting `stop_testing`, so a set event
    means the claim is already visible. Gating the reading on it puts the parent's
    time-limit evaluation after the worker's claim on every run, with no sleep on
    either side and no elapsed real time in the comparison: the deadline and the
    reading are both constants. A missed handshake raises instead of hanging.
    """

    def __init__(self, signalled, timeout=30.0):
        self._signalled = signalled
        self._timeout = timeout

    def __call__(self):
        if not self._signalled.wait(timeout=self._timeout):
            raise RuntimeError(
                f"handshake missed: no worker signalled a stop within {self._timeout}s"
            )
        return _PAST_THE_DEADLINE


def _stop_code_against_an_expired_deadline(worker, handshake_timeout=30.0):
    stop_testing = multiprocessing.Event()
    exit_code, _ = _drive_parent_monitor_loop(
        worker,
        stop_testing,
        args=_runner_args(stop_time=_DEADLINE),
        clock=_ClockReadAfterAWorkerSignals(stop_testing, handshake_timeout),
    )
    return exit_code


def test_time_limit_does_not_displace_a_cause_claimed_before_it():
    """A worker claims the probe verdict, and only then is the parent's expired time
    limit evaluated. The parent must report 5, not 3.

    Master loses the verdict here and reports the benign `Global time limit
    reached`.
    """
    assert (
        _stop_code_against_an_expired_deadline(_worker_claiming(HUNG_CHECK_EXIT_CODE))
        == HUNG_CHECK_EXIT_CODE
    )


def test_an_expired_time_limit_still_claims_a_run_with_no_other_cause():
    """Mirror arm. Without it the test above also passes on an implementation that
    never claims the time limit at all, and the #106183 contract - a plain timeout is
    the benign stop condition - would go unpinned at this layer."""
    assert (
        _stop_code_against_an_expired_deadline(_worker_claiming_nothing)
        == GLOBAL_TIME_LIMIT_EXIT_CODE
    )


def test_the_deadline_handshake_is_not_vacuous():
    """The clock above is only a forcing mechanism if an unmet handshake fails
    loudly. With a worker that never signals a stop it must raise, not pass and not
    hang - otherwise the two arms above could be reading the clock before the claim
    and would be racing after all."""
    try:
        _stop_code_against_an_expired_deadline(
            _worker_signalling_nothing, handshake_timeout=1.0
        )
    except RuntimeError as e:
        assert "no worker signalled a stop" in str(e), e
    else:
        raise AssertionError("the driver accepted a run nothing signalled in")


# --- Not starting the probe once a stop is already pending --------------------
#
# The probe takes 65-165 s to conclude and drags a stacktrace sweep (up to 30 s
# per server process) behind it, so a run that is already tearing down must not
# start one. Whether it ran is not visible in the exit code - a second claim loses
# to the first either way - so the observable is the call count.


def _probe_calls(stop_testing, worker, hung_check=True):
    calls = []

    saved = (
        _runner.check_server_liveness,
        _runner.print_c_stacktraces,
        _runner.print_sql_stacktraces,
    )

    def counting_probe(*a, **k):
        calls.append(1)
        return False  # so a run that does probe still terminates

    _runner.check_server_liveness = counting_probe
    _runner.print_c_stacktraces = lambda *a, **k: None
    _runner.print_sql_stacktraces = lambda *a, **k: None
    try:
        exit_code, _ = _drive_parent_monitor_loop(
            worker, stop_testing, args=_runner_args(hung_check=hung_check)
        )
    finally:
        (
            _runner.check_server_liveness,
            _runner.print_c_stacktraces,
            _runner.print_sql_stacktraces,
        ) = saved
    return len(calls), exit_code


def test_a_pending_stop_prevents_a_new_liveness_probe():
    """A worker has claimed and signalled before the parent reaches the probe.

    `_EventVisibleOnFirstCheck` makes the guard's own `is_set()` wait for that
    signal, so the ordering is forced rather than raced. The claimed cause also has
    to survive, which is why the exit code is asserted too.
    """
    calls, exit_code = _probe_calls(
        _EventVisibleOnFirstCheck(), _worker_claiming(MAX_FAILURES_EXIT_CODE)
    )
    assert calls == 0, "a probe was started on a run that was already stopping"
    assert exit_code == MAX_FAILURES_EXIT_CODE


def test_the_probe_runs_while_no_stop_is_pending():
    """Positive control, mandatory: 0 calls is also what a run that never reached
    the probe produces, so the arm above proves nothing without this one."""
    calls, exit_code = _probe_calls(
        multiprocessing.Event(), _worker_signalling_nothing
    )
    assert calls >= 1
    assert exit_code == HUNG_CHECK_EXIT_CODE


def _parent_probe_carrier_with_a_competitor(competitor_code):
    """Drive the parent's probe site with a competitor claiming inside its sweep.

    The parent's carrier is created inside `do_run_tests`, so it is captured from
    the first claim rather than injected. The competitor is then given that same
    object, which is what makes the two claims genuinely compete.

    The competitor claims from inside the stubbed sweep rather than from a thread
    racing it. That is deterministic where a race is not, and it keeps the driver
    single-threaded: `do_run_tests` forks its workers, and forking a process that
    has started a thread is a documented deadlock hazard.
    """
    collected = []
    saved = (
        _runner.check_server_liveness,
        _runner.print_c_stacktraces,
        _runner.print_sql_stacktraces,
        _runner.try_claim_stop_cause,
    )
    real_claim = _runner.try_claim_stop_cause
    carriers = []

    def capturing_claim(carrier, exit_code):
        carriers.append(carrier)
        return real_claim(carrier, exit_code)

    def fake_collect(*a, **k):
        # Reached only from inside the probe's abort block, which is the window
        # the competing claim has to land in.
        assert carriers, (
            "the sweep opened with no cause claimed, so the claim is below it"
        )
        real_claim(carriers[0], competitor_code)
        collected.append(1)

    _runner.check_server_liveness = lambda *a, **k: False
    # On the first collector called, so the window observed is the whole one.
    _runner.print_sql_stacktraces = fake_collect
    _runner.print_c_stacktraces = lambda *a, **k: None
    _runner.try_claim_stop_cause = capturing_claim
    try:
        _drive_parent_monitor_loop(
            _worker_signalling_nothing,
            multiprocessing.Event(),
            args=_runner_args(hung_check=True),
        )
    finally:
        (
            _runner.check_server_liveness,
            _runner.print_c_stacktraces,
            _runner.print_sql_stacktraces,
            _runner.try_claim_stop_cause,
        ) = saved
    assert collected, "the sweep never ran, so nothing competed"
    return carriers[0].value


def test_the_parent_probe_claims_its_cause_before_collecting_stacktraces():
    """The same claim-before-collect invariant at the parent's probe site.

    The exit code cannot see this: under first-writer-wins a second claim loses
    either way, so the observable is which cause the carrier holds. The competitor
    uses the death code so the arm cannot pass merely because the hung-check code
    is also what an uncontested run produces.
    """
    assert (
        _parent_probe_carrier_with_a_competitor(STOP_TESTING_EXIT_CODE)
        == HUNG_CHECK_EXIT_CODE
    )


def test_the_probe_is_gated_on_the_hung_check_flag():
    """The other half of the guard, and a second reason the counter is not
    vacuous: without `--hung-check` the probe is never started."""
    calls, exit_code = _probe_calls(
        _EventVisibleOnFirstCheck(),
        _worker_claiming(MAX_FAILURES_EXIT_CODE),
        hung_check=False,
    )
    assert calls == 0
    assert exit_code == MAX_FAILURES_EXIT_CODE


# --- Claiming before collecting -----------------------------------------------


def _drive_abort_site_with_a_competitor(
    reason,
    competitor_code,
    start_competitor=True,
    handshake_timeout=30.0,
    is_concurrent=False,
    stop_tests_calls=None,
):
    """Drive an abort site with a real stacktrace-collection window.

    The competitor claims its own cause from inside the collection window, which
    is the only shape where the two claims genuinely compete. First-writer-wins
    keeps the detected cause only because the site claims before it collects.

    The interleaving is a two-way event handshake, not a pair of sleeps: the
    window opens, the competitor claims inside it, and only then does the window
    close. Sleeps would make the mutation-detection direction probabilistic - a
    competitor thread not scheduled within the window lets a claim that was moved
    below the collection win anyway, and the arm passes on the defect. Every wait
    is bounded so a stuck thread fails loudly instead of hanging a CI job.

    `start_competitor=False` leaves the second handshake unsatisfiable, which is
    how `test_the_competitor_handshake_is_not_vacuous` drives this helper's own
    failure mode.

    Passing a list as `stop_tests_calls` records each `stop_tests()` call instead
    of discarding it, so a caller can assert whether the site broadcast SIGTERM.
    The real function is never called: its `killpg` would signal this pytest
    process.
    """
    carrier = multiprocessing.Value("i", 0)
    collection_started = threading.Event()
    competitor_claimed = threading.Event()

    class FakeCase:
        def __init__(self, suite, case, args, concurrent):
            self.name = case

        def run(self, args, suite, client_options):
            return _runner.TestResult(
                self.name, _runner.TestStatus.FAIL, reason, 0.1, "x"
            )

        def process_result(self, result, messages):
            return result

    def fake_collect(*a, **k):
        collection_started.set()
        if not competitor_claimed.wait(timeout=handshake_timeout):
            raise RuntimeError(
                "handshake missed: the competitor did not claim inside the"
                f" collection window within {handshake_timeout}s"
            )

    def competitor():
        if not collection_started.wait(timeout=handshake_timeout):
            return
        _runner.try_claim_stop_cause(carrier, competitor_code)
        competitor_claimed.set()

    def record_stop_tests():
        if stop_tests_calls is not None:
            stop_tests_calls.append(1)

    saved = (
        _runner.print_c_stacktraces,
        _runner.print_sql_stacktraces,
        _runner.stop_tests,
        _runner.TestCase,
    )
    # On the first collector called, so the window observed is the whole one.
    _runner.print_sql_stacktraces = fake_collect
    _runner.print_c_stacktraces = lambda *a, **k: None
    _runner.stop_tests = record_stop_tests
    _runner.TestCase = FakeCase
    thread = (
        threading.Thread(target=competitor, daemon=True) if start_competitor else None
    )
    if thread:
        thread.start()
    try:
        with contextlib.redirect_stdout(io.StringIO()):
            _runner.run_tests_array(
                (
                    ["00001_x"],
                    1,
                    _StubSuite(),
                    is_concurrent,
                    _runner_args(),
                    multiprocessing.Value("i", 0),
                    multiprocessing.Event(),
                    multiprocessing.Value("i", 0),
                    [],
                    1,
                    multiprocessing.Value("i", 0),
                    multiprocessing.Value("i", 0),
                    1,
                    carrier,
                )
            )
    except _runner.StopTesting:
        pass
    finally:
        (
            _runner.print_c_stacktraces,
            _runner.print_sql_stacktraces,
            _runner.stop_tests,
            _runner.TestCase,
        ) = saved
        if thread:
            thread.join(timeout=handshake_timeout + 5)
    assert collection_started.is_set(), "the collection window did not open"
    assert competitor_claimed.is_set(), "the competitor's claim was not inside it"
    return carrier.value


def test_death_claims_its_cause_before_collecting_stacktraces():
    """The death path's claim, and its position before the collection. Asserting
    the raised code cannot see this: the sequential arm raises the default, which
    equals the death code either way. The carrier is the observable."""
    assert (
        _drive_abort_site_with_a_competitor(
            FailureReason.SERVER_DIED, HUNG_CHECK_EXIT_CODE
        )
        == STOP_TESTING_EXIT_CODE
    )


def test_the_competitor_handshake_is_not_vacuous():
    """The handshake above is only an oracle if an unmet one fails loudly. With no
    competitor thread the driver must raise, not pass and not hang."""
    try:
        _drive_abort_site_with_a_competitor(
            FailureReason.SERVER_DIED,
            HUNG_CHECK_EXIT_CODE,
            start_competitor=False,
            handshake_timeout=1.0,
        )
    except RuntimeError as e:
        assert "the competitor did not claim" in str(e), e
    else:
        raise AssertionError("the driver accepted a window nothing competed in")


def test_the_time_limit_does_not_win_the_liveness_collection_window():
    """The competitor is the global time limit, which is the cause that can fire on
    a clock while the worker is still collecting.

    The e2e arm for this shape has to make the run outlast a deadline to get here,
    which puts process startup inside the budget. Here the window is opened and
    closed by a handshake, so the interleaving holds however slow the runner is.
    """
    assert (
        _drive_abort_site_with_a_competitor(
            FailureReason.LIVENESS_CHECK_FAILED, GLOBAL_TIME_LIMIT_EXIT_CODE
        )
        == HUNG_CHECK_EXIT_CODE
    )


def test_liveness_claims_its_cause_before_collecting_stacktraces():
    """The same invariant on the liveness path, with the causes swapped so the
    test above cannot pass merely because the death code is also the default."""
    assert (
        _drive_abort_site_with_a_competitor(
            FailureReason.LIVENESS_CHECK_FAILED, STOP_TESTING_EXIT_CODE
        )
        == HUNG_CHECK_EXIT_CODE
    )


def _stop_tests_calls_on_the_liveness_path(is_concurrent):
    calls = []
    carrier = _drive_abort_site_with_a_competitor(
        FailureReason.LIVENESS_CHECK_FAILED,
        STOP_TESTING_EXIT_CODE,
        is_concurrent=is_concurrent,
        stop_tests_calls=calls,
    )
    # The claim happens at the same site, above the `stop_tests()` guard, so this
    # separates "the branch was not taken" from "the site was never reached" - a
    # count of zero means nothing without it.
    assert carrier == HUNG_CHECK_EXIT_CODE, carrier
    return len(calls)


def test_only_the_sequential_runner_broadcasts_sigterm_on_the_liveness_path():
    """`stop_tests()` is called on one side of the `is_concurrent` split only.

    Both sides exit 5, so the exit code cannot see this. The parallel half is the
    load-bearing one: `stop_tests()` broadcasts SIGTERM to the whole process group
    via `killpg`, so a parallel worker calling it kills the parent with 143 before
    it can re-raise, and 143 is itself reported as "Server died". The sequential
    runner owns the main process, so it must call it to tear its own children down.
    """
    assert _stop_tests_calls_on_the_liveness_path(is_concurrent=False) == 1
    assert _stop_tests_calls_on_the_liveness_path(is_concurrent=True) == 0


# --- Reason-string and artifact consumers -------------------------------------


def test_new_reason_is_matched_by_a_failure_pattern():
    """`TestCase.process_result` writes `reason.value` into the per-test output,
    and `ci/praktika/cidb.py` matches those strings to build the cause-filtered
    CIDB history link. A reason value no pattern matches loses that link.

    Only the new value is asserted: `INTERNAL_QUERY_FAIL` and `CONNECTION_ERROR`
    have no matching pattern today, so an enum-wide invariant is already false on
    master.
    """
    from ci.settings.settings import TEST_FAILURE_PATTERNS

    value = FailureReason.LIVENESS_CHECK_FAILED.value
    assert any(pattern in value for pattern in TEST_FAILURE_PATTERNS), value


def test_new_leaf_is_excluded_from_auto_revert():
    """A run-wide abort names no single change to revert."""
    from ci.jobs.revert_ci_regressions import SYNTHETIC_TEST_NAMES

    assert "Server liveness check failed" in SYNTHETIC_TEST_NAMES


# --- End to end: the exit code the process returns ----------------------------
#
# Only this layer can see the failure mode the original bug returns through: a
# worker's `stop_tests()` broadcasts SIGTERM to the whole process group via
# `killpg`, killing the parent with 143 before it can re-raise - and 143 is
# itself reported as "Server died". A test asserting only `TestResult.reason`
# passes while the run still reports a death.

_SHIM = r'''
import os, sys
runner = sys.argv.pop(1)
sys.argv[0] = runner
source = open(runner, encoding="utf-8").read()
guard = 'if __name__ == "__main__":'
definitions, main_block = source.split(guard, 1)
ns = {"__name__": "__main__", "__file__": runner}
exec(compile(definitions, runner, "exec"), ns)
if os.environ.get("STUB_LIVENESS_FAILS") == "1":
    ns["check_server_liveness"] = lambda *a, **k: False
if os.environ.get("STUB_HEALTH_CHECK_RAISES") == "1":
    # Reaches the WORKER decision site (`run`'s health-check block) rather than
    # the parent's periodic probe.
    def _boom(self, suite, case_name):
        raise RuntimeError("stub: health check send failed")
    ns["TestCase"].send_test_name_failed = _boom
# lldb over every server process blocks up to 30s each, and the SQL dump needs a
# live server, so neither collection is what these tests assert: both are replaced
# by markers they can observe.
def _stub_stacktraces(*a, **k):
    print("stub: print_c_stacktraces")
ns["print_c_stacktraces"] = _stub_stacktraces
def _stub_sql_stacktraces(*a, **k):
    print("stub: print_sql_stacktraces")
ns["print_sql_stacktraces"] = _stub_sql_stacktraces
# Prints one line per claim attempt, granted or not. The "Global time limit reached"
# banner sits inside the granted branch, so without this an attempt that was made and
# refused is indistinguishable from one that was never made.
if os.environ.get("STUB_TRACE_CLAIMS") == "1":
    _real_claim = ns["try_claim_stop_cause"]
    def _traced_claim(carrier, code):
        granted = _real_claim(carrier, code)
        print("stub: claim code=%d granted=%s" % (code, granted))
        return granted
    ns["try_claim_stop_cause"] = _traced_claim
exec(compile(guard + main_block, runner, "exec"), ns)
'''


def _make_suite(queries_dir: Path, count: int, seconds: int):
    """`count` parallel-safe `.sh` tests that pass after `seconds`, so the parent
    monitor loop gets a chance to run its periodic checks while they are in
    flight."""
    suite = queries_dir / "0_stateless"
    suite.mkdir(parents=True, exist_ok=True)
    for i in range(count):
        name = f"{i:05d}_sleep"
        script = suite / f"{name}.sh"
        script.write_text(f"#!/usr/bin/env bash\nsleep {seconds}\necho ok\n", encoding="utf-8")
        script.chmod(0o755)
        (suite / f"{name}.reference").write_text("ok\n", encoding="utf-8")


def _run_runner(
    queries_dir,
    extra_args,
    stub_liveness_fails,
    stub_health_check_raises=False,
    trace_claims=False,
    jobs=2,
    timeout=300,
):
    shim = Path(queries_dir) / "shim.py"
    shim.write_text(_SHIM, encoding="utf-8")
    env = dict(os.environ)
    env["STUB_LIVENESS_FAILS"] = "1" if stub_liveness_fails else "0"
    env["STUB_HEALTH_CHECK_RAISES"] = "1" if stub_health_check_raises else "0"
    env["STUB_TRACE_CLAIMS"] = "1" if trace_claims else "0"
    return subprocess.run(
        [
            sys.executable,
            str(shim),
            str(_CLICKHOUSE_TEST),
            "--queries",
            str(queries_dir),
            "--no-stateful",
            "-j",
            str(jobs),
            *extra_args,
            "00",  # name filter: matches the fixtures above
        ],
        cwd=str(_REPO_ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def _assert_exit_code(proc, expected):
    assert proc.returncode == expected, (
        f"expected {expected}, got {proc.returncode}\n"
        f"stdout:\n{proc.stdout[-4000:]}\nstderr:\n{proc.stderr[-4000:]}"
    )


def test_parent_hung_check_exits_with_the_hung_check_code(tmp_path):
    """The reported run: the parent's periodic probe fails while tests are still
    in flight. Master exits `STOP_TESTING_EXIT_CODE` here, which is reported as
    "Server died"."""
    _make_suite(tmp_path, count=2, seconds=6)
    proc = _run_runner(tmp_path, ["--hung-check"], stub_liveness_fails=True)
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)
    assert "Hung check failed" in proc.stdout
    # Not clobbered by a worker's SIGTERM broadcast.
    assert proc.returncode != 128 + 15


def test_hung_check_collects_stacktraces_before_stopping(tmp_path):
    """Diagnostics are what a stall investigation has to work from, so the sweep
    must still run on the new path."""
    _make_suite(tmp_path, count=2, seconds=6)
    proc = _run_runner(tmp_path, ["--hung-check"], stub_liveness_fails=True)
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)
    assert "stub: print_c_stacktraces" in proc.stdout
    # The SQL dump is the only source of query_id per stuck thread, and the probe
    # that failed here was HTTP while this dump goes over TCP.
    assert "stub: print_sql_stacktraces" in proc.stdout


def test_parallel_worker_probe_failure_exits_with_the_hung_check_code(tmp_path):
    """A WORKER reaches the verdict, not the parent's periodic probe.

    This is the arm that pins the `killpg` hazard: `stop_tests()` broadcasts
    SIGTERM to the whole process group, so a parallel worker calling it kills the
    parent with 143 before the parent can re-raise - and 143 is itself reported as
    "Server died", silently restoring the original bug. Only the process exit code
    can see that; a test inspecting `TestResult.reason` passes either way.
    """
    _make_suite(tmp_path, count=4, seconds=1)
    proc = _run_runner(
        tmp_path,
        ["--testname"],
        stub_liveness_fails=True,
        stub_health_check_raises=True,
        jobs=2,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)
    assert proc.returncode != 128 + 15, "the parent was SIGTERM-killed by a worker"


# `do_run_tests` prints this split before it picks a runner. `-j 1` only narrows
# the worker count, so the split is the observable that separates the two
# configurations.
_ALL_SEQUENTIAL = "Found 0 parallel tests and 2 sequential tests"


def test_sequential_worker_probe_failure_exits_with_the_hung_check_code(tmp_path):
    """The other side of the `is_concurrent` split: the sequential runner owns the
    main process, so it does call `stop_tests()` and must still exit 5.

    `--sequential` is what puts the fixtures on that path: `is_sequential_test`
    matches its substrings before consulting tags, so an empty `parallel_tests`
    makes `do_run_tests` skip the worker pool entirely and call `run_tests_array`
    with `is_concurrent=False`. Asserting the split is not decoration - without it
    this arm silently degrades into a second copy of the parallel one.
    """
    _make_suite(tmp_path, count=2, seconds=1)
    proc = _run_runner(
        tmp_path,
        ["--testname", "--sequential=sleep"],
        stub_liveness_fails=True,
        stub_health_check_raises=True,
        jobs=1,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)
    assert _ALL_SEQUENTIAL in proc.stdout, proc.stdout[-2000:]


def test_the_parallel_arm_is_not_secretly_sequential(tmp_path):
    """Negative control for the assertion above: the parallel arm's own
    configuration must not satisfy it, or it distinguishes nothing."""
    _make_suite(tmp_path, count=2, seconds=1)
    proc = _run_runner(
        tmp_path,
        ["--testname"],
        stub_liveness_fails=True,
        stub_health_check_raises=True,
        jobs=1,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)
    assert _ALL_SEQUENTIAL not in proc.stdout, proc.stdout[-2000:]


def test_responsive_server_finishes_normally(tmp_path):
    """Control: with the probe passing, `--hung-check` changes nothing."""
    _make_suite(tmp_path, count=2, seconds=1)
    proc = _run_runner(tmp_path, ["--hung-check"], stub_liveness_fails=False)
    _assert_exit_code(proc, 0)


def test_max_failures_is_not_conflated_with_the_hung_check(tmp_path):
    """`--max-failures` keeps its own code even with `--hung-check` on."""
    suite = tmp_path / "0_stateless"
    suite.mkdir(parents=True, exist_ok=True)
    for i in range(4):
        name = f"{i:05d}_fail"
        script = suite / f"{name}.sh"
        script.write_text("#!/usr/bin/env bash\necho fail\n", encoding="utf-8")
        script.chmod(0o755)
        (suite / f"{name}.reference").write_text("ok\n", encoding="utf-8")

    proc = _run_runner(
        tmp_path,
        ["--hung-check", "--max-failures", "0", "--max-failures-chain", "1"],
        stub_liveness_fails=False,
    )
    _assert_exit_code(proc, MAX_FAILURES_EXIT_CODE)


def test_the_claim_tracer_sees_a_time_limit_that_wins(tmp_path):
    """The tracer reports `granted=True` for a time limit that does win, so a
    `granted=False` reading from it distinguishes a refused claim from a broken
    tracer."""
    _make_suite(tmp_path, count=2, seconds=8)
    proc = _run_runner(
        tmp_path,
        ["--global_time_limit", "3"],
        stub_liveness_fails=False,
        trace_claims=True,
    )
    _assert_exit_code(proc, GLOBAL_TIME_LIMIT_EXIT_CODE)
    assert (
        f"stub: claim code={GLOBAL_TIME_LIMIT_EXIT_CODE} granted=True" in proc.stdout
    ), proc.stdout[-3000:]


def test_time_limit_still_claims_a_run_with_no_other_cause(tmp_path):
    """The mirror arm, without which the test above also passes on an
    implementation that never claims the time limit at all. The #106183 contract
    stays intact: a plain timeout is the benign stop condition."""
    _make_suite(tmp_path, count=2, seconds=8)
    proc = _run_runner(
        tmp_path,
        ["--global_time_limit", "3"],
        stub_liveness_fails=False,
    )
    _assert_exit_code(proc, GLOBAL_TIME_LIMIT_EXIT_CODE)


def _artifact_test_names(diagnostics_dir: Path):
    import json

    path = diagnostics_dir / _runner.RANDOM_SETTINGS_DIAGNOSTICS_FILE
    if not path.is_file():
        return []
    return [
        json.loads(line)["test_name"]
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_probe_failure_saves_no_random_settings_artifact(tmp_path):
    """The artifact means "this test fails under these settings", which a
    server-wide stall does not establish - and `--diagnose-random-settings`
    replays it, so a stall would be re-run as if it were a settings bug.

    The positive control is not optional: an empty artifact file is also what a
    misconfigured harness produces, so the negative arm alone proves nothing.
    Both arms run through the real guard in `run_tests_array`, driven by a real
    run - asserting on a re-implementation of the condition would pass whatever
    the runner actually does.
    """
    # Positive control: an ordinary output mismatch DOES get an artifact.
    diff_dir = tmp_path / "diff_run"
    diff_queries = tmp_path / "diff_queries"
    suite = diff_queries / "0_stateless"
    suite.mkdir(parents=True)
    script = suite / "00001_mismatch.sh"
    script.write_text("#!/usr/bin/env bash\necho fail\n", encoding="utf-8")
    script.chmod(0o755)
    (suite / "00001_mismatch.reference").write_text("ok\n", encoding="utf-8")

    proc = _run_runner(
        diff_queries,
        ["--random-settings-diagnostics-dir", str(diff_dir)],
        stub_liveness_fails=False,
    )
    assert proc.returncode != 0, proc.stdout[-2000:]
    assert _artifact_test_names(diff_dir) == ["00001_mismatch"], (
        "the positive control saved no artifact, so the negative arm below is "
        f"vacuous\nstdout:\n{proc.stdout[-2000:]}"
    )

    # Negative arm: a failed liveness probe does NOT. This must go through the
    # WORKER site, which is the only path where a per-test result carries the new
    # reason and so reaches the guard - the parent's periodic probe aborts the run
    # without ever producing such a result, which would make this arm vacuous.
    hung_dir = tmp_path / "hung_run"
    hung_queries = tmp_path / "hung_queries"
    _make_suite(hung_queries, count=4, seconds=1)

    proc = _run_runner(
        hung_queries,
        ["--testname", "--random-settings-diagnostics-dir", str(hung_dir)],
        stub_liveness_fails=True,
        stub_health_check_raises=True,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)
    assert _artifact_test_names(hung_dir) == []
