"""
Regression tests for the hung-check liveness schedule in `do_run_tests`
(tests/clickhouse-test).

The probe used to sit in the parent's `while processes: sleep(0.1)` loop with no
interval gate and with `check_server_liveness`'s whole 10-attempt / up-to-165 s
retry window running synchronously. A healthy 3 h run therefore issued ~10^5
`SELECT 1` probes, and while the server was slow the parent blocked continuously,
running neither the memory-pressure worker shed, nor the `args.stop_time`
deadline check, nor the `stop_testing` handling.

The probe is now interval-gated and takes one attempt per poll, with master's
10-attempt / 65 s back-off ladder reproduced as loop-owned state. These tests pin
the two properties that must NOT change (the 65 s instant-refusal and 165 s
socket-timeout abort budgets) as well as the ones that are new (the interval, the
one-attempt-per-poll contract, the deadline confirmation and the terminal drain).

`do_run_tests` is not callable in isolation, so the checks are two-pronged:
structural over its AST, and behavioural by running the real function on a fake
clock with fake workers and a stubbed probe.
"""

import argparse
import ast
import contextlib
import inspect
import io
import re
import runpy
import signal
import textwrap
import types
from pathlib import Path

_CLICKHOUSE_TEST = str(
    Path(__file__).resolve().parent.parent.parent / "tests" / "clickhouse-test"
)

VERDICT = "Hung check failed: server is not responding"

# Offset applied to the fake clock so absolute timestamps look realistic.
_T0 = 1_000_000.0


def _load():
    # runpy.run_path does NOT execute __main__, so no argument parsing happens.
    return runpy.run_path(_CLICKHOUSE_TEST)


def _do_run_tests_ast():
    fn_src = textwrap.dedent(inspect.getsource(_load()["do_run_tests"]))
    return ast.parse(fn_src).body[0]


def _worker_loop(fn):
    """The parent's `while processes:` polling loop."""
    for node in ast.walk(fn):
        if (
            isinstance(node, ast.While)
            and isinstance(node.test, ast.Name)
            and node.test.id == "processes"
        ):
            return node
    raise AssertionError("`while processes:` loop not found in do_run_tests")


def _loop_body_ifs(fn):
    return [s for s in _worker_loop(fn).body if isinstance(s, ast.If)]


def _probe_calls(node):
    return [
        child
        for child in ast.walk(node)
        if isinstance(child, ast.Call)
        and isinstance(child.func, ast.Name)
        and child.func.id == "check_server_liveness"
    ]


def _hung_check_if(fn):
    # Selected by the probe call it contains, not by mentioning `args.hung_check`:
    # the deadline block's condition mentions it too.
    for node in _loop_body_ifs(fn):
        if "args.hung_check" in ast.unparse(node.test) and _probe_calls(node):
            return node
    raise AssertionError("the polled `args.hung_check` block was not found in the loop")


def _terminal_drain(fn):
    """The hung-check drain that runs after the polling loop exits."""
    loop = _worker_loop(fn)
    for node in ast.walk(fn):
        for _, value in ast.iter_fields(node):
            if not isinstance(value, list) or loop not in value:
                continue
            for stmt in value[value.index(loop) + 1 :]:
                if (
                    isinstance(stmt, ast.If)
                    and "args.hung_check" in ast.unparse(stmt.test)
                    and _probe_calls(stmt)
                ):
                    return stmt
    raise AssertionError(
        "no `args.hung_check` drain found as a SIBLING of the `while processes:` "
        "loop; a death after the last probe would escape the check once the "
        "workers finish"
    )


# ---------------------------------------------------------------------------
# structural
# ---------------------------------------------------------------------------


def test_probe_takes_one_attempt_per_poll():
    # The retry window must belong to the loop, not to the callee: otherwise one
    # firing blocks the parent for the whole 65-165 s window.
    calls = _probe_calls(_hung_check_if(_do_run_tests_ast()))
    assert len(calls) == 1, "expected exactly one probe call in the gated block"
    kwargs = {kw.arg: ast.unparse(kw.value) for kw in calls[0].keywords}
    assert kwargs.get("max_retries") == "1", (
        "the polled probe must pass max_retries=1; without it the callee owns the "
        "whole retry window and blocks the parent loop"
    )


def test_gate_uses_the_schedule_constants():
    fn = _do_run_tests_ast()
    src = ast.unparse(_hung_check_if(fn))
    assert "hung_check_delay" in src
    assert "HUNG_CHECK_MAX_FAILURES" in src
    assert "HUNG_CHECK_INTERVAL" in src
    # The interval is what bounds the healthy-run probe load.
    assigned = {
        target.id: ast.unparse(node.value)
        for node in ast.walk(fn)
        if isinstance(node, ast.Assign)
        for target in node.targets
        if isinstance(target, ast.Name)
    }
    assert assigned["HUNG_CHECK_INTERVAL"] == "30.0"
    assert assigned["HUNG_CHECK_MAX_FAILURES"] == "10"


def test_last_hung_check_starts_unset_and_is_stamped_after_the_probe():
    fn = _do_run_tests_ast()
    init = [
        ast.unparse(node.value)
        for node in ast.walk(fn)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "last_hung_check" for t in node.targets
        )
    ]
    assert init[0] == "None", (
        "last_hung_check must start as None so the first poll fires immediately; "
        "initialising it to time() delays the first probe by a whole interval"
    )
    block = _hung_check_if(fn)
    stamp = [
        node
        for node in ast.walk(block)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "last_hung_check" for t in node.targets
        )
    ]
    assert stamp, "the gated block must stamp last_hung_check"
    probe = _probe_calls(block)[0]
    assert min(s.lineno for s in stamp) > probe.lineno, (
        "last_hung_check must be stamped AFTER the probe returns, so the probe's "
        "own duration is not charged against the next interval"
    )


def test_gate_reads_a_fresh_time_not_the_loops_now():
    # `now` is captured before the memory-pressure check, which itself blocks for
    # up to 5 s per iteration while the server is down (get_server_memory_fraction
    # does not cache the limit on failure). Reusing it charges that block against
    # our interval and inflates the dead-server abort budget.
    test_src = ast.unparse(_hung_check_if(_do_run_tests_ast()).test)
    assert "time()" in test_src
    assert "now" not in {
        node.id
        for node in ast.walk(_hung_check_if(_do_run_tests_ast()).test)
        if isinstance(node, ast.Name)
    }, "the hung-check gate must not reuse the loop's stale `now`"


def test_verdict_string_is_unchanged():
    # CIDB queries and the CI failure signature key on this exact string. There
    # are TWO producers - the polled block and the terminal drain - and BOTH must
    # print it verbatim, so count them rather than merely asserting the literal
    # appears somewhere (which one stale copy would satisfy).
    fn = _do_run_tests_ast()
    verdict_prints = [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "print"
        and len(node.args) == 1
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
        and "Hung check failed" in node.args[0].value
    ]
    assert len(verdict_prints) == 2, [ast.unparse(p) for p in verdict_prints]
    for call in verdict_prints:
        assert call.args[0].value == VERDICT, ast.unparse(call)


def test_loop_body_keeps_the_deadline_check_before_the_hung_check():
    # The deadline block's comment records a deliberate invariant: a real server
    # death keeps precedence over the benign timeout. Reordering these two blocks
    # would silently invert it.
    #
    # The blocks are identified by what they DO, not by the names their
    # conditions mention: both conditions now mention both `args.stop_time` and
    # `args.hung_check`, so a substring match cannot tell them apart and would
    # not notice a swap.
    blocks = _loop_body_ifs(_do_run_tests_ast())
    assert len(blocks) == 4, [ast.unparse(b.test) for b in blocks]

    def sheds_workers(node):
        return "MEMORY_CHECK_INTERVAL" in ast.unparse(node.test)

    def claims_the_deadline(node):
        return any(
            isinstance(child, ast.Assign)
            and any(
                isinstance(t, ast.Name) and t.id == "global_time_limit_reached"
                for t in child.targets
            )
            for child in ast.walk(node)
        )

    def probes(node):
        return bool(_probe_calls(node))

    def handles_the_stop(node):
        return ast.unparse(node.test) == "stop_testing.is_set()"

    roles = [sheds_workers, claims_the_deadline, probes, handles_the_stop]
    for position, (block, is_role) in enumerate(zip(blocks, roles)):
        assert is_role(block), (
            f"loop-body statement {position} is not the expected one: "
            f"{ast.unparse(block.test)[:80]}"
        )


def test_deadline_block_requires_a_liveness_confirmation():
    fn = _do_run_tests_ast()
    deadline = _loop_body_ifs(fn)[1]
    src = ast.unparse(deadline.test)
    assert "last_hung_check_ok" in src and "args.stop_time" in src, (
        "the benign deadline must require a probe that succeeded at or after the "
        "deadline; otherwise a death in the last probe interval is reported as an "
        "expected timeout"
    )


def test_the_deadline_can_be_claimed_only_once():
    # The drain's claim is guarded on `not global_time_limit_reached`, and that
    # guard is currently unreachable BY CONSTRUCTION rather than by luck: the flag
    # is only ever set together with `stop_testing`, which is never cleared, and
    # the drain runs only while `stop_testing` is unset. Pin the two facts that
    # make it so, because a future change breaking either one would make a double
    # claim reachable and silently turn the guard into the only thing preventing
    # a second "Global time limit reached" report.
    fn = _do_run_tests_ast()
    setters = [
        node
        for node in ast.walk(fn)
        if isinstance(node, ast.Assign)
        and any(
            isinstance(t, ast.Name) and t.id == "global_time_limit_reached"
            for t in node.targets
        )
        and ast.unparse(node.value) == "True"
    ]
    assert len(setters) == 1, [ast.unparse(s) for s in setters]

    def stmt_list_containing(node, target):
        for parent in ast.walk(node):
            for _, value in ast.iter_fields(parent):
                if isinstance(value, list) and target in value:
                    return value
        raise AssertionError("statement not found")

    siblings = stmt_list_containing(fn, setters[0])
    following = siblings[siblings.index(setters[0]) + 1 :]
    assert following and ast.unparse(following[0]) == "stop_testing.set()", (
        "global_time_limit_reached must be set together with stop_testing; "
        "otherwise the terminal drain could claim the deadline a second time"
    )
    assert "stop_testing.clear()" not in ast.unparse(fn)


def test_terminal_drain_is_outside_the_loop_and_raises():
    drain = _terminal_drain(_do_run_tests_ast())
    raises = [
        node
        for node in ast.walk(drain)
        if isinstance(node, ast.Raise)
        and isinstance(node.exc, ast.Call)
        and isinstance(node.exc.func, ast.Name)
        and node.exc.func.id == "StopTesting"
    ]
    assert len(raises) == 2, (
        "the drain must raise StopTesting itself (the loop it follows has already "
        "exited, so setting stop_testing alone is a no-op) - once for a confirmed "
        "death and once to claim the benign deadline"
    )
    src = ast.unparse(drain)
    assert "not stop_testing.is_set()" in ast.unparse(drain.test)
    assert "GLOBAL_TIME_LIMIT_EXIT_CODE" in src


# ---------------------------------------------------------------------------
# behavioural harness
# ---------------------------------------------------------------------------


class _Clock:
    def __init__(self):
        self.t = _T0
        self.ticks = 0

    def time(self):
        return self.t

    def sleep(self, seconds):
        # The parent's own poll interval; counting it gives the number of loop
        # iterations, i.e. exactly how many probes an ungated gate would issue.
        if seconds == 0.1:
            self.ticks += 1
        self.t += seconds


class _Suite:
    suite = "0_stateless"

    def __init__(self, n):
        self.parallel_tests = [f"t{i}" for i in range(n)]
        self.sequential_tests = []


class _FakeManager:
    """Stand-in for `multiprocessing.Manager()`.

    A real `Manager` forks a server process and binds an AF_UNIX listener at
    `$TMPDIR/pymp-*/listener-*`, against a 108-byte path cap - so under a long
    `TMPDIR` the whole behavioural half of this file dies with `OSError: AF_UNIX
    path too long` before `do_run_tests` even runs. The IPC buys nothing here:
    `do_run_tests` only calls `.list()` and `.extend()` on the result
    (tests/clickhouse-test:5103-5105) before handing it to the workers, and the
    fake workers here never read the queue.
    """

    def __call__(self):
        return self

    def list(self, iterable=()):
        return list(iterable)


def run_loop(
    *,
    alive,
    attempt_cost=0.0,
    workers_done=None,
    deadline=None,
    wall_limit=None,
    model_memory=True,
    pressure=None,
    stop_at=None,
):
    """Run the real `do_run_tests` against a fake clock and a stubbed probe.

    `alive(t)` decides whether the server answers at fake-run-time `t`.
    `workers_done` is when the fake workers exit (None: they never do).
    `pressure` is the memory fraction the stub reports (None: the healthy 0.1 /
    unreachable-server model); `stop_at` sets `stop_testing` from outside once
    that much fake run-time has passed, modelling a worker or signal handler
    requesting a stop.
    Returns a dict describing what the run did.
    """
    ct = _load()
    fn = ct["do_run_tests"]
    g = fn.__globals__
    clock = _Clock()
    state = {
        "attempts": [],
        "max_retries": set(),
        "verdict": None,
        "blocked": 0.0,
        "memory_calls": [],
    }

    def elapsed():
        return clock.t - _T0

    class _Process:
        exitcode = 0

        def __init__(self, *_args, **_kwargs):
            self.name = "fake-worker"
            self.pid = 1
            self._alive = True

        def start(self):
            pass

        def is_alive(self):
            if workers_done is not None and elapsed() >= workers_done:
                return False
            if wall_limit is not None and elapsed() >= wall_limit:
                return False
            return self._alive

        def terminate(self):
            self._alive = False

        def kill(self):
            self._alive = False

        def join(self, timeout=None):
            self._alive = False

    def liveness(_args, max_retries=10):
        state["max_retries"].add(max_retries)
        entered = clock.t
        for attempt in range(max_retries):
            ok = alive(elapsed())
            state["attempts"].append((round(elapsed(), 3), ok))
            clock.sleep(0.05 if ok else attempt_cost)
            if ok:
                state["blocked"] += clock.t - entered
                return True
            if attempt < max_retries - 1:
                clock.sleep(min(2**attempt, 10))
        state["blocked"] += clock.t - entered
        return False

    def memory_fraction(_args):
        state["memory_calls"].append(round(elapsed(), 3))
        if pressure is not None:
            # A saturated server can report pressure while failing the liveness
            # probe: the fraction comes from /proc/<pid>/statm and `_max_memory`
            # is cached once fetched, so neither read needs the server to answer
            # `SELECT 1` (tests/clickhouse-test:4776-4818).
            clock.sleep(0.05)
            return pressure
        if alive(elapsed()):
            clock.sleep(0.05)
            return 0.1
        if not model_memory:
            return None
        # `_max_memory` is NOT cached on failure, so every check re-issues the
        # limit query and blocks for its full timeout while the server is down.
        clock.sleep(5.0)
        return None

    mp = g["multiprocessing"]
    saved = {
        name: g[name]
        for name in (
            "time",
            "sleep",
            "multiprocessing",
            "check_server_liveness",
            "print_c_stacktraces",
            "get_server_memory_fraction",
        )
    }
    stop_testing = mp.Event()

    def sleep(seconds):
        clock.sleep(seconds)
        # Model an external stop request (a worker's KeyboardInterrupt handler, a
        # signal handler, or a `--max-failures` abort) arriving while the parent
        # is inside a failure sequence.
        if stop_at is not None and elapsed() >= stop_at:
            stop_testing.set()

    g["time"] = clock.time
    g["sleep"] = sleep
    # `Value` and `Event` are sharedctypes/synchronize objects needing no server
    # process, and the code under test reads them through `.value`/`get_lock()`;
    # only `Manager` is replaced (see `_FakeManager`).
    g["multiprocessing"] = types.SimpleNamespace(
        Process=_Process, Value=mp.Value, Manager=_FakeManager(), Event=mp.Event
    )
    g["check_server_liveness"] = liveness
    g["print_c_stacktraces"] = lambda _args: state.__setitem__(
        "verdict", round(elapsed(), 2)
    )
    g["get_server_memory_fraction"] = memory_fraction

    args = argparse.Namespace(
        hung_check=True,
        stop_time=(_T0 + deadline) if deadline is not None else None,
        no_self_parallel=False,
    )
    outcome = "returned"
    message = ""
    # `do_run_tests`' graceful-deadline paths install SIG_IGN process-wide
    # (tests/clickhouse-test:5220, :5349) and never restore it - it is not a
    # module global, so `saved` above cannot cover it. Leaking it would break
    # every later test in this pytest process that SIGTERMs a child.
    saved_sigterm = signal.getsignal(signal.SIGTERM)
    try:
        with contextlib.redirect_stdout(io.StringIO()) as out:
            fn(
                2,
                _Suite(4),
                args,
                mp.Value("i", 0),
                [],
                stop_testing,
                mp.Event(),
            )
    except ct["StopTesting"] as exc:
        code = getattr(exc, "exit_code", None)
        outcome = {
            ct["STOP_TESTING_EXIT_CODE"]: "server-died",
            ct["GLOBAL_TIME_LIMIT_EXIT_CODE"]: "graceful",
        }.get(code, f"other({code})")
        message = str(exc)
    finally:
        g.update(saved)
        signal.signal(signal.SIGTERM, saved_sigterm)

    return {
        "outcome": outcome,
        "message": message,
        "verdict": state["verdict"],
        "attempts": state["attempts"],
        "n_attempts": len(state["attempts"]),
        "max_retries": state["max_retries"],
        "wall": round(elapsed(), 2),
        "ticks": clock.ticks,
        "blocked": state["blocked"],
        "memory_calls": state["memory_calls"],
        "stdout": out.getvalue(),
    }


DEAD = lambda _t: False  # noqa: E731 - server never answers
HEALTHY = lambda _t: True  # noqa: E731


def down_from(start):
    return lambda t: t < start


def down_between(start, end):
    return lambda t: not (start <= t < end)


def test_run_loop_does_not_leak_the_sigterm_disposition():
    # do_run_tests' graceful-deadline paths install SIG_IGN
    # (tests/clickhouse-test:5220, :5349). SIG_IGN survives exec - CPython's
    # _Py_RestoreSignals resets only SIGPIPE/SIGXFZ/SIGXFSZ - so leaking it would
    # stop every later test in this pytest process from being able to SIGTERM a
    # child (ci/praktika/utils.py:291 kills via killpg(SIGTERM), and
    # ci/tests/test_teepopen_timeout_kills_process.py asserts rc == -SIGTERM).
    before = signal.getsignal(signal.SIGTERM)
    result = run_loop(alive=HEALTHY, deadline=10.0, wall_limit=900.0)
    assert result["outcome"] == "graceful"  # the SIG_IGN path really ran
    assert signal.getsignal(signal.SIGTERM) is before


# ---------------------------------------------------------------------------
# behavioural: the two abort budgets must be preserved
# ---------------------------------------------------------------------------


def test_instant_refusal_still_aborts_at_about_65s():
    # 10 instant failures spaced by master's [1,2,4,8,10,10,10,10,10] ladder.
    # This is the band that matters most: a third of the observed CI hits have no
    # server process left at all.
    cached = run_loop(alive=DEAD, model_memory=False)
    assert cached["outcome"] == "server-died"
    assert 65.0 <= cached["verdict"] <= 70.0, cached["verdict"]
    # With the memory limit UNCACHED every memory check also blocks 5 s. Gating
    # on the loop's stale `now` instead of a fresh time() inflates this to ~107 s.
    uncached = run_loop(alive=DEAD, model_memory=True)
    assert uncached["outcome"] == "server-died"
    assert 65.0 <= uncached["verdict"] <= 75.0, uncached["verdict"]


def test_socket_timeout_still_aborts_at_about_165s():
    cached = run_loop(alive=DEAD, attempt_cost=10.0, model_memory=False)
    assert cached["outcome"] == "server-died"
    assert 165.0 <= cached["verdict"] <= 172.0, cached["verdict"]
    uncached = run_loop(alive=DEAD, attempt_cost=10.0, model_memory=True)
    assert uncached["outcome"] == "server-died"
    assert 165.0 <= uncached["verdict"] <= 180.0, uncached["verdict"]


def test_exactly_one_attempt_per_poll():
    result = run_loop(alive=DEAD, attempt_cost=10.0)
    assert result["max_retries"] == {1}, result["max_retries"]


def test_a_success_resets_the_failure_counter():
    # A recovering server must not accumulate toward an abort: the CI logs show
    # recovery 9-171 s after the probe window. Two separate sub-budget stalls with
    # a healthy stretch between them must not add up to a verdict - which is what
    # actually catches a counter that is never reset, since a SINGLE sub-budget
    # stall cannot reach the failure budget either way.
    two_stalls = lambda t: not (  # noqa: E731
        0.0 <= t < 60.0 or 400.0 <= t < 460.0
    )
    result = run_loop(alive=two_stalls, wall_limit=900.0)
    assert result["outcome"] == "returned", result["outcome"]
    assert result["verdict"] is None
    assert VERDICT not in result["stdout"]
    # Both stalls really were probed, i.e. the scenario exercised the reset.
    failures = [t for t, ok in result["attempts"] if not ok]
    assert any(t < 60.0 for t in failures), failures
    assert any(400.0 <= t < 460.0 for t in failures), failures


def test_healthy_run_probes_on_the_interval_not_on_every_tick():
    three_hours = 3 * 60 * 60
    result = run_loop(alive=HEALTHY, wall_limit=three_hours)
    assert result["outcome"] == "returned"
    # ~one probe per 30 s interval.
    assert 300 <= result["n_attempts"] <= 450, result["n_attempts"]
    # `ticks` is the number of loop iterations, i.e. what the ungated probe
    # issued: the whole point of the change is that these are no longer equal.
    assert result["n_attempts"] * 100 < result["ticks"], (
        result["n_attempts"],
        result["ticks"],
    )


def test_first_poll_fires_immediately():
    result = run_loop(alive=HEALTHY, wall_limit=120.0)
    assert result["attempts"], "no probe was issued at all"
    assert result["attempts"][0][0] < 1.0, result["attempts"][0]


def test_a_stall_shorter_than_the_failure_budget_does_not_abort():
    for start, duration, cost in ((0.0, 40.0, 0.0), (5.0, 120.0, 10.0)):
        result = run_loop(
            alive=down_between(start, start + duration),
            attempt_cost=cost,
            wall_limit=900.0,
        )
        assert result["outcome"] == "returned", (start, duration, cost, result)
        assert result["verdict"] is None


# ---------------------------------------------------------------------------
# behavioural: the deadline must not steal the exit code
# ---------------------------------------------------------------------------


def test_death_near_the_deadline_is_reported_as_a_death():
    # Splitting the retry window lets the deadline block run BETWEEN two failing
    # probes; without the liveness confirmation it claims the benign
    # GLOBAL_TIME_LIMIT_EXIT_CODE and a real death is reported as an expected
    # timeout.
    for cost in (0.0, 10.0):
        for die_at in (150.0, 200.0, 250.0, 280.0, 295.0):
            result = run_loop(
                alive=down_from(die_at),
                attempt_cost=cost,
                deadline=300.0,
                wall_limit=900.0,
            )
            assert result["outcome"] == "server-died", (cost, die_at, result["outcome"])


def test_healthy_run_still_reports_the_benign_deadline():
    # The mirror image: the deadline condition must not be so strict that the
    # graceful stop never fires - it is the EXPECTED stop for the flaky and
    # targeted checks.
    #
    # The stop must also arrive PROMPTLY. Asserting only the outcome is not enough:
    # a condition that waits for the next scheduled probe instead of taking the
    # extra deadline-triggered one still ends up graceful, just up to a whole
    # interval late - and if the confirmation is never recorded, the run only stops
    # when the workers do (here: at wall_limit), which the outcome also hides.
    for deadline in (10.0, 45.0, 90.0, 200.0):
        result = run_loop(alive=HEALTHY, deadline=deadline, wall_limit=900.0)
        assert result["outcome"] == "graceful", (deadline, result["outcome"])
        # 5 s is the loop's own post-stop worker drain; allow one extra second.
        assert result["wall"] <= deadline + 6.0, (
            deadline,
            result["wall"],
            "the graceful stop must be claimed at the deadline, not one probe "
            "interval later or only when the workers finish",
        )


def test_a_confirmation_exactly_at_the_deadline_counts():
    # The confirmation is compared with `>=`: a probe that succeeded EXACTLY at
    # args.stop_time does confirm it. With `>` the deadline waits for the next
    # scheduled probe, delaying the graceful stop by a whole interval.
    #
    # Reaching that boundary needs the deadline to fall exactly on the timestamp a
    # successful probe stamped, so it is derived from an unconstrained run rather
    # than hard-coded (the stamp depends on the loop's tick and the probe's cost).
    calibration = run_loop(alive=HEALTHY, wall_limit=120.0)
    first_probe_at, first_ok = calibration["attempts"][0]
    assert first_ok, calibration["attempts"][0]
    stamped_at = round(first_probe_at + 0.05, 10)

    result = run_loop(alive=HEALTHY, deadline=stamped_at, wall_limit=400.0)
    assert result["outcome"] == "graceful", result["outcome"]
    assert result["wall"] <= stamped_at + 6.0, (
        stamped_at,
        result["wall"],
        "a probe that succeeded exactly at the deadline must confirm it",
    )


def test_no_probe_storm_once_the_deadline_has_passed():
    # The extra deadline-triggered poll is guarded on "no failure sequence
    # pending", so it cannot re-probe on every iteration while the server is
    # down. Probing inline in the deadline condition instead would.
    with_deadline = run_loop(
        alive=down_from(250.0), attempt_cost=10.0, deadline=300.0, wall_limit=900.0
    )
    baseline = run_loop(alive=down_from(250.0), attempt_cost=10.0, wall_limit=900.0)
    assert with_deadline["outcome"] == "server-died"
    assert with_deadline["n_attempts"] == baseline["n_attempts"], (
        with_deadline["n_attempts"],
        baseline["n_attempts"],
    )
    # The attempt COUNT alone cannot see an extra poll that merely displaces a
    # scheduled one: dropping the "no failure sequence pending" guard re-probes
    # past the deadline and shortens the whole ladder instead of lengthening it.
    assert with_deadline["verdict"] == baseline["verdict"], (
        with_deadline["verdict"],
        baseline["verdict"],
        "a deadline must not change WHEN the death is reported",
    )


# ---------------------------------------------------------------------------
# behavioural: the terminal drain
# ---------------------------------------------------------------------------


def test_death_after_the_last_probe_is_still_reported():
    # The loop exits as soon as the workers finish, so a failure sequence that
    # has not yet reached its budget would otherwise be abandoned and the run
    # would record a generic failure instead of "Server died".
    for cost in (0.0, 10.0):
        for die_at in (50.0, 100.0, 150.0, 190.0, 199.0):
            result = run_loop(
                alive=down_from(die_at), attempt_cost=cost, workers_done=200.0
            )
            assert result["outcome"] == "server-died", (cost, die_at, result["outcome"])
            assert VERDICT in result["stdout"]
            # The drain must spend master's own 65 s back-off ladder before
            # condemning the server, not race through its remaining attempts:
            # a drain keeping a private counter reports ~20 s too early.
            assert result["verdict"] >= die_at + 60.0, (
                cost,
                die_at,
                result["verdict"],
                "the drain reported a death before master's back-off ladder elapsed",
            )


def test_healthy_run_returns_normally_after_the_drain():
    for workers_done in (35.0, 90.0, 200.0):
        result = run_loop(alive=HEALTHY, workers_done=workers_done)
        assert result["outcome"] == "returned", (workers_done, result["outcome"])
        # The drain's terminal confirmation costs at most one extra probe.
        assert result["n_attempts"] <= int(workers_done / 30.0) + 2, (
            workers_done,
            result["n_attempts"],
        )


def test_drain_honours_the_back_off_the_loop_started():
    # The workers finish while a failure sequence is pending and the remaining
    # back-off has not elapsed. Probing immediately would collapse two attempts
    # into one and abort a server that answers on the next retry.
    for workers_done in (59.0, 61.0):
        result = run_loop(
            alive=down_between(0.0, 65.0), workers_done=workers_done, wall_limit=900.0
        )
        assert result["outcome"] == "returned", (workers_done, result["outcome"])
        # No two consecutive failed attempts closer than master's own ladder.
        failures = [t for t, ok in result["attempts"] if not ok]
        for k, (previous, current) in enumerate(zip(failures, failures[1:]), start=1):
            assert current - previous >= min(2 ** (k - 1), 10) - 0.25, (
                workers_done,
                k,
                previous,
                current,
            )


def test_drain_adds_no_wall_clock_to_a_healthy_run():
    # Carrying the back-off unconditionally makes the drain sleep out the rest of
    # the 30 s grid on EVERY run - up to +29.5 s of dead wall-clock on every CI
    # functional job. Master returns right after removing the last worker.
    for workers_done in (5.0, 20.0, 31.0, 35.0, 45.0, 59.0, 61.0, 90.0, 200.0):
        result = run_loop(alive=HEALTHY, workers_done=workers_done)
        assert result["outcome"] == "returned"
        assert result["wall"] - workers_done <= 1.0, (workers_done, result["wall"])


# ---------------------------------------------------------------------------
# behavioural: the parent keeps working while a failure sequence is pending
# ---------------------------------------------------------------------------


def test_memory_shed_still_runs_while_a_failure_sequence_is_pending():
    # The whole point of owning the retry window in the loop: a saturated server
    # can fail `SELECT 1` while its RSS is still readable, and shedding a worker
    # is the one mechanism that could relieve the pressure. Master spends the
    # entire window inside one blocking probe call, so it performs ZERO memory
    # checks and sheds nothing (measured on pristine master: 0 checks, 0 shed
    # lines).
    result = run_loop(alive=DEAD, attempt_cost=10.0, pressure=0.95)
    assert result["outcome"] == "server-died", result["outcome"]

    failures = [t for t, ok in result["attempts"] if not ok]
    assert len(failures) >= 2, failures
    inside = [t for t in result["memory_calls"] if failures[0] <= t <= failures[-1]]
    assert len(inside) >= 2, (result["memory_calls"], failures)

    shed_at = result["stdout"].find("signalling one worker to stop")
    verdict_at = result["stdout"].find(VERDICT)
    assert shed_at >= 0, result["stdout"]
    assert verdict_at >= 0, result["stdout"]
    assert shed_at < verdict_at, (
        shed_at,
        verdict_at,
        "the worker shed must run DURING the failure sequence, not after the "
        "verdict - after it there is nothing left to relieve",
    )
    # The shed must be REACHED from inside the sequence, not merely printed
    # before the verdict: pin the memory check that produced it as one of the
    # in-window ones, so gating the shed on `hung_check_failures == 0` (which
    # only ever lets the very first, pre-failure check shed) is caught.
    assert any(t > failures[0] for t in inside), (inside, failures)

    # The shed REQUEST, not just the message. `workers_to_shed` is created
    # inside `do_run_tests`, so the counter is read back from the line's own
    # "(N tracked, M pending shed)" tail - and it is the counter, not the print,
    # that the workers act on. Without this the message can be emitted while the
    # request is never actually recorded, in which case the same check sheds over
    # and over (measured: 4 identical lines, all "0 pending shed") because the
    # cap `len(processes) - workers_to_shed.value > 1` never tightens.
    pending = re.findall(r"\((\d+) tracked, (\d+) pending shed\)", result["stdout"])
    assert pending, result["stdout"]
    assert [int(m) for _, m in pending] == [1], (
        pending,
        "the shed must record exactly one pending request; a message printed "
        "without incrementing workers_to_shed sheds nothing and repeats",
    )


def test_an_external_stop_is_honoured_between_failed_probes():
    # A worker hitting --max-failures, a signal handler, or any other stop
    # request must be seen while the probe ladder is still running. Master blocks
    # in one call for the whole window, so it only notices at the verdict
    # (measured on pristine master: wall 170.1 s, and it condemns the server on
    # the way out).
    result = run_loop(alive=DEAD, attempt_cost=10.0, stop_at=20.0)
    assert result["message"] == "test run was stopped (see earlier output for the cause)"
    assert result["wall"] < 60.0, (
        result["wall"],
        "the external stop must be honoured before the 65 s failure budget elapses",
    )
    assert VERDICT not in result["stdout"], (
        "the run stopped for an external reason, so the hung check must not have "
        "condemned the server"
    )


def test_the_parent_is_not_starved_during_a_failure_sequence():
    # `blocked` is the time spent inside the probe. Master: 0.93 (instant
    # refusal) to 0.97 (socket timeout) - one call spans the whole window, so
    # nothing else in the loop runs at all. The fix: 0.00 to 0.59.
    #
    # The fix cannot reach 0 in the socket-timeout band because each of the ten
    # attempts genuinely waits out the callee's own 10 s socket timeout; what
    # changed is that the parent regains control between them (max contiguous
    # block 165 s -> 10 s). 0.70 therefore sits between the two designs with
    # margin on both sides: 0.11 above the fix's worst measured case, 0.23 below
    # master's best. A regression handing the retry window back to the callee
    # would land at >= 0.93 and fail this by a wide margin.
    for label, kwargs in (
        ("instant-cached", dict(alive=DEAD, model_memory=False)),
        ("instant-uncached", dict(alive=DEAD, model_memory=True)),
        ("timeout-cached", dict(alive=DEAD, attempt_cost=10.0, model_memory=False)),
        ("timeout-uncached", dict(alive=DEAD, attempt_cost=10.0, model_memory=True)),
        ("under-pressure", dict(alive=DEAD, attempt_cost=10.0, pressure=0.95)),
    ):
        result = run_loop(**kwargs)
        assert result["outcome"] == "server-died", (label, result["outcome"])
        duty = result["blocked"] / result["wall"]
        assert duty < 0.70, (label, duty, result["blocked"], result["wall"])


def test_workers_stopping_at_the_deadline_still_report_the_benign_code():
    # Workers stop themselves once args.stop_time passes. If the last one goes in
    # the same iteration, no further loop iteration runs to claim the deadline,
    # so the drain has to claim it - otherwise an expected timeout is recorded as
    # a generic failure.
    for deadline in (10.0, 20.0, 31.0, 45.0, 60.0, 61.0, 90.0, 120.0, 200.0):
        result = run_loop(alive=HEALTHY, workers_done=deadline, deadline=deadline)
        assert result["outcome"] == "graceful", (deadline, result["outcome"])


if __name__ == "__main__":
    for name, fn in sorted(dict(globals()).items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"ok {name}")
    print("All hung-check schedule tests passed.")
