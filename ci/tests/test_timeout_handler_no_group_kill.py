"""
A single test's timeout must not abort the whole run.

`timeout_handler` in `tests/clickhouse-test` fires from a per-test SIGALRM. It
used to call `stop_tests`, whose `killpg` broadcasts SIGTERM to our own
process group - which every parallel worker and the parent share. One test
exceeding its own deadline therefore terminated the entire run, and the job
side relabelled the result as "Server died" (exit -15 is in
`ABORTED_RUN_EXIT_CODES`), hiding a run where every executed test had passed.

The handler must instead kill only the timing-out test's own out-of-group
clients. Both counters are asserted so the test cannot pass vacuously:
broadcasts to our own group must be 0, while the out-of-group child must still
be killed. A third check pins the whole-run callers, which still need the
broadcast.

Surviving the alarm is only half the contract: the run must then REPORT the
timed-out test exactly once. Three tests drive the real `run_tests_array` over
the windows where `run` cannot convert the alarm itself, using the production
`process_result`: before any result exists, after one exists but before its
status marker is stamped, and after the marker was stamped but before that is
recorded. The last one matters because a doubled description also matches the
job-side leaf pattern, so the run would report a test that never ran.

No server and no wall-clock: the real handler body is exec'd against the real
module globals with `killpg` / `pgrep` / `getpgid` faked.
"""

import ast
import inspect
import io
import multiprocessing
import os
import re
import runpy
import signal
import socket
import textwrap
import types
from contextlib import redirect_stdout
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent.parent
_CLICKHOUSE_TEST = str(_REPO_ROOT / "tests" / "clickhouse-test")


def _results_pattern():
    """The job-side leaf regex, read from its own source.

    Importing the module would pull in `praktika`, which is not importable
    here, and a copied regex would rot independently of the parser.
    """
    src = (
        _REPO_ROOT / "ci" / "jobs" / "scripts" / "functional_tests_results.py"
    ).read_text(encoding="utf-8")
    body = re.search(
        r"TEST_RESULT_PATTERN = re\.compile\((.*?)\n\)", src, re.S
    ).group(1)
    return re.compile(ast.literal_eval("(" + body.strip() + ")"))


_TEST_RESULT_PATTERN = _results_pattern()

_ct = runpy.run_path(_CLICKHOUSE_TEST)
# `runpy.run_path` returns a COPY of the executed namespace, while the functions
# defined in it close over the original dict - so patching has to go through a
# function's own `__globals__`, which is that original.
_CT_GLOBALS = _ct["cleanup_child_processes"].__globals__

_OUR_PGID = 4242
_IN_GROUP_CHILD = 5001  # a sibling worker: same group, must be spared
_OUT_OF_GROUP_CHILD = 5002  # this test's own client (start_new_session=True)
_OUT_OF_GROUP_PGID = 5002


def _timeout_handler_source():
    """The real nested `timeout_handler` body, lifted out of `run_tests_array`."""
    lines = inspect.getsource(_ct["run_tests_array"]).splitlines()
    start = next(
        i for i, l in enumerate(lines) if l.strip().startswith("def timeout_handler")
    )
    indent = len(lines[start]) - len(lines[start].lstrip())
    end = start + 1
    while end < len(lines):
        line = lines[end]
        if line.strip() and (len(line) - len(line.lstrip())) <= indent:
            break
        end += 1
    return textwrap.dedent("\n".join(lines[start:end]))


class _Recorder:
    def __init__(self):
        self.killpg = []  # (pgid, signal) reaching os.killpg
        self.killed_groups = []  # pgid arguments to kill_process_group

    @property
    def own_group_broadcasts(self):
        return [c for c in self.killpg if c[0] == _OUR_PGID]


@pytest.fixture(name="rec")
def _rec(monkeypatch):
    rec = _Recorder()
    pgids = {
        os.getpid(): _OUR_PGID,
        _IN_GROUP_CHILD: _OUR_PGID,
        _OUT_OF_GROUP_CHILD: _OUT_OF_GROUP_PGID,
    }
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: rec.killpg.append((pgid, sig)))
    monkeypatch.setattr(os, "getpgid", lambda pid: pgids[pid])
    monkeypatch.setitem(
        _CT_GLOBALS,
        "kill_process_group",
        lambda pgid, fatal_log: rec.killed_groups.append(pgid),
    )
    monkeypatch.setitem(
        _CT_GLOBALS,
        "pgrep",
        lambda ppid=None, pgid=None, command=None: [
            [_IN_GROUP_CHILD, os.getpid(), _OUR_PGID, "clickhouse-client sibling"],
            [
                _OUT_OF_GROUP_CHILD,
                os.getpid(),
                _OUT_OF_GROUP_PGID,
                "clickhouse-client own",
            ],
        ],
    )
    monkeypatch.setitem(_CT_GLOBALS, "print_sql_stacktraces", lambda args: None)
    monkeypatch.setitem(_CT_GLOBALS, "print_c_stacktraces", lambda args: None)
    return rec


def _run_timeout_handler(monkeypatch):
    # The handler closes over `args` and `cleanup_output` in `run_tests_array`;
    # exec'd standalone it reads them from the globals it is given, which must be
    # the real module globals so the patched helpers above are the ones it calls.
    monkeypatch.setitem(_CT_GLOBALS, "args", None)  # only the stubbed dumps see it
    monkeypatch.setitem(_CT_GLOBALS, "cleanup_output", io.StringIO())
    exec(_timeout_handler_source(), _CT_GLOBALS)  # pylint: disable=exec-used
    handler = _CT_GLOBALS.pop("timeout_handler")
    handler(signal.SIGALRM, None)


def test_per_test_timeout_does_not_signal_our_own_process_group(rec, monkeypatch):
    """The defect: one test's deadline SIGTERMed all 24 workers and the parent."""
    with pytest.raises(_ct["TestTimeout"]):
        _run_timeout_handler(monkeypatch)

    assert rec.own_group_broadcasts == [], (
        f"timeout_handler signalled our own process group {_OUR_PGID}: "
        f"{rec.own_group_broadcasts}. That kills every parallel worker and the "
        "parent, so the run exits -15 and is reported as 'Server died'."
    )


def test_per_test_timeout_still_kills_the_tests_own_clients(rec, monkeypatch):
    """Counter (b): the fix must not disarm the per-test teardown it replaces."""
    with pytest.raises(_ct["TestTimeout"]):
        _run_timeout_handler(monkeypatch)

    assert rec.killed_groups == [_OUT_OF_GROUP_PGID], (
        "timeout_handler must kill the timing-out test's own out-of-group "
        f"clients, got {rec.killed_groups}"
    )


def _parsed_leaf(report):
    """The report's per-test leaf, as the job-side parser would match it."""
    for line in report.splitlines():
        match = _TEST_RESULT_PATTERN.match(line)
        if match:
            return match
    return None


def _drive_run_tests_array(
    monkeypatch,
    run_impl,
    process_result=None,
    testname=False,
    case_mixin=None,
    queue=("00001_probe.sql",),
):
    """Run the real `run_tests_array` over one test whose `run` is `run_impl`.

    Returns (stdout, exit_code.value). Everything else is production code: the
    result is formatted by the real `TestCase.process_result`.

    `testname` selects the pre-test health-check path that `Fast test` and the
    functional suites use. `case_mixin` supplies the extra attributes and methods
    that path needs, so the default arms keep the minimal fake they had.
    """
    real_test_case = _ct["TestCase"]

    class _Case(real_test_case):
        effective_settings: dict = {}
        effective_merge_tree_settings: dict = {}

        def __init__(self, suite, case, args, is_concurrent):
            # pylint: disable=super-init-not-called
            self.name = case
            self.case = case
            self.args = args
            self.suite = suite
            self.testcase_args = None
            self.runs_count = 0
            # Production captures these at construction (the pre-test
            # environment) and restores them in `run`'s `finally`.
            self.base_url_params = ""
            self.base_client_options = ""

        run = run_impl

    if process_result is not None:
        _Case.process_result = process_result

    if case_mixin is not None:
        _Case = type("_Case", (case_mixin, _Case), {})

    # Bound outside the class body: a class body does not see the enclosing
    # function scope, so `testname = testname` there raises NameError.
    want_testname = testname

    class _Args:
        timeout = 60
        testname = want_testname
        max_failures = 0
        max_failures_chain = 10**9
        stop_time = None
        hung_check = False
        jobs = 1
        database = None

        def __getattr__(self, _name):
            return None

    monkeypatch.setitem(_CT_GLOBALS, "TestCase", _Case)
    monkeypatch.setitem(
        _CT_GLOBALS, "save_random_settings_artifact", lambda *a, **k: None
    )
    monkeypatch.setitem(_CT_GLOBALS, "get_next_test_progress", lambda c, t: "")
    monkeypatch.setitem(_CT_GLOBALS, "trim_for_log", lambda s, n: s)
    monkeypatch.setitem(_CT_GLOBALS, "colored", lambda s, *a, **k: s)

    exit_code = multiprocessing.Value("i", 0)
    suite = types.SimpleNamespace(
        suite="probe",
        sequential_tests=[],
        parallel_tests=[],
        blacklist_check=set(),
        suite_tmp_path="/tmp",
    )
    params = (
        list(queue),
        1,
        suite,
        True,
        _Args(),
        exit_code,
        multiprocessing.Event(),
        multiprocessing.Value("i", 0),
        [],
        0,
        multiprocessing.Value("i", 0),
        multiprocessing.Value("i", 0),
        len(queue),
        multiprocessing.Value("i", 0),
    )

    out = io.StringIO()
    with redirect_stdout(out):
        _ct["run_tests_array"](params)
    return out.getvalue(), exit_code.value


def test_timed_out_test_is_reported_when_run_cannot_catch_the_alarm(
    rec, monkeypatch
):
    """The alarm fires inside `TestCase.run`'s own `except` arm, so its
    `socket.timeout` arm is already spent and cannot build a result. The
    reporting path still needs one: without it `test_result` stays None and the
    worker dies on `test_result.description` instead of reporting a TIMEOUT."""

    def run(self, args, suite, client_options):
        try:
            raise ConnectionError("transient")
        except socket.timeout:  # already spent: cannot catch our alarm
            raise AssertionError("unreachable") from None
        except ConnectionError:
            # Stands in for `check_server_liveness`, whose worst case (165 s)
            # outlasts the per-test alarm (int(60*1.1)+60 = 126 s).
            signal.setitimer(signal.ITIMER_REAL, 0.05)
            while True:
                pass
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)

    report, exit_code = _drive_run_tests_array(monkeypatch, run)
    leaf = _parsed_leaf(report)

    assert leaf, (
        "a test whose alarm fired where `run` could not catch it must still "
        f"produce a leaf the job side can parse, got:\n{report}"
    )
    assert leaf.group(2) == "[ FAIL ]", f"expected FAIL, got {leaf.group(2)}"
    assert "Timeout" in report, f"the reason must be TIMEOUT:\n{report}"
    # The duration must be measured, like every other path in `run`, not the
    # configured `--timeout`. Bounding it below that value is enough to tell the
    # two apart here and keeps the assertion off the wall clock.
    assert float(leaf.group(3)) < 60, (
        "the leaf must report the measured duration, not the configured "
        f"--timeout, got {leaf.group(3)} sec"
    )
    assert exit_code == 1, (
        f"the timed-out test must count as a failure, exit_code={exit_code}"
    )


def test_alarm_between_run_and_formatting_still_yields_a_parseable_leaf(
    rec, monkeypatch
):
    """The alarm can also land after `run` produced a result but before
    `process_result` stamped the status marker. The real verdict must be kept
    AND formatted: an unstamped description matches no leaf pattern, so the job
    side would attribute the failure to `clickhouse-test` instead of the test.

    `process_result` raises the alarm on its first call, so the timeout is
    guaranteed to hit that exact window; a wall-clock timer would expire
    somewhere else and prove nothing."""
    calls = []

    def run(self, args, suite, client_options):
        return _ct["TestResult"](
            self.name,
            _ct["TestStatus"].FAIL,
            _ct["FailureReason"].RESULT_DIFF,
            1.0,
            "\nreal verdict preserved\n",
        )

    def process_result(self, result, messages):
        calls.append(result.reason)
        if len(calls) == 1:
            raise _ct["TestTimeout"]("Test execution timed out")
        return _ct["TestCase"].process_result(self, result, messages)

    report, exit_code = _drive_run_tests_array(
        monkeypatch, run, process_result=process_result
    )
    leaf = _parsed_leaf(report)

    assert len(calls) == 2, (
        f"the interrupted formatting must be completed once, calls={calls}"
    )
    assert calls[1] is _ct["FailureReason"].RESULT_DIFF, (
        f"the real verdict was replaced by the placeholder: {calls}"
    )
    assert leaf, f"no parseable leaf for the real verdict:\n{report}"
    assert leaf.group(2) == "[ FAIL ]", f"expected FAIL, got {leaf.group(2)}"
    assert "real verdict preserved" in report, (
        f"the real description was lost:\n{report}"
    )
    assert exit_code == 1, f"the real FAIL must still count, exit_code={exit_code}"


def test_alarm_after_formatting_does_not_stamp_the_report_twice(rec, monkeypatch):
    """The narrower sibling window: the alarm lands after `process_result`
    already mutated `description`, but before `formatted` records it. A flag that
    only tracks "the call returned" then disagrees with the object, and the
    recovery arm formats it a second time.

    A doubled line is not merely cosmetic: it matches the job-side leaf pattern
    too, so the run reports an extra failing test that never ran."""
    calls = []

    def run(self, args, suite, client_options):
        return _ct["TestResult"](
            self.name,
            _ct["TestStatus"].FAIL,
            _ct["FailureReason"].RESULT_DIFF,
            1.0,
            "\nreal verdict preserved\n",
        )

    def process_result(self, result, messages):
        calls.append(result.reason)
        result = _ct["TestCase"].process_result(self, result, messages)
        if len(calls) == 1:
            # Deliver the alarm strictly after the formatter mutated `result`,
            # which is the whole point of this arm. `raise_signal` runs the real
            # handler, so a blocked SIGALRM is deferred exactly as in production.
            signal.raise_signal(signal.SIGALRM)
        return result

    report, exit_code = _drive_run_tests_array(
        monkeypatch, run, process_result=process_result
    )

    assert len(calls) == 1, (
        f"an already-formatted result must not be formatted again, calls={calls}"
    )
    # Deferring must not become dropping. If the mask were left blocked, this
    # worker would silently stop honouring every later per-test deadline, so
    # assert the alarm was really delivered (the handler ran) and the mask is
    # clean afterwards.
    assert rec.killed_groups == [_OUT_OF_GROUP_PGID], (
        "the deferred alarm must still be delivered once the mask is restored, "
        f"but the handler never ran: {rec.killed_groups}"
    )
    assert signal.SIGALRM not in signal.pthread_sigmask(signal.SIG_BLOCK, []), (
        "SIGALRM was left blocked, which disarms every later per-test timeout"
    )
    leaves = [
        line for line in report.splitlines() if _TEST_RESULT_PATTERN.match(line)
    ]
    assert len(leaves) == 1, (
        "a double-stamped description yields a second line matching the job-side "
        f"leaf pattern, so the run reports a test that never ran:\n{report}"
    )
    assert leaves[0].lstrip().startswith("00001_probe:"), (
        f"the single leaf must be the real test, got:\n{leaves[0]}"
    )
    assert "real verdict preserved" in report, (
        f"the real description was lost:\n{report}"
    )
    assert exit_code == 1, f"the real FAIL must still count, exit_code={exit_code}"


def test_deadline_in_the_testname_prelude_is_not_absorbed(rec, monkeypatch):
    """`--testname` is the default CI path (`Fast test`, functional suites), and
    its pre-test health check runs under the same one-shot alarm as the test.

    `run` wraps that check in `except Exception`, and `check_server_liveness`
    retries under another one. A deadline raised as an `Exception` subclass is
    absorbed there, and because `signal.alarm` is armed once per attempt and
    never re-armed inside `run`, the attempt then continues with no deadline at
    all: the timeout is silently lost rather than reported.

    Both counters are asserted, so neither an unreached prelude nor a swallowed
    deadline can pass: the prelude must be entered, and nothing after it may run.
    """
    reached = []
    past_prelude = []

    # A live server, so the prelude's `except Exception` arm does NOT legitimately
    # return SERVER_DIED: the only thing that can end this attempt is the deadline.
    monkeypatch.setitem(_CT_GLOBALS, "clickhouse_execute", lambda *a, **k: b"1\n")

    class _TestnameCase:
        """The attributes and hooks the `--testname` prelude touches."""

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.base_url_params = ""
            self.base_client_options = ""
            self.case_file = "/dev/null"
            self.stdout_file = "/dev/null"
            self.stderr_file = "/dev/null"

        # A single Optional[FailureReason]; a tuple is truthy and would
        # early-return SKIPPED before the prelude.
        def should_skip_test(self, *_a, **_k):
            return None

        def should_skip_cloud_test(self, *_a, **_k):
            return (None, None)

        def send_test_name_failed(self, suite, case):
            reached.append(True)
            # Stand in for a hung pre-test query so the pending alarm lands here.
            signal.setitimer(signal.ITIMER_REAL, 0.05)
            while True:
                pass

        def configure_testcase_args(self, args, case_file, suite_tmp_path):
            # The first statement after the prelude. Reaching it means the
            # deadline was absorbed and this attempt is now unbounded.
            past_prelude.append(signal.alarm(0))
            raise AssertionError("unreachable")

        def add_effective_settings(self, client_options):
            return client_options

    # The real `TestCase.run` IS the code under test here: its `except Exception`
    # around the prelude is what must not absorb the deadline.
    try:
        report, exit_code = _drive_run_tests_array(
            monkeypatch,
            _ct["TestCase"].run,
            testname=True,
            case_mixin=_TestnameCase,
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.alarm(0)

    assert reached, "the prelude was never entered, so nothing was measured"
    assert not past_prelude, (
        "the deadline was absorbed by the health-check handlers and the attempt "
        f"continued with {past_prelude[0]} s left on the one-shot alarm, so this "
        "test would run unbounded instead of being reported as timed out"
    )

    leaf = _parsed_leaf(report)
    assert leaf, f"the timed-out test must still produce a parseable leaf:\n{report}"
    assert leaf.group(2) == "[ FAIL ]", f"expected FAIL, got {leaf.group(2)}"
    assert "Timeout" in report, f"the reason must be TIMEOUT:\n{report}"
    assert "server died" not in report.lower(), (
        "a single test's deadline must not be reported as a dead server, which "
        f"aborts the whole run:\n{report}"
    )
    assert rec.own_group_broadcasts == [], (
        f"the deadline must not signal our own process group: "
        f"{rec.own_group_broadcasts}"
    )
    assert exit_code == 1, (
        f"the timed-out test must count as a failure, exit_code={exit_code}"
    )


def test_deadline_does_not_turn_an_interrupted_pass_into_a_pass(rec, monkeypatch):
    """A verdict that says the test succeeded cannot survive its own deadline.

    The recovery arm used to synthesize a TIMEOUT only when no result existed, so
    an OK produced just before the alarm was preserved and formatted: the run
    reported a passing test and kept exit code 0 even though the deadline fired.
    A FAIL is kept, being more specific than the deadline."""
    raised = []

    def run(self, args, suite, client_options):
        return _ct["TestResult"](
            self.name, _ct["TestStatus"].OK, None, 1.0, "\npassed\n"
        )

    def process_result(self, result, messages):
        if not raised:
            raised.append(True)
            raise _ct["TestTimeout"]("Test execution timed out")
        return _ct["TestCase"].process_result(self, result, messages)

    report, exit_code = _drive_run_tests_array(
        monkeypatch, run, process_result=process_result
    )

    assert raised, "the deadline was never delivered, so nothing was measured"
    leaf = _parsed_leaf(report)
    assert leaf, f"the timed-out test must produce a parseable leaf:\n{report}"
    assert leaf.group(2) == "[ FAIL ]", (
        f"a test whose deadline expired must not be reported as passing, "
        f"got {leaf.group(2)}:\n{report}"
    )
    assert "Timeout" in report, f"the reason must be TIMEOUT:\n{report}"
    assert exit_code == 1, (
        f"a timed-out test must fail the run, exit_code={exit_code}"
    )


@pytest.mark.parametrize(
    "status,keeps_verdict",
    [
        # Already fail the run, so they name a more specific problem.
        ("FAIL", True),
        ("UNKNOWN", True),
        ("NOT_FAILED", True),
        # Do not fail the run, so keeping one hides a test that never
        # finished. `SKIPPED` is reachable after a test has executed
        # (`@@SKIP@@`, cloud post-filters, blacklists).
        ("SKIPPED", False),
        ("OK", False),
    ],
)
def test_which_verdicts_survive_their_own_deadline(
    rec, monkeypatch, status, keeps_verdict
):
    """Pin the whole status matrix, not just the two statuses that motivated it.

    Dropping a status from the survivor set silently rewrites a more specific
    verdict as a timeout; adding one lets a success outlive the deadline that
    expired on it."""
    raised = []

    def run(self, args, suite, client_options):
        return _ct["TestResult"](
            self.name,
            getattr(_ct["TestStatus"], status),
            None,
            1.0,
            f"\nverdict {status}\n",
        )

    def process_result(self, result, messages):
        if not raised:
            raised.append(True)
            raise _ct["TestTimeout"]("Test execution timed out")
        return _ct["TestCase"].process_result(self, result, messages)

    report, _exit_code = _drive_run_tests_array(
        monkeypatch, run, process_result=process_result
    )

    assert raised, "the deadline was never delivered, so nothing was measured"
    assert _parsed_leaf(report), f"no parseable leaf:\n{report}"
    if keeps_verdict:
        assert f"verdict {status}" in report, (
            f"a {status} verdict is more specific than the deadline and must be "
            f"kept:\n{report}"
        )
    else:
        assert f"verdict {status}" not in report, (
            f"a {status} verdict cannot outlive the deadline that expired on "
            f"it:\n{report}"
        )
        assert "Timeout" in report, f"it must be reported as a timeout:\n{report}"


def test_deadline_arriving_during_teardown_does_not_escape(rec, monkeypatch):
    """The alarm can be delivered inside the `finally` that cancels it.

    An exception raised from a `finally` is not caught by that statement's own
    `except`, so the deadline would propagate out of `run_tests_array` and, in
    sequential mode, abort the whole run instead of reporting one test."""
    fired = []
    real_alarm = signal.alarm

    def alarm(seconds):
        # Deliver the pending signal exactly at the cancellation point.
        if seconds == 0 and not fired:
            fired.append(True)
            rv = real_alarm(0)
            signal.raise_signal(signal.SIGALRM)
            return rv
        return real_alarm(seconds)

    def run(self, args, suite, client_options):
        return _ct["TestResult"](
            self.name, _ct["TestStatus"].OK, None, 1.0, "\npassed\n"
        )

    monkeypatch.setattr(signal, "alarm", alarm)
    report, exit_code = _drive_run_tests_array(monkeypatch, run)

    assert fired, "the cancellation window was never entered"
    leaf = _parsed_leaf(report)
    assert leaf, (
        f"the run must still report the test rather than aborting:\n{report}"
    )
    # Not propagating is only half of it: the deadline must still be recorded,
    # or swallowing it silently turns the timed-out test into a pass.
    assert leaf.group(2) == "[ FAIL ]", (
        f"the recorded deadline must still fail the test, got {leaf.group(2)}:"
        f"\n{report}"
    )
    assert "Timeout" in report, f"the reason must be TIMEOUT:\n{report}"
    assert exit_code == 1, (
        f"a timed-out test must fail the run, exit_code={exit_code}"
    )


def test_no_handler_around_a_test_can_absorb_the_deadline(monkeypatch, tmp_path):
    """The deadline type is only as strong as the handlers it passes through.

    `kill_process_group` reads the client fatal log while the per-test alarm is
    still armed, so a bare `except` there swallows the deadline whatever its base
    class is. Assert the property on the real function rather than the spelling:
    a deadline raised from the log read must propagate."""
    fatal_log = tmp_path / "client-fatal"
    fatal_log.write_bytes(b"anything")

    def exploding_open(*_a, **_k):
        raise _ct["TestTimeout"]("Test execution timed out")

    monkeypatch.setitem(_CT_GLOBALS, "pgrep", lambda **_k: [])
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: None)
    # The log read sits behind this flag, and sleeps guard it.
    monkeypatch.setitem(_CT_GLOBALS, "CAPTURE_CLIENT_STACKTRACE", True)
    monkeypatch.setitem(_CT_GLOBALS, "SANITIZED", False)
    monkeypatch.setitem(_CT_GLOBALS, "sleep", lambda _s: None)
    # The function resolves `open` through its own module globals, so patching
    # `builtins.open` would not be seen by it.
    monkeypatch.setitem(_CT_GLOBALS, "open", exploding_open)

    with pytest.raises(_ct["TestTimeout"]):
        _ct["kill_process_group"](_OUR_PGID, str(fatal_log))


def test_the_run_continues_with_the_next_test_after_a_deadline(rec, monkeypatch):
    """The whole point: the queue must outlive one test's deadline.

    Every other driver test supplies a single-element queue, so a run that
    stopped right after reporting the timed-out test would satisfy them all.
    Two tests, the first timing out: the second must execute, get its own leaf,
    and the run must still fail because of the first."""
    ran = []

    def run(self, args, suite, client_options):
        ran.append(self.name)
        if self.name == "00001_probe.sql":
            raise _ct["TestTimeout"]("Test execution timed out")
        return _ct["TestResult"](
            self.name, _ct["TestStatus"].OK, None, 1.0, "\npassed\n"
        )

    report, exit_code = _drive_run_tests_array(
        monkeypatch, run, queue=("00001_probe.sql", "00002_after.sql")
    )

    assert ran == ["00001_probe.sql", "00002_after.sql"], (
        f"the run must reach the test after the timed-out one, ran={ran}"
    )
    leaves = [
        _TEST_RESULT_PATTERN.match(line)
        for line in report.splitlines()
        if _TEST_RESULT_PATTERN.match(line)
    ]
    assert len(leaves) == 2, f"both tests must be reported once each:\n{report}"
    assert leaves[0].group(2) == "[ FAIL ]", f"first must be the timeout:\n{report}"
    assert leaves[1].group(2) == "[ OK ]", f"second must pass on merit:\n{report}"
    assert exit_code == 1, (
        f"the timed-out test must still fail the run, exit_code={exit_code}"
    )


def test_the_run_restores_the_environment_it_cannot_prove_was_restored(
    rec, monkeypatch
):
    """Masking narrows the leak window; it cannot close it.

    The deadline can be delivered after `run` enters its `finally` but before
    `pthread_sigmask` has taken effect, and then neither variable is restored
    and the single restore call is already spent. The recovery path therefore
    repeats the restore, where no alarm is armed. Both stores are idempotent,
    so repeating them is free.

    The restore is refused for the whole attempt here, which is the worst case
    the window can produce."""
    dirty = {"CLICKHOUSE_URL_PARAMS": "DIRTY", "CLICKHOUSE_CLIENT_OPT": "LEAK_OPT"}
    env = dict(dirty)
    refused = []

    class _Refusing:
        """Refuses the restore while an alarm could still be delivered."""

        def remove_settings_from_env(self):
            if not refused:
                refused.append(True)
                raise _ct["TestTimeout"]("Test execution timed out")
            env["CLICKHOUSE_URL_PARAMS"] = self.base_url_params
            env["CLICKHOUSE_CLIENT_OPT"] = self.base_client_options

    def run(self, args, suite, client_options):
        self.remove_settings_from_env()
        raise AssertionError("unreachable: the restore refuses first")

    report, exit_code = _drive_run_tests_array(
        monkeypatch, run, case_mixin=_Refusing
    )

    assert refused, "the un-restorable window was never entered"
    assert env == {"CLICKHOUSE_URL_PARAMS": "", "CLICKHOUSE_CLIENT_OPT": ""}, (
        f"the timed-out test's settings outlived it: {env}"
    )
    leaf = _parsed_leaf(report)
    assert leaf and leaf.group(2) == "[ FAIL ]", (
        f"the deadline must still be reported as a failure:\n{report}"
    )
    assert exit_code == 1, f"exit_code={exit_code}"


class _FiringEnv(dict):
    """`os.environ` that delivers a real SIGALRM after the first store."""

    def __init__(self, *a, **k):
        super().__init__(*a, **k)
        self.fired = False

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if key == "CLICKHOUSE_URL_PARAMS" and not self.fired:
            self.fired = True
            signal.raise_signal(signal.SIGALRM)


class _OsWithEnv:
    """`os` with a substituted `environ`, delegating everything else."""

    def __init__(self, environ):
        self.environ = environ

    def __getattr__(self, name):
        return getattr(os, name)


def test_env_restore_is_indivisible_under_the_deadline(monkeypatch):
    """A test's randomized options must not outlive the test that set them.

    `remove_settings_from_env` restores two variables, and every later test in
    the worker reads them as its own baseline. A deadline between the stores
    leaves `CLICKHOUSE_CLIENT_OPT` at the timed-out test's value for the rest of
    the worker's life, so an unrelated later test runs under settings that
    appear nowhere in its own output.

    The SIGALRM is real and is delivered from inside the first store, and the
    deadline must still arrive: masking has to defer it, not swallow it."""
    real_test_case = _ct["TestCase"]
    TestTimeout = _ct["TestTimeout"]

    class Case(real_test_case):
        def __init__(self):  # pylint: disable=super-init-not-called
            self.base_url_params = ""
            self.base_client_options = ""

    env = _FiringEnv(
        {"CLICKHOUSE_URL_PARAMS": "DIRTY", "CLICKHOUSE_CLIENT_OPT": "LEAK_OPT"}
    )

    def handler(_signum, _frame):
        raise TestTimeout("Test execution timed out")

    monkeypatch.setitem(_CT_GLOBALS, "os", _OsWithEnv(env))
    prev = signal.signal(signal.SIGALRM, handler)
    try:
        with pytest.raises(TestTimeout):
            Case().remove_settings_from_env()
    finally:
        signal.signal(signal.SIGALRM, prev)

    assert env.fired, "the window between the two stores was never entered"
    assert env["CLICKHOUSE_CLIENT_OPT"] == "", (
        "the timed-out test's client options leaked into every later test: "
        f"{env['CLICKHOUSE_CLIENT_OPT']!r}"
    )


def test_whole_run_teardown_still_broadcasts_to_the_group(rec):
    """`cleanup_child_processes` is the whole-run primitive (SERVER_DIED,
    KeyboardInterrupt, hung check). Extracting the per-child loop must leave its
    broadcast intact."""
    _ct["cleanup_child_processes"](os.getpid())

    assert rec.own_group_broadcasts == [
        (_OUR_PGID, signal.SIGTERM)
    ], f"cleanup_child_processes must still SIGTERM our group, got {rec.killpg}"
    assert rec.killed_groups == [_OUT_OF_GROUP_PGID]
