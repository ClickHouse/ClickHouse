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
timed-out test. The last test drives the real `run_tests_array` through the one
window where `run` cannot convert the alarm into a result (inside its own
`except` arms, past its spent `socket.timeout` arm) and asserts a per-test
FAIL/TIMEOUT is produced, using the production `process_result`.

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
    with pytest.raises(TimeoutError):
        _run_timeout_handler(monkeypatch)

    assert rec.own_group_broadcasts == [], (
        f"timeout_handler signalled our own process group {_OUR_PGID}: "
        f"{rec.own_group_broadcasts}. That kills every parallel worker and the "
        "parent, so the run exits -15 and is reported as 'Server died'."
    )


def test_per_test_timeout_still_kills_the_tests_own_clients(rec, monkeypatch):
    """Counter (b): the fix must not disarm the per-test teardown it replaces."""
    with pytest.raises(TimeoutError):
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


def _drive_run_tests_array(monkeypatch, run_impl, process_result=None):
    """Run the real `run_tests_array` over one test whose `run` is `run_impl`.

    Returns (stdout, exit_code.value). Everything else is production code: the
    result is formatted by the real `TestCase.process_result`.
    """
    real_test_case = _ct["TestCase"]

    class _Case(real_test_case):
        effective_settings: dict = {}
        effective_merge_tree_settings: dict = {}

        def __init__(self, suite, case, args, is_concurrent):
            # pylint: disable=super-init-not-called
            self.name = "00001_probe.sql"
            self.case = case
            self.args = args
            self.suite = suite
            self.testcase_args = None
            self.runs_count = 0

        run = run_impl

    if process_result is not None:
        _Case.process_result = process_result

    class _Args:
        timeout = 60
        testname = False
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
        ["00001_probe.sql"],
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
        1,
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
            raise TimeoutError("Test execution timed out")
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


def test_whole_run_teardown_still_broadcasts_to_the_group(rec):
    """`cleanup_child_processes` is the whole-run primitive (SERVER_DIED,
    KeyboardInterrupt, hung check). Extracting the per-child loop must leave its
    broadcast intact."""
    _ct["cleanup_child_processes"](os.getpid())

    assert rec.own_group_broadcasts == [
        (_OUR_PGID, signal.SIGTERM)
    ], f"cleanup_child_processes must still SIGTERM our group, got {rec.killpg}"
    assert rec.killed_groups == [_OUT_OF_GROUP_PGID]
