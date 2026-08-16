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

import multiprocessing
import os
import subprocess
import sys
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


def test_carrier_uses_the_value_lock():
    """The read and the write must be one locked transition. Two workers can
    detect different causes at the same time, and an unlocked read-modify-write
    lets the later one displace the earlier."""
    carrier = multiprocessing.Value("i", 0)
    held = []

    class RecordingLock:
        def __init__(self, inner):
            self._inner = inner

        def __enter__(self):
            held.append(True)
            return self._inner.__enter__()

        def __exit__(self, *exc):
            return self._inner.__exit__(*exc)

    real_lock = carrier.get_lock()

    class Probe:
        value = 0

        def get_lock(self):
            return RecordingLock(real_lock)

    probe = Probe()
    assert _runner.try_claim_stop_cause(probe, HUNG_CHECK_EXIT_CODE) is True
    assert probe.value == HUNG_CHECK_EXIT_CODE
    assert held, "the claim did not take the carrier's lock"


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
# lldb over every server process blocks up to 30s each and the collection itself
# is not what these tests assert. STUB_STACKTRACE_SECONDS reinstates a blocking
# window on purpose, so a detector that records its cause only after collecting
# can be beaten by a later one.
_stacktrace_seconds = float(os.environ.get("STUB_STACKTRACE_SECONDS", "0"))
def _stub_stacktraces(*a, **k):
    print("stub: print_c_stacktraces")
    if _stacktrace_seconds:
        import time as _time
        _time.sleep(_stacktrace_seconds)
ns["print_c_stacktraces"] = _stub_stacktraces
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
    stacktrace_seconds=0,
    jobs=2,
    timeout=300,
):
    shim = Path(queries_dir) / "shim.py"
    shim.write_text(_SHIM, encoding="utf-8")
    env = dict(os.environ)
    env["STUB_LIVENESS_FAILS"] = "1" if stub_liveness_fails else "0"
    env["STUB_HEALTH_CHECK_RAISES"] = "1" if stub_health_check_raises else "0"
    env["STUB_STACKTRACE_SECONDS"] = str(stacktrace_seconds)
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


def test_sequential_worker_probe_failure_exits_with_the_hung_check_code(tmp_path):
    """The other side of the `is_concurrent` split: the sequential runner owns the
    main process, so it does call `stop_tests()` and must still exit 5."""
    _make_suite(tmp_path, count=2, seconds=1)
    proc = _run_runner(
        tmp_path,
        ["--testname"],
        stub_liveness_fails=True,
        stub_health_check_raises=True,
        jobs=1,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)


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


def test_time_limit_does_not_steal_a_cause_recorded_during_collection(tmp_path):
    """Both causes fire in one run, and the window between them is real.

    A worker records the probe verdict and then collects stacktraces, which blocks
    (lldb over every server process, up to 30 s each). `stacktrace_seconds` makes
    that window wide enough for the parent's time limit to expire inside it, which
    is the only shape where the two claims genuinely compete. Master loses the
    verdict here and reports the benign `Global time limit reached` at OK.
    """
    _make_suite(tmp_path, count=4, seconds=1)
    proc = _run_runner(
        tmp_path,
        ["--testname", "--global_time_limit", "4"],
        stub_liveness_fails=True,
        stub_health_check_raises=True,
        stacktrace_seconds=8,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)


def test_parent_time_limit_does_not_steal_a_worker_cause(tmp_path):
    """The parent's own periodic probe against the same expiring time limit."""
    _make_suite(tmp_path, count=2, seconds=8)
    proc = _run_runner(
        tmp_path,
        ["--hung-check", "--global_time_limit", "3"],
        stub_liveness_fails=True,
    )
    _assert_exit_code(proc, HUNG_CHECK_EXIT_CODE)


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
