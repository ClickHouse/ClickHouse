"""
Tests for `ci.jobs.scripts.functional_tests_results.FTResultsProcessor`.

Specifically covers the CIDB-staging-cluster overload heuristic added on
@alexey-milovidov's request in PR #106154: when the test runner is killed
by the harness wall-clock timeout but the only evidence in the server's
`err.log` is repeated `Distributed`-shipping retries to an unresponsive
CIDB staging log cluster, the run must not be painted red as
`Server died` - the ClickHouse server was healthy.

See ClickHouse/ClickHouse#106154.
"""

import os
import signal
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

# `ci.jobs.scripts.functional_tests_results` imports `praktika` as a
# top-level package (it is run as a script in CI, where `ci/` is on the
# path). Importing `ci.praktika` first triggers the `sys.path` side
# effect in `ci/praktika/__init__.py` that puts `ci/` on `sys.path`,
# making the bare `praktika` import resolvable for the module under test.
import ci.praktika  # noqa: F401

from ci.jobs.scripts.functional_tests_results import (
    FTResultsProcessor,
    ABORTED_RUN_EXIT_CODES,
    is_ci_logs_cluster_overload,
    _STAGING_OVERLOAD_MIN_ERRORS,
)
from ci.praktika.result import Result


# Representative shipping-failure lines as they appear in the server's
# `err.log` when the CIDB staging cluster is overloaded. The real
# samples on PR #106154 mixed `TOO_MANY_PARTS`, `SOCKET_TIMEOUT`, and
# `NETWORK_ERROR` codes; the classifier keys off the logger name, not
# the error code, so any of them work.
_SHIPPING_ERROR_LINE_TOO_MANY_PARTS = (
    "2026.05.30 14:31:02.123456 [ 4242 ] {abc} <Error> "
    "DistributedAsyncInsertQueue: Code: 252. DB::Exception: Too many parts "
    "(...). (TOO_MANY_PARTS), Stack trace ... "
    "remote: kng4alm55c.us-east-2.aws.clickhouse-staging.com:9440"
)
_SHIPPING_ERROR_LINE_SOCKET_TIMEOUT = (
    "2026.05.30 14:32:05.789012 [ 4243 ] {def} <Error> "
    "BgDistSchPool: Code: 209. DB::NetException: Timeout: connect timed out "
    "(SOCKET_TIMEOUT) "
    "kng4alm55c.us-east-2.aws.clickhouse-staging.com:9440"
)
_SHIPPING_ERROR_LINE_NETWORK_ERROR = (
    "2026.05.30 14:33:42.123456 [ 4244 ] {ghi} <Error> "
    "system.query_log_sender: Code: 210. DB::NetException: Connection refused "
    "(NETWORK_ERROR) "
    "kng4alm55c.us-east-2.aws.clickhouse-staging.com:9440"
)

_REAL_FATAL_LINE = (
    "2026.05.30 14:34:00.000000 [ 4245 ] {} <Fatal> BaseDaemon: ########################################"
)
_REAL_SANITIZER_LINE = (
    "==1234==ERROR: AddressSanitizer: heap-use-after-free on address 0x..."
)
_REAL_LOGICAL_ERROR_LINE = (
    "2026.05.30 14:35:00.000000 [ 4246 ] {} <Error> Application: "
    "DB::Exception: LOGICAL_ERROR: invariant violated"
)
_UNRELATED_ERROR_LINE = (
    "2026.05.30 14:30:00.000000 [ 4247 ] {} <Error> TCPHandler: "
    "Code: 60. DB::Exception: Table default.foo does not exist"
)


def _write_err_log(tmp_path: Path, lines: list) -> Path:
    err_log = tmp_path / "clickhouse-server.err.log"
    err_log.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return err_log


# --- is_ci_logs_cluster_overload ---------------------------------------------


def test_overload_classifier_returns_false_when_log_missing(tmp_path):
    missing = tmp_path / "clickhouse-server.err.log"
    assert is_ci_logs_cluster_overload(missing) is False


def test_overload_classifier_returns_false_for_empty_log(tmp_path):
    err_log = _write_err_log(tmp_path, [])
    assert is_ci_logs_cluster_overload(err_log) is False


def test_overload_classifier_detects_pure_shipping_failures(tmp_path):
    lines = []
    for _ in range(_STAGING_OVERLOAD_MIN_ERRORS):
        lines.append(_SHIPPING_ERROR_LINE_TOO_MANY_PARTS)
        lines.append(_SHIPPING_ERROR_LINE_SOCKET_TIMEOUT)
        lines.append(_SHIPPING_ERROR_LINE_NETWORK_ERROR)
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is True


def test_overload_classifier_ignores_log_with_too_few_errors(tmp_path):
    # Under the min-errors threshold the heuristic must abstain even if
    # 100% of the errors are shipping retries — a handful of transient
    # blips is not a chronic overload.
    lines = [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS - 1)
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is False


def test_overload_classifier_rejects_log_with_fatal_marker(tmp_path):
    lines = [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    lines.append(_REAL_FATAL_LINE)
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is False


def test_overload_classifier_rejects_log_with_sanitizer_report(tmp_path):
    lines = [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    lines.append(_REAL_SANITIZER_LINE)
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is False


def test_overload_classifier_rejects_log_with_logical_error(tmp_path):
    lines = [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    lines.append(_REAL_LOGICAL_ERROR_LINE)
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is False


def test_overload_classifier_rejects_log_dominated_by_unrelated_errors(tmp_path):
    # Mostly unrelated errors, only a few shipping retries — keep the
    # `Server died` path.
    lines = [_UNRELATED_ERROR_LINE] * 200 + [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * 50
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is False


def test_overload_classifier_accepts_mostly_shipping_with_minor_noise(tmp_path):
    # 96% shipping retries, 4% unrelated noise — above the 95% threshold.
    shipping = _STAGING_OVERLOAD_MIN_ERRORS * 5
    other = max(1, shipping * 4 // 100 - 1)  # keep below 5%
    lines = [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * shipping + [_UNRELATED_ERROR_LINE] * other
    err_log = _write_err_log(tmp_path, lines)
    assert is_ci_logs_cluster_overload(err_log) is True


# --- FTResultsProcessor.run --------------------------------------------------


def _empty_test_results_file(tmp_path: Path, success_finish: bool = True) -> Path:
    """Write a minimal `test_result.txt` that parses cleanly with zero
    tests and an `All tests have finished` marker so `success_finish` is
    True. The CIDB-staging-cluster heuristic is independent of test
    output content, but the parser still needs a real file."""
    test_output = tmp_path / "test_result.txt"
    text = "All tests have finished\n" if success_finish else ""
    test_output.write_text(text, encoding="utf-8")
    return test_output


def test_run_emits_server_died_when_err_log_missing(tmp_path):
    # Runner killed by SIGTERM but the `err.log` does not exist - keep
    # the legacy `Server died` behaviour.
    _empty_test_results_file(tmp_path)
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(tmp_path / "no-such-file.err.log"),
    )
    result = processor.run(runner_exit_code=-signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    leaf_names = [r.name for r in result.results]
    assert "Server died" in leaf_names
    assert "CIDB log cluster unresponsive" not in leaf_names


def test_run_emits_server_died_on_staging_cluster_overload_without_aborted_exit(tmp_path):
    # Exit code outside the aborted set means the runner finished
    # normally; the overload heuristic must not kick in even if the
    # `err.log` looks like infrastructure noise.
    _empty_test_results_file(tmp_path)
    err_log = _write_err_log(
        tmp_path, [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    )
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
    )
    result = processor.run(runner_exit_code=0)

    leaf_names = [r.name for r in result.results]
    assert "CIDB log cluster unresponsive" not in leaf_names
    assert result.status == Result.Status.OK


def test_run_classifies_staging_overload_as_skipped(tmp_path):
    # Runner killed by harness wall-clock timeout (SIGTERM) + clean test
    # output + `err.log` dominated by shipping retries → `SKIPPED` leaf,
    # outer status stays OK, CI does not go red.
    _empty_test_results_file(tmp_path)
    err_log = _write_err_log(
        tmp_path, [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    )
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
    )

    for code in (
        -signal.SIGTERM,
        -signal.SIGKILL,
        128 + signal.SIGTERM,
        128 + signal.SIGKILL,
    ):
        result = processor.run(runner_exit_code=code)
        assert result.status == Result.Status.OK, (
            f"runner_exit_code={code} should not be FAIL"
        )
        leaf_names = [r.name for r in result.results]
        assert "Server died" not in leaf_names
        assert "CIDB log cluster unresponsive" in leaf_names
        skipped = next(r for r in result.results if r.name == "CIDB log cluster unresponsive")
        assert skipped.status == Result.Status.SKIPPED
        # Non-zero exit must not append the secondary `clickhouse-test FAIL`
        # leaf when we're in the staging-cluster-overload branch.
        assert "clickhouse-test" not in leaf_names


def test_run_keeps_server_died_when_real_crash_marker_present(tmp_path):
    # Same scenario as above plus a single `<Fatal>` line — the heuristic
    # must abstain and the run must be `FAIL` with `Server died`. Real
    # crashes can't hide behind shipping-failure noise.
    _empty_test_results_file(tmp_path)
    err_log = _write_err_log(
        tmp_path,
        [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
        + [_REAL_FATAL_LINE],
    )
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
    )
    result = processor.run(runner_exit_code=-signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    leaf_names = [r.name for r in result.results]
    assert "Server died" in leaf_names
    assert "CIDB log cluster unresponsive" not in leaf_names


def test_run_keeps_server_died_when_tests_failed(tmp_path):
    # Even if the `err.log` looks like a staging overload, if a test
    # actually failed there is a real culprit and the heuristic must
    # abstain.
    test_output = tmp_path / "test_result.txt"
    test_output.write_text(
        "00001_fake_test:                                  [ FAIL ] 1.00 sec.\n"
        "Database: test_db\n"
        "All tests have finished\n",
        encoding="utf-8",
    )
    err_log = _write_err_log(
        tmp_path, [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    )
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
    )
    result = processor.run(runner_exit_code=-signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    leaf_names = [r.name for r in result.results]
    assert "Server died" in leaf_names
    assert "CIDB log cluster unresponsive" not in leaf_names


def test_run_keeps_server_died_when_test_run_was_incomplete(tmp_path):
    # Blocker from clickhouse-gh[bot] correctness review on PR #106176:
    # if the harness wall-clock fires mid-suite, `test_result.txt` lacks
    # the `All tests have finished` marker and `success_finish` is False.
    # Even with an `err.log` dominated by staging-shipping retries, the
    # run is incomplete — not all selected tests ran, so the result must
    # stay `Server died` and CI must stay red.
    _empty_test_results_file(tmp_path, success_finish=False)
    err_log = _write_err_log(
        tmp_path, [_SHIPPING_ERROR_LINE_TOO_MANY_PARTS] * (_STAGING_OVERLOAD_MIN_ERRORS * 5)
    )
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
    )
    result = processor.run(runner_exit_code=-signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    leaf_names = [r.name for r in result.results]
    assert "Server died" in leaf_names
    assert "CIDB log cluster unresponsive" not in leaf_names


def test_overload_classifier_scans_full_file_for_late_fatal_marker(tmp_path):
    # Blocker from clickhouse-gh[bot] correctness review on PR #106176:
    # under chronic staging overload `err.log` can grow to hundreds of
    # MiB. A `<Fatal>` appended after the first 64 MiB of shipping noise
    # — the cap an earlier revision used — must still disqualify the run
    # from being reclassified. The classifier now streams the full file
    # line by line so no byte cap can hide a late marker.
    target_size = 65 * 1024 * 1024  # 65 MiB — past the old 64 MiB cap
    line_with_newline = _SHIPPING_ERROR_LINE_TOO_MANY_PARTS + "\n"
    n_lines = max(
        _STAGING_OVERLOAD_MIN_ERRORS * 5,
        target_size // len(line_with_newline) + 1,
    )
    err_log = tmp_path / "clickhouse-server.err.log"
    with err_log.open("w", encoding="utf-8") as f:
        for _ in range(n_lines):
            f.write(line_with_newline)
        f.write(_REAL_FATAL_LINE + "\n")

    # Sanity-check that the file actually exceeds the old scan window —
    # otherwise this test would not exercise the fix.
    assert err_log.stat().st_size > 64 * 1024 * 1024
    assert is_ci_logs_cluster_overload(err_log) is False


def test_run_keeps_server_died_when_oversized_log_has_late_logical_error(tmp_path):
    # Same shape as the late-`<Fatal>` test but for `LOGICAL_ERROR`, and
    # exercised end-to-end through `FTResultsProcessor.run`. Confirms the
    # full integration path keeps the `Server died` leaf when a real
    # marker hides past the first 64 MiB of shipping noise.
    _empty_test_results_file(tmp_path)
    target_size = 65 * 1024 * 1024  # 65 MiB — past the old 64 MiB cap
    line_with_newline = _SHIPPING_ERROR_LINE_TOO_MANY_PARTS + "\n"
    n_lines = max(
        _STAGING_OVERLOAD_MIN_ERRORS * 5,
        target_size // len(line_with_newline) + 1,
    )
    err_log = tmp_path / "clickhouse-server.err.log"
    with err_log.open("w", encoding="utf-8") as f:
        for _ in range(n_lines):
            f.write(line_with_newline)
        f.write(_REAL_LOGICAL_ERROR_LINE + "\n")

    assert err_log.stat().st_size > 64 * 1024 * 1024
    processor = FTResultsProcessor(
        wd=str(tmp_path),
        server_err_log_path=str(err_log),
    )
    result = processor.run(runner_exit_code=-signal.SIGTERM)

    assert result.status == Result.Status.FAIL
    leaf_names = [r.name for r in result.results]
    assert "Server died" in leaf_names
    assert "CIDB log cluster unresponsive" not in leaf_names


def test_aborted_run_exit_codes_set_includes_expected_codes():
    # Sanity-check that the constants the heuristic branches on are the
    # ones documented in PR #106154's investigation.
    assert -signal.SIGTERM in ABORTED_RUN_EXIT_CODES  # -15
    assert -signal.SIGKILL in ABORTED_RUN_EXIT_CODES  # -9
    assert (128 + signal.SIGTERM) in ABORTED_RUN_EXIT_CODES  # 143
    assert (128 + signal.SIGKILL) in ABORTED_RUN_EXIT_CODES  # 137
