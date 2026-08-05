"""
Tests for `ci.jobs.stress_job.read_test_results` and
`ci.jobs.stress_job.process_results`.

Regression coverage for two flavours of "Unknown job error" in
`Stress test (*)` / `Upgrade check (*)` jobs:

  - The original chronic "Cannot parse test_results.tsv
    (list index out of range)" caused by unguarded indexing on
    blank or short rows (PR #101039 alexey-milovidov directive).
  - A later variant observed on PR #105243 (Stress test arm_debug)
    where stray `apt-get install` output leaked into the result
    file. The parser saw "malformed row at line 2: ['\\n(Reading
    database ... ']" and discarded every valid row, including the
    real `Hung check failed, possible deadlock found` failure on
    line 1.

`read_test_results` must:
  - silently tolerate blank lines (trailing-newline artifacts);
  - skip rows with fewer than 2 cells rather than raising, so that
    pollution mid-file does not erase neighbouring valid rows;
  - return both the valid rows and the malformed-row metadata so
    callers can still surface the corruption.

`process_results` must turn that malformed-row metadata into a
visible `Result` so investigators notice the file is corrupt.
"""

import ast
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.stress_job import (
    process_results,
    read_test_results,
    sanitize_test_result_line,
)
from ci.jobs.scripts.stress.stress import (
    HUNG_CHECK_INFO_BUDGET,
    build_hung_check_info,
    escape_tsv_info,
)


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "test_results.tsv"
    path.write_text(content, encoding="utf-8")
    return path


def test_well_formed_single_row(tmp_path):
    path = _write(tmp_path, "test1\tOK\t1.0\t\n")
    results, malformed = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"
    assert results[0].duration == 1.0
    assert malformed == []


def test_well_formed_multiple_rows(tmp_path):
    path = _write(
        tmp_path, "test1\tOK\t1.0\t\ntest2\tFAIL\t2.5\t['log.txt']\n"
    )
    results, malformed = read_test_results(path)
    assert len(results) == 2
    assert results[0].name == "test1"
    assert results[1].name == "test2"
    assert results[1].duration == 2.5
    assert malformed == []


def test_empty_file_returns_no_rows(tmp_path):
    # A fully empty file is not an error at the parser level; the
    # caller (`process_results`) treats it as "Empty results".
    path = _write(tmp_path, "")
    assert read_test_results(path) == ([], [])


def test_single_trailing_newline_is_tolerated(tmp_path):
    """The dominant cause of the chronic "list index out of range" — a
    file ending with `\\n` makes `csv.reader` emit an empty list for
    the trailing line. It must not blow up."""
    path = _write(tmp_path, "test1\tOK\t1.0\t\n\n")
    results, malformed = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"
    assert malformed == []


def test_multiple_trailing_newlines_are_tolerated(tmp_path):
    path = _write(tmp_path, "test1\tOK\t1.0\t\n\n\n\n")
    results, malformed = read_test_results(path)
    assert len(results) == 1
    assert malformed == []


def test_blank_line_only_returns_no_rows(tmp_path):
    path = _write(tmp_path, "\n")
    assert read_test_results(path) == ([], [])


def test_status_only_two_column_row_is_accepted(tmp_path):
    # Some writers emit only `name<TAB>status<NEWLINE>` (no duration,
    # no info). The parser must accept that.
    path = _write(tmp_path, "test1\tOK\n")
    results, malformed = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"
    assert malformed == []


def test_truncated_tail_row_is_collected_as_malformed(tmp_path):
    path = _write(tmp_path, "test1\tOK\t1.0\t\ntest2\n")
    results, malformed = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"
    assert malformed == [(2, "test2")]


def test_malformed_row_in_middle_does_not_drop_neighbours(tmp_path):
    path = _write(
        tmp_path, "test1\tOK\t1.0\t\nbad_row\ntest2\tFAIL\t2.0\t\n"
    )
    results, malformed = read_test_results(path)
    assert [r.name for r in results] == ["test1", "test2"]
    assert malformed == [(2, "bad_row")]


def test_polluted_file_keeps_real_failure(tmp_path):
    """Regression for PR #105243 Stress test (arm_debug): the real
    `Hung check failed, possible deadlock found  FAIL` row must
    survive even when 100+ lines of stray `apt-get install` output
    have been appended to the file."""
    pollution = "\n".join(f"(Reading database ... {i}%" for i in range(100))
    content = (
        "Hung check failed, possible deadlock found\tFAIL\t\\N\tinfo\n"
        + pollution
        + "\nTest script exit code\tOK\t\\N\t\n"
    )
    path = _write(tmp_path, content)
    results, malformed = read_test_results(path)
    names = [r.name for r in results]
    assert "Hung check failed, possible deadlock found" in names
    assert "Test script exit code" in names
    assert len(malformed) == 100


def test_process_results_surfaces_polluted_file(tmp_path):
    content = (
        "Hung check failed, possible deadlock found\tFAIL\t\\N\tinfo\n"
        "(Reading database ... 5%\n"
        "(Reading database ... 10%\n"
    )
    _write(tmp_path, content)
    server_logs = tmp_path / "server_logs_does_not_exist"
    test_results, _files = process_results(tmp_path, server_logs)
    names = [r.name for r in test_results]
    assert "Hung check failed, possible deadlock found" in names
    assert "Corrupt test_results.tsv" in names
    corrupt = next(r for r in test_results if r.name == "Corrupt test_results.tsv")
    assert "2 malformed row(s)" in (corrupt.info or "")


def test_process_results_all_malformed_reports_corrupt_not_unknown(tmp_path):
    _write(tmp_path, "(Reading database ... 5%\n(Reading database ... 10%\n")
    server_logs = tmp_path / "server_logs_does_not_exist"
    test_results, _files = process_results(tmp_path, server_logs)
    assert len(test_results) == 1
    assert test_results[0].name == "Corrupt test_results.tsv"
    assert test_results[0].name != "Unknown job error"


def test_sanitize_replaces_nul_bytes():
    # Pre-existing behaviour we don't want to regress: NUL bytes in
    # log payloads must be escaped before csv parsing.
    assert sanitize_test_result_line("a\0b") == "a\\0b"
    assert sanitize_test_result_line("plain\tline\n") == "plain\tline\n"


def test_sanitize_strips_carriage_returns():
    # CR is dropped: dpkg/apt-get progress frames captured via
    # `clickhouse-test --capture-client-stacktrace` arrive as
    # `(Reading database ... 5%\r... 10%\r...)` and must not be
    # turned into LF by universal-newlines mode.
    assert sanitize_test_result_line("a\rb\rc") == "abc"


def test_invalid_status_row_is_collected_as_malformed(tmp_path):
    # A row with 2+ cells but an unrecognised status (anything that
    # is not OK / FAIL / ERROR / ...) must not abort parsing — it
    # must be collected as malformed so the neighbouring valid rows
    # still surface.
    path = _write(
        tmp_path,
        "real_failure\tFAIL\t\\N\tdetails\n"
        "weird\tWHATEVER\n"
        "later_check\tOK\n",
    )
    results, malformed = read_test_results(path)
    names = [r.name for r in results]
    assert "real_failure" in names
    assert "later_check" in names
    assert "weird" not in names
    assert len(malformed) == 1
    assert malformed[0][0] == 2


def test_escape_tsv_info_roundtrips_cr_through_parser(tmp_path):
    # The writer encodes CR as `\\r` rather than dropping it, so the
    # parser's unescape pass must restore real CR in the info field.
    # Otherwise the `\\r` would leak into the displayed log.
    from ci.jobs.scripts.stress.stress import escape_tsv_info

    info = "(Reading database ... 5%\r10%\r) done"
    escaped = escape_tsv_info(info)
    assert "\r" not in escaped
    assert "\\r" in escaped
    path = _write(tmp_path, f"row\tFAIL\t\\N\t{escaped}\n")
    results, malformed = read_test_results(path)
    assert malformed == []
    assert len(results) == 1
    assert results[0].info == info


def test_dpkg_progress_in_info_does_not_split_row(tmp_path):
    """Exact failure pattern from PR #105243 Stress test (arm_debug):
    the `Hung check failed` info field contained dpkg progress
    `(Reading database ... N%\\r)` frames. Universal-newlines mode
    turned each `\\r` into an LF and the row exploded into 122
    single-cell fragments, blanking the real failure."""
    dpkg_progress = "".join(
        f"(Reading database ... {p}%\r" for p in (5, 10, 20, 50, 100)
    )
    content = (
        "Hung check failed, possible deadlock found\tFAIL\t\\N\t"
        f"info before\\n{dpkg_progress}info after\n"
        "Test script exit code\tOK\t\\N\t\n"
    )
    path = _write(tmp_path, content)
    results, malformed = read_test_results(path)
    names = [r.name for r in results]
    assert names == [
        "Hung check failed, possible deadlock found",
        "Test script exit code",
    ]
    assert malformed == []


_VERDICT = "Found hung queries in processlist:"


def _hung_log(tmp_path: Path, content) -> Path:
    path = tmp_path / "hung_check.log"
    path.write_bytes(content if isinstance(content, bytes) else content.encode("utf-8"))
    return path


def _processlist(count: int) -> str:
    # `ORDER BY elapsed DESC`: query 0 is the longest-running one.
    return "".join(
        f"query:   SELECT hung_query_{i} FROM t\nelapsed: {count - i}.0\n"
        for i in range(count)
    )


def test_verdict_survives_a_large_log_statistics_section(tmp_path):
    """Regression: `--report-logs-stats` output is unbounded in bytes and printed
    last, so reading the end of the log embedded statistics and never the verdict.
    On the real 309 KB artifact that section is 9.4x the whole budget."""
    log = _hung_log(
        tmp_path,
        "banner\n" * 100
        + f"{_VERDICT}\n"
        + _processlist(1200)
        + "Top patterns of log messages:\n"
        + "count message_format_string\n" * 2000,
    )
    info = build_hung_check_info(log)
    assert _VERDICT in info
    assert "hung_query_0 " in info
    assert "message_format_string" not in info


def test_oldest_query_is_kept_when_the_processlist_exceeds_the_budget(tmp_path):
    """The discriminating case: a genuine processlist larger than the budget, with
    no statistics section at all. Reading the end loses the verdict here too."""
    log = _hung_log(tmp_path, f"banner\n{_VERDICT}\n" + _processlist(1200))
    info = build_hung_check_info(log)
    assert _VERDICT in info
    assert "hung_query_0 " in info
    assert "hung_query_1199 " not in info
    assert "showing the first 32 KiB" in info


def test_a_single_query_line_longer_than_the_budget_is_still_shown(tmp_path):
    """`system.processes.query` is unbounded and `Vertical` does not escape it, so
    one fuzzer query is one line longer than the budget. The window must keep its
    trailing fragment; trimming to a line boundary would erase the processlist."""
    huge = "SELECT hung_query_0, " + "x" * (2 * HUNG_CHECK_INFO_BUDGET)
    log = _hung_log(tmp_path, f"banner\n{_VERDICT}\nquery:   {huge}\n")
    info = build_hung_check_info(log)
    assert "hung_query_0" in info
    assert len(info) > HUNG_CHECK_INFO_BUDGET


def test_hung_check_polling_loop_is_throttled():
    """The loop prints one progress token per probe *before* the verdict, so an
    unthrottled loop can fill the embedded window in the genuine-hang case."""
    source = Path(__file__).resolve().parents[2] / "tests" / "clickhouse-test"
    tree = ast.parse(source.read_text(encoding="utf-8"))

    def calls(node, name):
        return [
            n
            for n in ast.walk(node)
            if isinstance(n, ast.Call) and getattr(n.func, "id", None) == name
        ]

    loops = [
        n
        for n in ast.walk(tree)
        if isinstance(n, ast.While) and calls(n, "get_processlist_size")
    ]
    assert len(loops) == 1, f"expected one hung-check polling loop, found {len(loops)}"
    assert calls(loops[0], "sleep"), "the hung-check polling loop must not spin"


def test_small_log_is_embedded_whole_and_round_trips(tmp_path):
    raw = b"banner\nNo queries hung.\nnul\0tab\tcr\rlf\ninvalid utf-8: \xff\n"
    log = _hung_log(tmp_path, raw)
    info = build_hung_check_info(log)
    assert info == raw.decode("utf-8", errors="replace")
    assert "truncated" not in info
    path = _write(tmp_path, f"row\tFAIL\t\\N\t{escape_tsv_info(info)}\n")
    results, malformed = read_test_results(path)
    assert malformed == []
    # `read_test_results` restores tab/CR/LF; NUL stays escaped as `\0`.
    assert [r.info for r in results] == [info.replace("\0", "\\0")]


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
