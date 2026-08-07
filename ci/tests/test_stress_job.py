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

import os
import re
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.stress_job import (
    process_results,
    read_test_results,
    sanitize_test_result_line,
)

_DOCKER_SCRIPTS = Path(__file__).resolve().parents[2] / "tests" / "docker_scripts"


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


def test_info_codec_is_reversible_for_literal_backslashes(tmp_path):
    """A payload containing a literal backslash must survive the round trip.

    Before the escape character was itself escaped, an on-disk `\\n` was
    ambiguous and the reader always read it as an encoded LF: a fuzzer
    query `SELECT '\\n'` was displayed as a real line break, and
    `C:\\new\\table` as `C:<LF>ew<TAB>able`.
    """
    from ci.jobs.scripts.stress.stress import escape_tsv_info

    # A field-initial `"` must not make csv consume the following rows.
    path = _write(
        tmp_path,
        'row1\tFAIL\t\\N\t"unterminated hello\n'
        "row2\tOK\t\\N\t\n"
        "row3\tFAIL\t\\N\tthird\n",
    )
    results, malformed = read_test_results(path)
    assert [r.name for r in results] == ["row1", "row2", "row3"]
    assert malformed == []
    assert results[0].info == '"unterminated hello'

    payloads = {
        # Lost before the fix.
        "literal_n": "SELECT '\\n'",
        "literal_t": "SELECT '\\t'",
        "literal_r": "SELECT '\\r'",
        "doubled": "SELECT '\\\\n'",
        "windows_path": "C:\\new\\table",
        "trailing": "SELECT 'x\\",
        # The double quote is an ordinary character in the `Escaped` rule,
        # so neither producer escapes it and the reader must not treat it
        # as structural.
        "dq_paired": 'he said "hello" ok',
        "dq_leading": '"quoted"',
        "dq_unterminated": '"unterminated hello',
        # Controls: these already round-tripped and must keep doing so.
        "plain": "SELECT 1 FROM numbers(10)",
        "real_lf": "line one\nline two",
        "real_tab": "col1\tcol2",
        "real_cr": "a\rb",
        "real_nul": "a\0b",
        "literal_nul": "SELECT '\\0'",
    }
    content = "".join(
        f"{name}\tFAIL\t\\N\t{escape_tsv_info(text)}\n"
        for name, text in payloads.items()
    )
    results, malformed = read_test_results(_write(tmp_path, content))
    assert malformed == []
    assert {r.name: r.info for r in results} == payloads

    # On-disk cell as emitted by `escaped()` in stress_tests.lib, which
    # escapes the single quote and the whole `Escaped` alphabet. The
    # backspace and form feed only ever reach the file from this producer:
    # `escape_tsv_info` passes those two bytes through unescaped.
    path = _write(
        tmp_path,
        "Cannot start clickhouse-server\tFAIL\t\\N\t"
        "Code: 695. Load job \\'a\\bb\\fc\\0d\\' failed\\n\n",
    )
    results, malformed = read_test_results(path)
    assert malformed == []
    assert results[0].info == "Code: 695. Load job 'a\bb\fc\0d' failed\n"


def test_shell_producers_append_escaped_output_with_printf():
    """The shell half of the codec, which the Python tests cannot observe.

    `escaped()` emits the row terminator at the same escape level as the
    payload, so a caller appending its output through `echo -e` would
    collapse both one level: the terminator becomes a real LF and the row
    explodes into several physical lines, while every payload backslash is
    destroyed. `printf '%s'` appends the substitution verbatim.
    """
    escaped_call = re.compile(r"\$\((escaped|head_escaped|trim_server_logs)\b")
    terminator = re.compile(r"format_custom_row_after_delimiter='(.*?)'")
    printf_format = re.compile(r'printf\s+"([^"]*)"')

    printf_sites = 0
    terminator_settings = 0
    for name in ("stress_tests.lib", "upgrade_runner.sh"):
        source = (_DOCKER_SCRIPTS / name).read_text(encoding="utf-8")
        for number, line in enumerate(source.split("\n"), start=1):
            stripped = line.lstrip()
            if stripped.startswith("#") or not escaped_call.search(line):
                continue
            where = f"{name}:{number}"
            assert "echo -e" not in line, f"{where}: appends with `echo -e`"
            assert "printf " in line, f"{where}: does not append with `printf`"
            # `printf` interprets escapes in its format, so the escaped payload
            # must be a `%s` argument; `%b` or an inline substitution re-collapse it.
            fmt = printf_format.search(line)
            assert fmt, f"{where}: no double-quoted printf format"
            assert not escaped_call.search(
                fmt.group(1)
            ), f"{where}: escaped output is inside the printf format"
            assert "%s" in fmt.group(1), f"{where}: format has no `%s` conversion"
            printf_sites += 1
        for value in terminator.findall(source):
            terminator_settings += 1
            # The emitted terminator is the two characters `\n`, which a
            # double-quoted bash string spells with four backslashes.
            assert value == "\\\\\\\\n", f"{name}: terminator is {value!r}"

    assert printf_sites == 9
    # Counted over both files, since only `escaped()` sets it: left unset the
    # terminator defaults to a real newline and every multi-line row splits.
    assert (
        terminator_settings == 1
    ), f"format_custom_row_after_delimiter set {terminator_settings} times, want 1"


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


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
