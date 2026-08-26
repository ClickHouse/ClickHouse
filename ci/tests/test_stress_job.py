"""
Tests for `ci.jobs.stress_job.read_test_results`.

Regression coverage for the chronic infrastructure failure
"Cannot parse test_results.tsv (list index out of range)"
that surfaced on `Stress test (*)` / `Upgrade check (*)` jobs across
many unrelated PRs (see PR #101039 alexey-milovidov directive).

The unguarded `line[0]` / `line[1]` access in the original parser
turned three different kinds of well-behaved-but-imperfect TSV files
into the unhelpful "Unknown job error":

  - a single trailing newline at end-of-file
  - multiple blank lines between content
  - a truncated tail row (writer crashed mid-write)

The first two should be silently tolerated. The third should surface
a useful error that names the offending line number and its content,
not the opaque `list index out of range`.
"""

import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from ci.jobs.stress_job import read_test_results, sanitize_test_result_line


def _write(tmp_path: Path, content: str) -> Path:
    path = tmp_path / "test_results.tsv"
    path.write_text(content, encoding="utf-8")
    return path


def test_well_formed_single_row(tmp_path):
    path = _write(tmp_path, "test1\tOK\t1.0\t\n")
    results = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"
    assert results[0].duration == 1.0


def test_well_formed_multiple_rows(tmp_path):
    path = _write(
        tmp_path, "test1\tOK\t1.0\t\ntest2\tFAIL\t2.5\t['log.txt']\n"
    )
    results = read_test_results(path)
    assert len(results) == 2
    assert results[0].name == "test1"
    assert results[1].name == "test2"
    assert results[1].duration == 2.5


def test_empty_file_returns_no_rows(tmp_path):
    # A fully empty file is not an error at the parser level; the
    # caller (`process_results`) treats it as "Empty results".
    path = _write(tmp_path, "")
    assert read_test_results(path) == []


def test_single_trailing_newline_is_tolerated(tmp_path):
    """The dominant cause of the chronic "list index out of range" — a
    file ending with `\\n` makes `csv.reader` emit an empty list for
    the trailing line. It must not blow up."""
    path = _write(tmp_path, "test1\tOK\t1.0\t\n\n")
    results = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"


def test_multiple_trailing_newlines_are_tolerated(tmp_path):
    path = _write(tmp_path, "test1\tOK\t1.0\t\n\n\n\n")
    results = read_test_results(path)
    assert len(results) == 1


def test_blank_line_only_returns_no_rows(tmp_path):
    path = _write(tmp_path, "\n")
    assert read_test_results(path) == []


def test_status_only_two_column_row_is_accepted(tmp_path):
    # Some writers emit only `name<TAB>status<NEWLINE>` (no duration,
    # no info). The parser must accept that.
    path = _write(tmp_path, "test1\tOK\n")
    results = read_test_results(path)
    assert len(results) == 1
    assert results[0].name == "test1"


def test_truncated_tail_row_surfaces_useful_error(tmp_path):
    path = _write(tmp_path, "test1\tOK\t1.0\t\ntest2\n")
    with pytest.raises(ValueError) as exc_info:
        read_test_results(path)
    msg = str(exc_info.value)
    # Investigator-friendly: includes the line number, the file path,
    # and the offending row content.
    assert "line 2" in msg
    assert "test_results.tsv" in msg
    assert "test2" in msg


def test_malformed_row_in_middle_surfaces_useful_error(tmp_path):
    path = _write(
        tmp_path, "test1\tOK\t1.0\t\nbad_row\ntest2\tFAIL\t2.0\t\n"
    )
    with pytest.raises(ValueError) as exc_info:
        read_test_results(path)
    msg = str(exc_info.value)
    assert "line 2" in msg
    assert "bad_row" in msg


def test_sanitize_replaces_nul_bytes():
    # Pre-existing behaviour we don't want to regress: NUL bytes in
    # log payloads must be escaped before csv parsing.
    assert sanitize_test_result_line("a\0b") == "a\\0b"
    assert sanitize_test_result_line("plain\tline\n") == "plain\tline\n"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
