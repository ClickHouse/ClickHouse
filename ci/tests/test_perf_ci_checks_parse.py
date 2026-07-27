"""
Tests for `ci.jobs.performance_tests.read_ci_checks_results`.

Regression coverage for a perf-comparison shard that fills the runner root
filesystem and then dies without uploading any artifacts, so the red check
names no query and cannot be acted on.

`compare.sh` writes `ci-checks.tsv` last, through a ClickHouse File-engine
table. `WriteBufferFromFileDescriptor::nextImpl` loops on `::write` and throws
only after a partial write has been flushed, so an ENOSPC failure leaves an
arbitrary byte prefix of the file on disk: 0 bytes, a partial header, or a row
cut mid-field. `csv.DictReader` fills a short row's missing fields with
`restval` (`None` by default), and a truncated header line yields fewer field
names than a data row has cells.

`read_ci_checks_results` must never raise on any of those shapes, because an
exception escapes `main()` and kills the job before praktika uploads the
artifacts - the very failure this parser exists to remove. It must still return
the intact rows, report how many were skipped, and never report a truncated file
as clean: a row cut inside a number still parses, so an unterminated trailing
line has to be dropped rather than trusted.
"""

import inspect
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.performance_tests as performance_tests
from ci.jobs.performance_tests import read_ci_checks_results

# Column names and types exactly as compare.sh's `create table ci_checks engine
# File(TSVWithNamesAndTypes, 'ci-checks.tsv')` writes them, so the fixture
# cannot drift from the real file.
COLUMNS = [
    ("pull_request_number", "UInt32"),
    ("commit_sha", "LowCardinality(String)"),
    ("check_name", "LowCardinality(String)"),
    ("check_status", "LowCardinality(String)"),
    ("check_duration_ms", "UInt64"),
    ("check_start_time", "DateTime"),
    ("test_name", "LowCardinality(String)"),
    ("test_status", "LowCardinality(String)"),
    ("test_duration_ms", "Float64"),
    ("report_url", "String"),
    ("pull_request_url", "String"),
    ("commit_url", "String"),
    ("task_url", "String"),
    ("base_ref", "String"),
    ("base_repo", "String"),
    ("head_ref", "String"),
    ("head_repo", "String"),
]

NAMES = "\t".join(c for c, _ in COLUMNS)
TYPES = "\t".join(t for _, t in COLUMNS)


def _row(test_name, test_status, duration_ms):
    return "\t".join(
        [
            "0",
            "6c5c34bf727ee7a2f0b0f8f4dbc1c0d9e1a2b3c4",
            "Performance Comparison (arm_release, release_base, 2/6)",
            "failure",
            "4740000",
            "2026-07-26 22:11:03",
            test_name,
            test_status,
            duration_ms,
            "https://s3.amazonaws.com/clickhouse-test-reports/report.html",
            "https://github.com/ClickHouse/ClickHouse/commit/6c5c34bf",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
    )


# The summary row carries the report message in test_status and an empty
# test_name; compare.sh emits it before the per-query rows.
SUMMARY = _row("", "5 slower", "0")
QUERY_ROWS = [
    _row("arithmetic.xml #0::old", "slower", "1234.500"),
    _row("arithmetic.xml #1::new", "unstable", "2345.750"),
]


def _write(tmp_path, text):
    path = tmp_path / "ci-checks.tsv"
    path.write_text(text, encoding="utf-8")
    return path


def test_well_formed_file_is_parsed_exactly(tmp_path):
    # The case that must not regress: a complete file yields every query row
    # with its name, status and duration in seconds, and no malformed rows.
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY] + QUERY_ROWS) + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert malformed == 0
    assert [(r.name, r.status, r.duration) for r in results] == [
        ("arithmetic.xml #0::old", "slower", 1.2345),
        ("arithmetic.xml #1::new", "unstable", 2.34575),
    ]


def test_empty_file_is_reported_as_incomplete(tmp_path):
    # A 0-byte file is what an ENOSPC failure in compare.sh's final
    # clickhouse-local leaves behind. Path.is_file() is true for it, so the
    # caller reaches the parser and must not die here.
    path = _write(tmp_path, "")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is False
    assert results == []
    assert malformed == 0


def test_header_line_only_is_reported_as_incomplete(tmp_path):
    # Only the column-names line was flushed; the types line never made it.
    path = _write(tmp_path, NAMES + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is False
    assert results == []


def test_both_headers_no_data_rows_is_complete_and_empty(tmp_path):
    # A legitimate run that produced no query rows is NOT a truncated file:
    # the caller must still import the (empty) result set.
    path = _write(tmp_path, NAMES + "\n" + TYPES + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert results == []
    assert malformed == 0


def test_row_truncated_mid_field_keeps_intact_rows(tmp_path):
    # The row is cut inside test_name, so DictReader fills test_status and
    # test_duration_ms with None. Unguarded, float(None) raises TypeError.
    truncated = QUERY_ROWS[1].split("\t")
    cut = "\t".join(truncated[:6]) + "\tarithmetic.xml #1"
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY, QUERY_ROWS[0], cut]))
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert malformed == 1
    assert [r.name for r in results] == ["arithmetic.xml #0::old"]


def test_row_truncated_inside_duration_keeps_intact_rows(tmp_path):
    # Cut inside test_duration_ms: the field is present but not a number, so
    # the conversion raises ValueError rather than TypeError.
    cells = QUERY_ROWS[1].split("\t")
    cut = "\t".join(cells[:8]) + "\t"
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY, QUERY_ROWS[0], cut]))
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert malformed == 1
    assert [r.name for r in results] == ["arithmetic.xml #0::old"]


def test_truncated_header_line_does_not_raise(tmp_path):
    # The names line itself was cut, so there are fewer field names than a data
    # row has cells. DictReader then has no "test_name" key at all, which would
    # be a KeyError on subscript access.
    short_names = "\t".join(c for c, _ in COLUMNS[:4])
    path = _write(tmp_path, "\n".join([short_names, TYPES, SUMMARY] + QUERY_ROWS))
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert results == []
    assert malformed == 3


def test_summary_row_is_not_imported_as_a_test_case(tmp_path):
    # An empty test_name is the summary row, not a malformed one: it must be
    # skipped silently and must not inflate the malformed count.
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY]) + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert results == []
    assert malformed == 0


def test_unterminated_types_line_is_reported_as_incomplete(tmp_path):
    # The types line was cut mid-write, so it is not a header at all. Counting
    # it as one reports a truncated file as CLEAN, and the caller then imports
    # an empty result set without printing any warning.
    path = _write(tmp_path, NAMES + "\n" + "UInt32\tLowCardinality(Str")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is False
    assert results == []


def test_row_cut_inside_duration_digits_is_not_imported(tmp_path):
    # The nastiest prefix: the file ends part-way through test_duration_ms, so
    # every consumed field is present and the value still parses - as 0.001
    # instead of 1.2345. An unterminated line must be dropped, not trusted.
    cells = QUERY_ROWS[0].split("\t")
    cut = "\t".join(cells[:8]) + "\t1"
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY, cut]))
    results, malformed, complete = read_ci_checks_results(path)
    assert results == []
    assert malformed == 1


def test_short_row_is_malformed_even_when_consumed_fields_are_present(tmp_path):
    # A row carrying the first nine columns has a name, a status and a duration,
    # but it was still cut: the trailing columns are missing. Checking only the
    # consumed fields would import it as valid.
    short = "\t".join(QUERY_ROWS[0].split("\t")[:9])
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY, short]) + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert results == []
    assert malformed == 1


def test_header_cut_after_test_status_does_not_raise(tmp_path):
    # The names line was cut just past test_status, so DictReader has a
    # "test_name" key but no "test_duration_ms" key at all. Reading the duration
    # by subscript rather than .get would be a KeyError here, and the row is not
    # rejected by the name check because test_name is a real string.
    short_names = "\t".join(c for c, _ in COLUMNS[:8])
    path = _write(tmp_path, "\n".join([short_names, TYPES, SUMMARY] + QUERY_ROWS) + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert results == []
    assert malformed == 2


def test_every_truncation_offset_is_survivable(tmp_path):
    # The ENOSPC prefix length is arbitrary, so sweep every offset of a
    # realistic file rather than sampling a few shapes. Two invariants: nothing
    # raises, and a prefix reported clean never carries a value that differs
    # from the truth (returning FEWER rows is fine - that is an honest prefix).
    text = "\n".join([NAMES, TYPES, SUMMARY] + QUERY_ROWS) + "\n"
    truth = [
        ("arithmetic.xml #0::old", "slower", 1.2345),
        ("arithmetic.xml #1::new", "unstable", 2.34575),
    ]
    data = text.encode("utf-8")
    path = tmp_path / "ci-checks.tsv"
    for offset in range(len(data) + 1):
        path.write_bytes(data[:offset])
        results, malformed, complete = read_ci_checks_results(path)
        assert isinstance(malformed, int)
        assert isinstance(complete, bool)
        got = [(r.name, r.status, r.duration) for r in results]
        if complete and malformed == 0:
            assert got == truth[: len(got)], offset


def test_every_truncation_offset_of_a_non_ascii_file_is_survivable(tmp_path):
    # A byte prefix of a UTF-8 file can end inside a multi-byte character, which
    # a strict decode rejects with UnicodeDecodeError. test_name comes from an
    # .xml basename and query_display_name from arbitrary query text, so the
    # decode must be lenient rather than assume ASCII.
    # U+00E9 is two bytes in UTF-8, so some prefix lengths cut it in half.
    rows = [r.replace("arithmetic", "arithm\u00e9tic") for r in QUERY_ROWS]
    data = ("\n".join([NAMES, TYPES, SUMMARY] + rows) + "\n").encode("utf-8")
    path = tmp_path / "ci-checks.tsv"
    for offset in range(len(data) + 1):
        path.write_bytes(data[:offset])
        results, malformed, complete = read_ci_checks_results(path)
        assert isinstance(malformed, int)
        assert isinstance(complete, bool)


def test_ci_checks_import_is_wired_into_main():
    # The helper is only half the guard: main() has to keep calling it and keep
    # surfacing both of its signals. These couplings are invisible to the tests
    # above, which call the helper directly.
    source = inspect.getsource(performance_tests.main)
    assert "read_ci_checks_results(" in source, (
        "main() must parse ci-checks.tsv through the guarded helper. An inlined "
        "raw parse lets an exception escape main() and kill the job before "
        "praktika uploads any artifacts, which is the failure being fixed."
    )
    assert "next(f)" not in source, (
        "A bare next() on the file object raises StopIteration on an empty or "
        "header-only ci-checks.tsv, which is exactly the ENOSPC shape."
    )
    assert "is empty or truncated" in source, (
        "A truncated ci-checks.tsv must be reported. Without the warning the "
        "shard goes red with no query rows and nothing says why."
    )
    assert "malformed row(s)" in source, (
        "Rows dropped as malformed must be reported, otherwise a partially "
        "written file silently imports fewer queries than it ran."
    )
    assert "results[-2].results = test_results" in source, (
        "The parsed rows must still be imported into the previous subtask; "
        "dropping the assignment silently loses every per-query CIDB row."
    )


if __name__ == "__main__":
    import tempfile
    from pathlib import Path

    for name, fn in sorted(globals().items()):
        if not (name.startswith("test_") and callable(fn)):
            continue
        if "tmp_path" in inspect.signature(fn).parameters:
            with tempfile.TemporaryDirectory() as d:
                fn(Path(d))
        else:
            fn()
    print("All perf ci-checks parse tests passed.")
