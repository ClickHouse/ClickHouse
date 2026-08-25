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

import ast
import inspect
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.performance_tests as performance_tests
from ci.jobs.performance_tests import import_ci_checks_results, read_ci_checks_results

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


def test_headers_without_any_data_row_is_reported_as_incomplete(tmp_path):
    # Both header lines were flushed but no data row was. That is not a run that
    # produced no queries: compare.sh's upload_results unions an unconditional
    # single-row summary select (`select '' test_name, <report message>, ...`)
    # into every ci-checks.tsv, so the shortest file a completed run can write is
    # two header lines plus that summary row. A data-row-less file is therefore a
    # prefix and must not be imported.
    path = _write(tmp_path, NAMES + "\n" + TYPES + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is False
    assert results == []
    assert malformed == 0


def test_row_truncated_mid_field_keeps_intact_rows(tmp_path):
    # The row is cut inside test_name and is a COMPLETE line (the fixture is
    # newline-terminated), so it reaches the row guards rather than the
    # trailing-fragment rule. DictReader fills every field past the cut with
    # None, so the guard block rejects it; without any row-level rejection
    # float(None) would raise TypeError and kill the job. A cut row that is the
    # file's LAST line is dropped by the trailing-newline rule instead - that
    # shape is covered by test_row_cut_inside_duration_digits_is_not_imported
    # and test_unterminated_types_line_is_reported_as_incomplete.
    truncated = QUERY_ROWS[1].split("\t")
    cut = "\t".join(truncated[:6]) + "\tarithmetic.xml #1"
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY, QUERY_ROWS[0], cut]) + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert malformed == 1
    assert [r.name for r in results] == ["arithmetic.xml #0::old"]


def test_row_truncated_inside_duration_keeps_intact_rows(tmp_path):
    # Cut right at the start of test_duration_ms, on a COMPLETE line: the field
    # itself is an empty string, but every column past it is missing, so the
    # all-columns guard rejects the row. That guard is what makes a row's
    # completeness independent of which fields the parser happens to consume.
    cells = QUERY_ROWS[1].split("\t")
    cut = "\t".join(cells[:8]) + "\t"
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY, QUERY_ROWS[0], cut]) + "\n")
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


def test_row_with_an_unparsable_duration_is_malformed(tmp_path):
    # The one shape that reaches the float() conversion guard. The names line was
    # cut just past test_duration_ms, so that column is the LAST field: the row
    # carries a value for every declared field and the all-columns guard lets it
    # through, but the value is the empty string left by a row cut at the start
    # of the duration. float("") raises ValueError, which unguarded would kill
    # main() before praktika uploads any artifacts.
    short_names = "\t".join(c for c, _ in COLUMNS[:9])
    cells = QUERY_ROWS[0].split("\t")
    cut = "\t".join(cells[:8]) + "\t"
    path = _write(tmp_path, "\n".join([short_names, TYPES, SUMMARY, cut]) + "\n")
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert results == []
    assert malformed == 1


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


def test_unterminated_row_with_every_field_present_is_not_imported(tmp_path):
    # The prefix stopped exactly at a row's last byte, so that row carries every
    # declared column and each value parses correctly. Neither the all-columns
    # guard nor the duration conversion can reject it, and the value it would
    # import is the right one - so only the unterminated-line rule can catch it.
    # That rule is what makes the file's own framing load-bearing: a completed
    # `File(TSVWithNamesAndTypes)` write is newline-terminated, so a final line
    # without its newline was cut mid-write no matter how much of it arrived.
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY] + QUERY_ROWS))
    results, malformed, complete = read_ci_checks_results(path)
    assert complete is True
    assert malformed == 1
    assert [r.name for r in results] == ["arithmetic.xml #0::old"]


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
        # Unconditionally, not only for prefixes reported clean: whatever rows a
        # prefix yields must be an exact prefix of the truth. Guarding this on
        # `complete and malformed == 0` would let a wrong VALUE through on any
        # offset the parser flags, which is the shape a trailing-fragment-
        # trusting parser produces.
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


# A value no import can produce, so its survival proves the assignment was
# skipped rather than merely assigned something empty.
_SENTINEL = object()


def _import_stub():
    # A stand-in for the praktika Result the importer assigns into. Constructing
    # a real Result pulls in the job environment, and only the `.results`
    # attribute matters here.
    target = SimpleNamespace(results=_SENTINEL)
    # The real call passes the whole results list and the importer addresses
    # results[-2], so mirror that shape rather than passing the target directly.
    return target, [target, SimpleNamespace(results=[])]


def test_import_assigns_parsed_rows_and_prints_no_warning(tmp_path, capsys):
    # The happy path: a complete file is imported into the previous subtask and
    # neither warning is printed.
    path = _write(tmp_path, "\n".join([NAMES, TYPES, SUMMARY] + QUERY_ROWS) + "\n")
    target, results = _import_stub()
    assert import_ci_checks_results(path, results) is True
    assert [(r.name, r.status, r.duration) for r in target.results] == [
        ("arithmetic.xml #0::old", "slower", 1.2345),
        ("arithmetic.xml #1::new", "unstable", 2.34575),
    ]
    out = capsys.readouterr().out
    assert "is empty or truncated" not in out
    assert "malformed row(s)" not in out


def test_import_of_a_truncated_file_warns_and_assigns_nothing(tmp_path, capsys):
    # An incomplete file must be reported and left unimported, so the warning
    # names the real cause instead of the shard going red with no explanation.
    # The sentinel proves the assignment did not happen, which is what an
    # inverted `if not complete:` would break. It is the report that differs:
    # the real target's row list is empty before the call either way.
    path = _write(tmp_path, NAMES + "\n" + "UInt32\tLowCardinality(Str")
    target, results = _import_stub()
    assert import_ci_checks_results(path, results) is False
    assert target.results is _SENTINEL
    assert "is empty or truncated" in capsys.readouterr().out


def test_import_of_an_absent_file_warns_and_assigns_nothing(tmp_path, capsys):
    # This is the path the atomic publish takes on a failed write: upload_results
    # deliberately leaves the final ci-checks.tsv missing rather than publishing
    # a torn one, so an absent file is now an EXPECTED outcome rather than a
    # freak one. An unguarded open() here raises FileNotFoundError out of main()
    # before praktika uploads the artifacts - the shard-level ERROR with no
    # report that this change exists to remove.
    path = str(tmp_path / "ci-checks.tsv")
    assert not os.path.exists(path)
    target, results = _import_stub()
    assert import_ci_checks_results(path, results) is False
    assert target.results is _SENTINEL
    assert "did not generate ci-checks.tsv" in capsys.readouterr().out


def test_import_reports_malformed_rows_and_keeps_the_intact_ones(tmp_path, capsys):
    # A partially written file still imports its intact rows, but the drop has
    # to be visible in the log - otherwise the shard reports fewer queries than
    # it ran and nothing says why.
    cells = QUERY_ROWS[1].split("\t")
    cut = "\t".join(cells[:6]) + "\tarithmetic.xml #1"
    path = _write(
        tmp_path, "\n".join([NAMES, TYPES, SUMMARY, QUERY_ROWS[0], cut]) + "\n"
    )
    target, results = _import_stub()
    assert import_ci_checks_results(path, results) is True
    assert [r.name for r in target.results] == ["arithmetic.xml #0::old"]
    assert "had 1 malformed row(s)" in capsys.readouterr().out


def test_ci_checks_import_is_wired_into_main():
    # The importer is only half the guard: main() has to keep calling it. This
    # coupling is invisible to the tests above, which call it directly.
    source = inspect.getsource(performance_tests.main)
    assert "import_ci_checks_results(" in source, (
        "main() must import ci-checks.tsv through the guarded helper. An inlined "
        "raw parse lets an exception escape main() and kill the job before "
        "praktika uploads any artifacts, which is the failure being fixed."
    )
    assert "next(f)" not in source, (
        "A bare next() on the file object raises StopIteration on an empty or "
        "header-only ci-checks.tsv, which is exactly the ENOSPC shape."
    )
    # The presence check and its warning live in the helper, not in main(), so
    # that all three outcomes (absent / truncated / complete) are behaviourally
    # testable. They are still asserted, just against the function that now owns
    # them.
    helper_source = inspect.getsource(performance_tests.import_ci_checks_results)
    assert "Path(path).is_file()" in helper_source, (
        "A shard that died before compare.sh wrote ci-checks.tsv at all leaves "
        "no file: import_ci_checks_results() must keep its is_file() guard, or "
        "open() raises FileNotFoundError and kills the job before the artifacts "
        "upload."
    )
    assert "did not generate ci-checks.tsv" in helper_source, (
        "import_ci_checks_results() must report the absent-file case too, "
        "otherwise the shard goes red with no query rows and no explanation."
    )


def test_no_subtask_main_appends_carries_rows_to_preserve():
    # Why `if not complete: return False` is a diagnostic and not a data guard.
    # Reading it as data preservation invites the opposite fix - treating a
    # partial parse as non-importable to "avoid overwriting" the target - which
    # would trade a reported warning for a silent one. Every element main()
    # appends before the import site is built without a `results=` argument, so
    # the assignment target's row list is empty whatever the parse returned.
    # Asserted over the append sites themselves rather than one simulated run,
    # because it has to hold for every stage entry point `--param` can select.
    tree = ast.parse(inspect.getsource(performance_tests))
    main_node = next(
        n
        for n in tree.body
        if isinstance(n, ast.FunctionDef) and n.name == "main"
    )
    call_line = min(
        n.lineno
        for n in ast.walk(main_node)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Name)
        and n.func.id == "import_ci_checks_results"
    )
    appends = [
        n.args[0]
        for n in ast.walk(main_node)
        if isinstance(n, ast.Call)
        and isinstance(n.func, ast.Attribute)
        and n.func.attr == "append"
        and isinstance(n.func.value, ast.Name)
        and n.func.value.id == "results"
        and n.lineno < call_line
    ]
    assert appends, "expected results.append() calls before the import site"
    with_rows = [
        ast.unparse(a)
        for a in appends
        if isinstance(a, ast.Call)
        and any(k.arg == "results" for k in a.keywords)
    ]
    assert not with_rows, (
        "A subtask appended before the ci-checks.tsv import now carries "
        f"sub-results of its own: {with_rows}. The importer assigns over "
        "results[-2].results, so that assignment is no longer harmless and the "
        "docstring's reasoning needs revisiting."
    )


def test_import_helper_reports_both_signals():
    # The warning strings and the assignment target are pinned here as well as
    # behaviourally, so that a rewrite cannot quietly drop a log line the
    # operator greps for.
    source = inspect.getsource(performance_tests.import_ci_checks_results)
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
    import contextlib
    import io
    import tempfile
    from pathlib import Path

    class _Capsys:
        """Minimal stand-in for pytest's capsys, for running this file directly."""

        def __init__(self, buffer):
            self._buffer = buffer

        def readouterr(self):
            return SimpleNamespace(out=self._buffer.getvalue(), err="")

    for name, fn in sorted(globals().items()):
        if not (name.startswith("test_") and callable(fn)):
            continue
        params = inspect.signature(fn).parameters
        kwargs = {}
        with contextlib.ExitStack() as stack:
            if "tmp_path" in params:
                kwargs["tmp_path"] = Path(stack.enter_context(tempfile.TemporaryDirectory()))
            if "capsys" in params:
                buffer = io.StringIO()
                stack.enter_context(contextlib.redirect_stdout(buffer))
                kwargs["capsys"] = _Capsys(buffer)
            fn(**kwargs)
    print("All perf ci-checks parse tests passed.")
