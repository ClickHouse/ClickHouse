"""
Guards the two halves of the perf-comparison error report: the `rg` patterns in
`ci/jobs/scripts/perf/compare.sh` that extract a test's error message, and the
`error_tests` counter in `ci/jobs/scripts/perf/report.py` that turns those rows
into the `N errors` check status.

Both failed silently and reinforced each other. The patterns were written in BRE
(`\\(Exception\\|Error\\):[^:]`) for GNU grep and never migrated when the script
moved to `rg`, a Rust-regex engine where `\\(` `\\|` `\\+` are literals. `rg` then
matched nothing, exited 1, and the `||` chain fell through to `head -10 "$log"` -
the documented last resort - so every failing test reported the first ten lines
of a Python traceback instead of its exception. Nothing errored and the Run
Errors table was populated, so it looked like it worked.

`report.py` counted the ROWS of that table, and because the fallback always
emitted exactly ten, every failure reported "10 errors" for one broken test.
That is why the status carried no information about how many tests failed, and
why a triager had to fetch the logs tarball to learn what broke.

The tests drive the real artifacts: the patterns are extracted from `compare.sh`
itself (so they cannot drift from the script they guard) and run against a
recorded `-err.log` from a genuine failure, and the counter is exercised by
running `report.py` end to end on a fixture and reading the status it emits.
Each is asserted in both directions: the old form must be shown to fail, so a
passing test cannot be a control that never had the ability to redden.
"""

import os
import shutil
import subprocess

import pytest

_PERF_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "jobs", "scripts", "perf")
)
_COMPARE_SH = os.path.join(_PERF_DIR, "compare.sh")
_REPORT_PY = os.path.join(_PERF_DIR, "report.py")

# A real `text_index-err.log` from the perf-comparison shard on PR #109744
# (arm_release, master_head, 1/6), trimmed to the frames that matter. The
# traceback comes first, which is precisely why `head` is the wrong answer: the
# exception a triager needs is at the bottom.
_ERR_LOG = """\
Traceback (most recent call last):
  File "/home/ubuntu/actions-runner/_work/ClickHouse/ClickHouse/./tests/performance/scripts/perf.py", line 1122, in <module>
    c.execute(q)
    ~~~~~~~~~^^^
  File "/usr/local/lib/python3.13/dist-packages/clickhouse_driver/client.py", line 382, in execute
    rv = self.process_ordinary_query(
        query, params=params, with_column_types=with_column_types,
    ...<2 lines>...
        columnar=columnar
    )
  File "/usr/local/lib/python3.13/dist-packages/clickhouse_driver/client.py", line 580, in process_ordinary_query
    return self.receive_result(with_column_types=with_column_types,
clickhouse_driver.errors.ServerException: Code: 36.
DB::Exception: Cannot drop index idx_sp because it's affected by mutation with ID 'mutation_22.txt' which is not finished yet. Wait this mutation, or KILL it with command "KILL MUTATION WHERE mutation_id = 'mutation_22.txt'". Stack trace:
"""

# The error shape the second pattern exists for, per its own comment in
# compare.sh. It has no "Exception:"/"Error:" line, so only pattern 2 can find
# it, which keeps that pattern covered rather than shadowed by the first.
_TIMEOUT_LOG = """\
Traceback (most recent call last):
  File "perf.py", line 1122, in <module>
    c.execute(q)
socket.timeout: timed out
"""

_needs_rg = pytest.mark.skipif(
    not shutil.which("rg"), reason="ripgrep is needed to test the patterns it runs"
)


def _extraction_patterns():
    """The two `rg` patterns compare.sh runs, in order, read out of the script.

    Taken from the source rather than duplicated, so a future edit to either
    pattern is tested as-is instead of against a stale copy.
    """
    patterns = []
    with open(_COMPARE_SH, encoding="utf-8") as fd:
        for line in fd:
            stripped = line.strip()
            if not stripped.startswith(("rg ", "|| rg ")):
                continue
            if "--max-count=2" not in stripped or '"$log"' not in stripped:
                continue
            body = stripped.split("'", 1)[1]
            patterns.append(body[: body.rindex("'")])
    assert len(patterns) == 2, f"expected 2 error-extraction patterns, got {patterns}"
    return patterns


def _rg(pattern, log, tmp_path, name="test-err.log"):
    """Run one extraction pattern exactly as compare.sh runs it."""
    path = tmp_path / name
    path.write_text(log, encoding="utf-8")
    proc = subprocess.run(
        ["rg", "--no-filename", "--max-count=2", "-i", pattern, str(path)],
        capture_output=True,
        text=True,
        check=False,
    )
    return proc.returncode, proc.stdout


@_needs_rg
def test_patterns_extract_the_exception_not_the_traceback(tmp_path):
    pattern = _extraction_patterns()[0]
    rc, out = _rg(pattern, _ERR_LOG, tmp_path)
    assert rc == 0, f"pattern {pattern!r} matched nothing; head -10 would be used"
    # Assert on the message a triager acts on, not on an error code that several
    # unrelated failures share.
    assert "Cannot drop index idx_sp" in out
    assert "ServerException: Code: 36." in out
    # The fallback's output must not be what we get.
    assert "Traceback (most recent call last)" not in out


@_needs_rg
def test_bre_patterns_would_extract_nothing(tmp_path):
    # The exact patterns that shipped, kept here as the negative control: if
    # ripgrep ever accepted BRE, the test above could pass without proving the
    # dialect was the defect.
    for pattern in (r"\(Exception\|Error\):[^:]", r"^[^ ]\+: "):
        rc, out = _rg(pattern, _ERR_LOG, tmp_path)
        assert rc == 1 and out == "", f"BRE pattern {pattern!r} unexpectedly matched"


@_needs_rg
def test_second_pattern_covers_errors_without_an_exception_line(tmp_path):
    first, second = _extraction_patterns()
    rc, _ = _rg(first, _TIMEOUT_LOG, tmp_path)
    assert rc == 1, "fixture must not be matched by the first pattern"
    rc, out = _rg(second, _TIMEOUT_LOG, tmp_path)
    assert rc == 0, f"pattern {second!r} matched nothing on a bare error line"
    assert "socket.timeout: timed out" in out


def _run_report(tmp_path, run_errors_tsv):
    """Run report.py on a fixture and return its (status, message).

    Every input it reads is created empty, because a missing file is recorded as
    a report error and replaces the message with "Errors while building the
    report." - which would mask the counter entirely.
    """
    work = tmp_path / "wd"
    (work / "report").mkdir(parents=True)
    (work / "analyze").mkdir()
    (work / "metrics").mkdir()
    (work / "left-commit.txt").write_text("0" * 40, encoding="utf-8")
    (work / "right-commit.txt").write_text("1" * 40, encoding="utf-8")
    (work / "report" / "errors.log").write_text("", encoding="utf-8")
    for name in (
        "partial-queries-report",
        "changed-perf",
        "unconfirmed-changes",
        "unstable-queries",
        "test-perf-changes",
        "test-times",
        "max-single-run-times",
        "all-queries",
    ):
        (work / "report" / f"{name}.tsv").write_text("", encoding="utf-8")
    (work / "analyze" / "skipped-tests.tsv").write_text("", encoding="utf-8")
    (work / "metrics" / "changes.tsv").write_text("", encoding="utf-8")
    (work / "run-errors.tsv").write_text(run_errors_tsv, encoding="utf-8")

    proc = subprocess.run(
        ["python3", _REPORT_PY, "--report", "main"],
        cwd=str(work),
        capture_output=True,
        text=True,
        check=True,
    )
    status = message = None
    for line in proc.stdout.splitlines():
        line = line.strip()
        if line.startswith("<!--status:"):
            status = line[len("<!--status:") : -len("-->")].strip()
        elif line.startswith("<!--message:"):
            message = line[len("<!--message:") : -len("-->")].strip()
    assert status and message, proc.stdout[-2000:]
    return status, message


def test_error_count_is_tests_not_rows(tmp_path):
    # The real artifact shape: one failing test, ten rows, because the fallback
    # emitted `head -10`. This has to read as one error.
    tsv = "".join(f"text_index\tline {i}\n" for i in range(10))
    status, message = _run_report(tmp_path, tsv)
    assert status == "failure"
    assert message.startswith("1 errors"), message


def test_error_count_distinguishes_two_failing_tests(tmp_path):
    # Ten rows for one test and three for another must be two errors, not
    # thirteen: without counting tests, the number tracks message lines and any
    # change to `head`'s argument silently moves it.
    tsv = "".join(f"foo\tline {i}\n" for i in range(10))
    tsv += "".join(f"bar\tline {i}\n" for i in range(3))
    status, message = _run_report(tmp_path, tsv)
    assert status == "failure"
    assert message.startswith("2 errors"), message


def test_any_run_error_still_fails_the_job(tmp_path):
    # The gate is unchanged: a single row is still a failure. Counting tests
    # must not become a way for a failing job to report success.
    status, message = _run_report(tmp_path, "text_index\tboom\n")
    assert status == "failure"
    assert message.startswith("1 errors"), message


def test_no_run_errors_keeps_the_job_green(tmp_path):
    status, message = _run_report(tmp_path, "")
    assert status == "success"
    assert "errors" not in message, message


def test_blank_line_in_run_errors_does_not_crash(tmp_path):
    # Both producers prefix every line with `<test>\t`, so an empty row should
    # not occur; the counter tolerates one anyway rather than raising IndexError
    # inside report.py, where it would surface as "Errors while building the
    # report." and hide the real failure.
    status, message = _run_report(tmp_path, "text_index\tboom\n\nother\tbang\n")
    assert status == "failure"
    assert message.startswith("2 errors"), message
