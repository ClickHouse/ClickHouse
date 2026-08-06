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

The `run_stress_test` tests at the bottom cover a third flavour: a
server abort that is collected into `fatal.log` and then discarded,
because the crash parser used to be gated on a `Server died` test
row that only the single early liveness probe in `stress.py` writes.
The job then reported the content-free `Test script failed` /
`script exit code: 1`, losing the assertion and the stack trace
(observed on `Stress test (*)` for 7 shas over 7 days, master
included). The parser must run on the collected fatal evidence too.
"""

import os
import re
import shlex
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.stress_job as stress_job
from ci.jobs.stress_job import (
    process_results,
    read_test_results,
    sanitize_test_result_line,
)
from ci.praktika.result import Result
from ci.praktika.settings import Settings


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


# --------------------------------------------------------------------------
# `run_stress_test`: a collected server abort must be parsed and reported
# --------------------------------------------------------------------------

# Verbatim shape from master e230874627deff161d87fe26e280441bf6074c68
# (`Stress test (amd_debug)`), whose uploaded fatal.log was 81453 bytes while
# CIDB recorded only `Test script failed`.
FATAL_ABORT_LOG = """\
2026.08.05 11:02:07.678235 [ 698 ] {} <Warning> Application: Cannot set max size of core file
2026.08.05 11:31:00.100000 [ 900 ] {} <Error> TCPHandler: Code: 60. DB::Exception: Unknown table
2026.08.05 11:32:07.162513 [ 13898 ] {q1} <Fatal> : Logical error: 'Digest does not match'.
2026.08.05 11:32:07.399020 [ 3282 ] {} <Fatal> BaseDaemon: ########## Short fault info ############
2026.08.05 11:32:07.414671 [ 3282 ] {} <Fatal> BaseDaemon: (version 26.8.1.804 (official build), \
build id: 959B, git hash: e2308746, architecture: x86_64) (from thread 13898) Received signal 6
2026.08.05 11:32:07.418410 [ 3282 ] {} <Fatal> BaseDaemon: Signal description: Aborted
2026.08.05 11:32:07.426334 [ 3282 ] {} <Fatal> BaseDaemon: Stack trace: 0x00007f667397a9fd
2026.08.05 11:32:07.443113 [ 3282 ] {} <Fatal> BaseDaemon: 3. pthread_kill @ 0x0000000000096
2026.08.05 11:32:07.444113 [ 3282 ] {} <Fatal> BaseDaemon: 6. /ClickHouse/src/Common/Exception.cpp:66:5: \
DB::abortOnFailedAssertion(String const&) @ 0x0000000014671
2026.08.05 11:32:07.445113 [ 3282 ] {} <Fatal> BaseDaemon: 8. /ClickHouse/src/Columns/IColumn.h:941:9: \
DB::IColumn::assertTypeEquality(DB::IColumn const&) const @ 0x000000000d5b4
2026.08.05 11:32:07.446113 [ 3282 ] {} <Fatal> BaseDaemon: Integrity check of the executable \
successfully passed (checksum: 0116483704)
2026.08.05 11:32:07.447113 [ 3282 ] {} <Fatal> BaseDaemon: ClickHouse version 26.8.1.804 is old \
and should be upgraded to the latest version.
"""

# A green run still writes plenty of log, just no <Fatal>. Keeping this
# realistic is what makes the "no false FAIL" arm meaningful.
CLEAN_LOG = """\
2026.08.05 11:02:07.678235 [ 698 ] {} <Warning> Application: Cannot set max size of core file
2026.08.05 11:10:00.000000 [ 900 ] {} <Error> TCPHandler: Code: 60. DB::Exception: Unknown table
2026.08.05 11:20:00.000000 [ 901 ] {} <Warning> MergeTreeData: Table is in readonly mode
2026.08.05 11:30:00.000000 [ 902 ] {} <Information> Application: Ready for connections
"""

# Only the benign lines the signal handler also emits at <Fatal> level, with
# no abort: `SignalListener::onFault` is the sole producer of all of them, so
# in a real log they never appear alone. Pins that the widened trigger cannot
# name a row after a version banner.
BENIGN_FATAL_ONLY_LOG = """\
2026.08.05 11:30:00.000000 [ 902 ] {} <Information> Application: Ready for connections
2026.08.05 11:32:07.446113 [ 3282 ] {} <Fatal> BaseDaemon: Integrity check of the executable \
successfully passed (checksum: 0116483704)
2026.08.05 11:32:07.447113 [ 3282 ] {} <Fatal> BaseDaemon: ClickHouse version 26.8.1.804 is old \
and should be upgraded to the latest version.
2026.08.05 11:32:07.448113 [ 3282 ] {} <Fatal> BaseDaemon: Application: Child process was \
terminated by signal 6.
"""

# The bucket-B carrier's real test_results.tsv: the wrapper's own exit row plus
# the durability gates, all of which passed.
BUCKET_B_TSV = (
    "Test script failed\tFAIL\t\\N\t script exit code: 1\n"
    "Server successfully started\tOK\t\\N\t\n"
    "No lost s3 keys\tOK\t\\N\t\n"
    "No SharedMergeTree lost forever in clickhouse-server.log\tOK\t\\N\t\n"
)

CLEAN_TSV = (
    "Test script exit code\tOK\t\\N\t\n"
    "Server successfully started\tOK\t\\N\t\n"
    "No lost s3 keys\tOK\t\\N\t\n"
)

SERVER_DIED_TSV = (
    "Server died\tFAIL\t\\N\t\n" "Server successfully started\tOK\t\\N\t\n"
)


def _run_stress_job(
    monkeypatch,
    tmp_path,
    *,
    server_logs,
    results_tsv,
    exit_code=1,
    dmesg=None,
):
    """Drive `run_stress_test` with the docker run replaced by a real shell
    command that writes the artifacts it would have produced.

    `Shell.run` is deliberately NOT stubbed: `Shell.check` delegates to it, so
    faking it would stop the fatal-evidence `rg` from ever executing and every
    assertion below would pass vacuously.
    """
    temp_dir = tmp_path / "ci/tmp"
    temp_dir.mkdir(parents=True)
    server_log_path = temp_dir / "server_log"
    result_path = temp_dir / "result_path"

    monkeypatch.setattr(stress_job.Utils, "cwd", staticmethod(lambda: str(tmp_path)))
    monkeypatch.setattr(stress_job, "Info", lambda: type("Info", (), {})())
    monkeypatch.setattr(
        stress_job.DockerImage,
        "get_docker_image",
        staticmethod(
            lambda _name: type("Image", (), {"pull_image": lambda s: "img"})()
        ),
    )
    monkeypatch.setattr(
        stress_job.Utils,
        "fix_ownership_after_docker",
        staticmethod(lambda _path, _image: None),
    )
    monkeypatch.setattr(
        stress_job.ClickHouseService, "collect_cores", staticmethod(lambda _p: [])
    )
    monkeypatch.setattr(stress_job, "get_additional_envs", lambda _info, _check: [])

    writes = [
        f"printf %s {shlex.quote(content)} > {server_log_path}/{name}"
        for name, content in server_logs.items()
    ]
    writes.append(
        f"printf %s {shlex.quote(results_tsv)} > {result_path}/test_results.tsv"
    )
    if dmesg is not None:
        writes.append(f"printf %s {shlex.quote(dmesg)} > {result_path}/dmesg.log")
    command = " && ".join(writes) + f" && exit {exit_code}"
    monkeypatch.setattr(stress_job, "get_run_command", lambda *a, **k: command)

    monkeypatch.setenv("CHECK_NAME", "Stress test (amd_debug)")
    monkeypatch.setattr(Settings, "TEMP_DIR", str(temp_dir))
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit):
        stress_job.run_stress_test()

    dumped = list(temp_dir.glob("result_*.json"))
    assert len(dumped) == 1, dumped
    return Result.from_fs(dumped[0].stem.removeprefix("result_"))


def _leaf_names(result):
    return [leaf.name for leaf in result.results]


def _failed_names(result):
    return [leaf.name for leaf in result.results if not leaf.is_ok()]


def test_collected_fatal_is_parsed_into_a_named_failure(monkeypatch, tmp_path):
    """The witness. Pre-fix this job reported only `Test script failed` with
    ` script exit code: 1`; the abort in the server log was written to
    fatal.log and dropped. Asserting on the parsed NAME is what makes this
    arm non-vacuous - a bare "some leaf failed" holds pre-fix too, because
    `Test script failed` is itself a FAIL."""
    result = _run_stress_job(
        monkeypatch,
        tmp_path,
        server_logs={"clickhouse-server.err.log": FATAL_ABORT_LOG},
        results_tsv=BUCKET_B_TSV,
    )
    parsed = [n for n in _failed_names(result) if "Digest does not match" in n]
    assert parsed, f"no parsed abort among {_failed_names(result)}"
    leaf = next(leaf for leaf in result.results if leaf.name == parsed[0])
    assert "Logical error" in leaf.name
    # Shape, not the literal id: a None id interpolates as the text `STID: None`.
    assert re.search(r"\(STID: \d{4}-[0-9a-f]{4}\)", leaf.name), leaf.name
    assert "assertTypeEquality" in (leaf.info or ""), leaf.info
    # The pre-fix rows stay: the wrapper still failed, it is just attributed now.
    assert "Test script failed" in _leaf_names(result)


def test_clean_run_with_realistic_logs_stays_green(monkeypatch, tmp_path):
    """The arm that protects every passing stress job: a busy server log with
    no <Fatal> must not trigger the parser. Measured against a real green
    `arm_release` run, whose fatal.log was 0 bytes while the seven failing
    carriers were 41-117 KB."""
    result = _run_stress_job(
        monkeypatch,
        tmp_path,
        server_logs={"clickhouse-server.err.log": CLEAN_LOG},
        results_tsv=CLEAN_TSV,
        exit_code=0,
    )
    assert result.is_ok(), _failed_names(result)
    assert _failed_names(result) == []


def test_server_died_without_fatal_is_unchanged(monkeypatch, tmp_path):
    """`server_died` stays load-bearing: it is the only signal for a SILENT
    death (SIGKILL/OOM with nothing logged), which fatal evidence cannot
    see. Behaviour here must be exactly what it was before the fix."""
    result = _run_stress_job(
        monkeypatch,
        tmp_path,
        server_logs={"clickhouse-server.err.log": CLEAN_LOG},
        results_tsv=SERVER_DIED_TSV,
    )
    assert not result.is_ok()
    assert "Server died" not in _leaf_names(result)
    assert "Unknown error" in _failed_names(result)


def test_oom_override_still_wins_over_a_collected_fatal(monkeypatch, tmp_path):
    """Documents, rather than changes, the pre-existing `is_oom` override at
    the end of `run_stress_test`: an OOM is deliberately allowed under stress,
    so it flips the whole job back to OK even when an abort was parsed.
    Measured `is_oom` inputs were 0 in all seven observed carriers, so this
    does not mask them today. Changing it is a separate concern."""
    result = _run_stress_job(
        monkeypatch,
        tmp_path,
        server_logs={"clickhouse-server.err.log": FATAL_ABORT_LOG},
        results_tsv=BUCKET_B_TSV,
        dmesg="Out of memory: Killed process 1234 (clickhouse-serv)\n",
    )
    assert result.is_ok()
    assert result.info and "OOM error (allowed in stress tests)" in result.info
    # The parsed abort is still recorded as a leaf, just not blocking.
    assert any("Digest does not match" in n for n in _leaf_names(result))


def test_fatal_only_in_a_replica_log_is_parsed(monkeypatch, tmp_path):
    """Multi-replica runs (DatabaseReplicated) put each server's log under its
    own `sc1`/`sc2` name; the crashed replica is the one to parse."""
    result = _run_stress_job(
        monkeypatch,
        tmp_path,
        server_logs={
            "clickhouse-server.err.log": CLEAN_LOG,
            "clickhouse-server-sc1.err.log": FATAL_ABORT_LOG,
        },
        results_tsv=BUCKET_B_TSV,
    )
    assert any("Digest does not match" in n for n in _failed_names(result))


def test_benign_fatal_lines_do_not_fabricate_a_failure_name(monkeypatch, tmp_path):
    """The signal handler logs an integrity-check line, a version banner and a
    `Changed settings:` dump at <Fatal> level, so `<Fatal>` alone does not
    prove an abort. All of them come from `SignalListener::onFault`, so a real
    log never carries them without a crash - but if one ever did, the row must
    not be named after a version banner. The parser finds no error pattern and
    reports its generic `Unknown error` instead."""
    result = _run_stress_job(
        monkeypatch,
        tmp_path,
        server_logs={"clickhouse-server.err.log": BENIGN_FATAL_ONLY_LOG},
        results_tsv=BUCKET_B_TSV,
    )
    failed = _failed_names(result)
    assert "Unknown error" in failed, failed
    assert not any("is old and should be upgraded" in n for n in failed), failed
    assert not any("Integrity check" in n for n in failed), failed


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
