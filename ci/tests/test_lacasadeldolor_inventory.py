"""
Tests for the failure inventory of La Casa del Dolor (`_all_failures` in
ci/jobs/lacasadeldolor_job.py): the AI triage may pass a run on a known issue of the
reports the log parser names, so a report it does not name must keep the run red.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import ci.jobs.lacasadeldolor_job as dolor
from ci.jobs.scripts.ai_fuzz_triage import (
    Occurrences,
    group_failures,
    known_issue_verdict,
)

_READY = "2026.11.09 10:00:00.000000 [ 1 ] {} <Information> Application: Ready\n"
_DEADLY_SIGNAL = "AddressSanitizer:DEADLYSIGNAL\n"


def _asan(func, pid, kind="heap-use-after-free on address 0x1"):
    return (
        f"=={pid}==ERROR: AddressSanitizer: {kind}\n"
        f"    #0 0x1 in DB::{func}() src/{func}.cpp:10:5\n"
        f"    #1 0x2 in DB::{func}Caller() src/{func}.cpp:20:5\n"
        f"SUMMARY: AddressSanitizer: {kind.split()[0]} src/{func}.cpp:10:5\n"
    )


def _keeper_run_failures(tmp_path, keeper_stderr):
    # A healthy server, and Keeper 1 with `keeper_stderr`.
    for name in ("server.log", "server.err.log", "keeper1.err.log"):
        (tmp_path / name).write_text(_READY, encoding="utf-8")
    (tmp_path / "keeper_stderr1.log").write_text(keeper_stderr, encoding="utf-8")
    (tmp_path / "fuzzerout.sql").write_text("SELECT 1;\n", encoding="utf-8")
    failures = dolor._all_failures(
        [], None, tmp_path, tmp_path / "stderr.log", tmp_path / "fuzzerout.sql"
    )
    return [(r.name, r.info) for r in failures]


def _known_issue_passes(failures):
    # The fingerprint's issue is open and saw this stack before the PR.
    groups = group_failures(failures)
    occurrences = Occurrences([], [], ["https://example/issue"], [], {}, "master", False)
    return bool(known_issue_verdict(groups, occurrences, True))


def test_interrupted_report_next_to_known_keeper_report_blocks_pass(tmp_path):
    failures = _keeper_run_failures(tmp_path, _asan("KeeperKnown", 11) + _DEADLY_SIGNAL)

    assert failures[0][0].startswith("AddressSanitizer")
    assert "keeper1 logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)


def test_complete_deadly_signal_keeper_report_is_counted_once(tmp_path):
    report = _DEADLY_SIGNAL + "=" * 65 + "\n" + _asan("KeeperKnown", 11, "SEGV on unknown address 0x0")
    failures = _keeper_run_failures(tmp_path, report)

    assert [n for n, _ in failures] == [failures[0][0]]
    assert _known_issue_passes(failures)


def test_survived_logical_error_next_to_known_server_report_blocks_pass(tmp_path):
    # `dolor.py` fails the run on a logical exception at `<Error>`, but the parser names the ASan
    # report, so the inventory must count the exception for no issue of that report to pass it.
    logical_error = (
        "2026.11.09 10:00:01.000000 [ 2001 ] {q7} <Error> executeQuery: Code: 190. DB::Exception: "
        "Logical error: Sizes of nested columns are inconsistent: 3 and 4. "
        "(SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT) (version 26.11.1.1) (in query: SELECT 1)\n"
    )
    for name in ("server.log", "server.err.log"):
        (tmp_path / name).write_text(_READY + logical_error, encoding="utf-8")
    (tmp_path / "stderr.log").write_text(_asan("ServerKnown", 12), encoding="utf-8")
    (tmp_path / "fuzzerout.sql").write_text("SELECT 1;\n", encoding="utf-8")
    named = dolor._parse_logs(tmp_path / "server.log", tmp_path / "stderr.log", tmp_path / "fuzzerout.sql")
    failures = dolor._all_failures(
        [named], None, tmp_path, tmp_path / "stderr.log", tmp_path / "fuzzerout.sql"
    )
    failures = [(r.name, r.info) for r in failures]

    assert failures[0][0].startswith("AddressSanitizer")
    assert "the server logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)
