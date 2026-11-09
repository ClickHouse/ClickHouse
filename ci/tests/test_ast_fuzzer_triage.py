"""
Tests for the report inventory of `triage_failure` (ci/jobs/ast_fuzzer_job.py).

The server and the client of the AST fuzzer write their sanitizer reports to one
`stderr.log`, and the log parser names the first report only. The AI triage may
pass a run on a known issue of the named report, so a second, unnamed report must
reach it as a failure no issue covers.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import types

import ci.jobs.ast_fuzzer_job as ast_job
import ci.jobs.scripts.ai_fuzz_triage as triage
import ci.praktika.result as praktika_result
from ci.jobs.scripts.ai_fuzz_triage import (
    Occurrences,
    group_failures,
    known_issue_verdict,
)

_BANNER = (
    "2026.11.09 10:00:00.000000 [ 1 ] {} <Fatal> BaseDaemon: "
    "########################################\n"
)


def _asan(func, pid):
    return (
        f"=== sanitizer report from /workspace/sanitizer.log.{pid} ===\n"
        f"=={pid}==ERROR: AddressSanitizer: heap-use-after-free on address 0x1\n"
        f"    #0 0x1 in DB::{func}() src/{func}.cpp:10:5\n"
        f"    #1 0x2 in DB::{func}Caller() src/{func}.cpp:20:5\n"
        f"SUMMARY: AddressSanitizer: heap-use-after-free src/{func}.cpp:10:5\n\n"
    )


def _logical_error(func):
    # A logical exception aborts through the signal handler, which logs the crash header.
    return (
        "2026.11.09 10:00:00.000000 [ 1068 ] {q} <Fatal> : Logical error: "
        f"'Bad cast in {func}'.\n"
        "2026.11.09 10:00:00.000001 [ 1068 ] {q} <Fatal> : Stack trace (when copying this "
        "message, always include the lines below):\n"
        f"0. src/{func}.cpp:10: DB::{func}() @ 0x0000000000000001\n"
        f"1. src/{func}.cpp:20: DB::{func}Caller() @ 0x0000000000000002\n\n" + _BANNER + (
        "2026.11.09 10:00:00.000001 [ 1 ] {} <Fatal> BaseDaemon: (version 26.11.1.1) (from thread 1068) "
        "(query_id: q) (query: SELECT 1) Received signal Aborted (6)\n"
    )
    )


def _triaged_failures(tmp_path, monkeypatch, stderr_text, server_text=""):
    # The real analysis of a dead server, then the failures `triage_failure` hands over.
    # `run-fuzzer.sh` appends the sanitizer reports to `server.log` too.
    artifacts = tuple(
        tmp_path / n
        for n in ("server.log", "fuzzer.log", "stderr.log", "dmesg.log", "fatal.log")
    )
    server_log, fuzzer_log, stderr_log, _, _ = artifacts
    server_log.write_text(server_text + stderr_text, encoding="utf-8")
    stderr_log.write_text(stderr_text, encoding="utf-8")
    fuzzer_log.write_text("Fuzzing step 1 out of 1\nSELECT 1\n", encoding="utf-8")
    monkeypatch.setattr(ast_job, "JOB_ARTIFACTS", artifacts)
    # `Result.create_from` names the result after the job, which only a CI run has.
    monkeypatch.setattr(
        praktika_result._Environment,
        "get",
        staticmethod(lambda: types.SimpleNamespace(JOB_NAME="AST fuzzer (amd_debug)")),
    )
    result = ast_job.analyze_job_logs(
        list(artifacts), True, 134, 0, True, fuzzer_log
    )
    assert result.is_failure()

    handed = []
    monkeypatch.setattr(
        triage, "triage_and_apply", lambda r, **kw: handed.extend(kw["failures"])
    )
    ast_job.triage_failure(
        result, "AST Fuzzer", list(artifacts), "", report_logs=([server_log], stderr_log)
    )
    return handed


def _known_issue_passes(failures):
    # The fingerprint's issue is open and saw this stack before the PR.
    groups = group_failures(failures)
    occurrences = Occurrences([], [], ["https://example/issue"], [], {}, "master", False)
    return bool(known_issue_verdict(groups, occurrences, True))


def test_second_report_blocks_known_issue_pass(tmp_path, monkeypatch):
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11) + _asan("ServerNew", 22)
    )

    assert [n for n, _ in failures][-1] == "Incomplete failure inventory"
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)


def test_single_report_still_passes_on_known_issue(tmp_path, monkeypatch):
    failures = _triaged_failures(tmp_path, monkeypatch, _asan("ClientKnown", 11))

    assert "Incomplete failure inventory" not in [n for n, _ in failures]
    assert _known_issue_passes(failures)


def test_logical_error_next_to_known_sanitizer_report_blocks_pass(tmp_path, monkeypatch):
    # A sanitizer abort logs no crash header, so the header is an independent server failure
    # that the parser does not name next to the ASan report.
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11), _logical_error("ServerNew")
    )

    assert failures[0][0].startswith("AddressSanitizer")
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)


_ORACLE = (
    "2026.11.09 10:00:00.000000 [ 2000 ] {q2} <Fatal> executeQuery: AST Fuzzer oracle "
    "mismatch detected!\n"
    "Fuzzed query: SELECT g FROM t GROUP BY g\n"
    "TLP Aggregate oracle mismatch!\n"
)


def test_oracle_mismatch_next_to_known_sanitizer_report_blocks_pass(tmp_path, monkeypatch):
    # An oracle mismatch throws without aborting, so it logs a `<Fatal>` record but no crash
    # report, and the parser names the ASan report.
    failures = _triaged_failures(
        tmp_path, monkeypatch, _asan("ClientKnown", 11), _ORACLE
    )

    assert failures[0][0].startswith("AddressSanitizer")
    assert "logged 2 reports, 1 named" in failures[-1][1]
    assert not _known_issue_passes(failures)
