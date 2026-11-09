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


def _triaged_failures(tmp_path, monkeypatch, stderr_text):
    # The real analysis of a dead server, then the failures `triage_failure` hands over.
    artifacts = tuple(
        tmp_path / n
        for n in ("server.log", "fuzzer.log", "stderr.log", "dmesg.log", "fatal.log")
    )
    server_log, fuzzer_log, stderr_log, _, _ = artifacts
    server_log.write_text(_BANNER + stderr_text, encoding="utf-8")
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
