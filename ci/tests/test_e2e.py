"""
End-to-end tests for the CI report pipeline.

Seeds a synthetic workspace and runs jobs to verify that the artifact
collection and encryption pipeline produces `.zst.enc` and `.rsa` files in
the result JSON.
"""

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

import tempfile

import pytest

from ci.jobs.ast_fuzzer_job import (
    JOB_ARTIFACTS,
    WORKSPACE_PATH,
    _format_status_error,
    _read_fuzzer_status,
)
from ci.praktika.utils import Utils

FUZZER_JOB = "AST fuzzer (amd_debug)"
STRESS_JOB = "Stress test (amd_debug)"


def test_read_fuzzer_status_valid():
    with tempfile.TemporaryDirectory() as d:
        status = Path(d) / "status.tsv"
        status.write_text("1\t0\t137\n")
        assert _read_fuzzer_status(status) == (True, 0, 137)


def test_read_fuzzer_status_missing_or_empty():
    # The runner aborts (timeout / OOM / docker failure) before writing
    # status.tsv. A missing or empty file must be reported as an early abort,
    # not as an opaque parse crash.
    with tempfile.TemporaryDirectory() as d:
        with pytest.raises(FileNotFoundError):
            _read_fuzzer_status(Path(d) / "status.tsv")
        empty = Path(d) / "status.tsv"
        empty.write_text("")
        with pytest.raises(FileNotFoundError):
            _read_fuzzer_status(empty)


def test_read_fuzzer_status_malformed():
    with tempfile.TemporaryDirectory() as d:
        bad = Path(d) / "status.tsv"
        bad.write_text("garbage line\n")
        with pytest.raises(ValueError):
            _read_fuzzer_status(bad)


def test_format_status_error_missing_is_neutral_with_log_tail():
    # Missing status.tsv -> neutral early-abort message, no traceback, log tail.
    # It must NOT assert infra / "re-run clears it": a missing file can also be
    # a server-startup or harness regression, so we point at the logs instead.
    with tempfile.TemporaryDirectory() as d:
        fuzzer_log = Path(d) / "fuzzer.log"
        fuzzer_log.write_text("\n".join(f"line{i}" for i in range(200)))
        msg = _format_status_error(
            FileNotFoundError("status.tsv was not produced"),
            [fuzzer_log, Path(d) / "absent.log"],
        )
        assert "aborted before writing status.tsv" in msg
        assert "re-run" not in msg.lower()
        assert "Traceback" not in msg
        assert "fuzzer.log (last lines)" in msg
        assert "line199" in msg  # tail, not head
        assert "line0\n" not in msg  # head bounded out


def test_format_status_error_malformed_keeps_traceback():
    # Malformed status.tsv -> harness bug; keep the traceback for debugging.
    try:
        raise ValueError("expected 3 tab-separated fields")
    except ValueError as e:
        msg = _format_status_error(e, [])
    assert "harness bug" in msg
    assert "Traceback" in msg


def test_fuzzer():
    ci_tmp = Path(WORKSPACE_PATH.parent)
    ci_backup = Path(ci_tmp.parent / "tmp_backup")
    ci_result = Path(ci_tmp.parent / "tmp_result")
    shutil.rmtree(ci_backup, ignore_errors=True)
    shutil.rmtree(ci_result, ignore_errors=True)
    if ci_tmp.exists():
        ci_tmp.rename(ci_backup)

    try:
        WORKSPACE_PATH.mkdir(parents=True, exist_ok=True)
        for p in JOB_ARTIFACTS:
            p.write_text("test\n")
        (WORKSPACE_PATH / "status.tsv").write_text("1\t0\t0\n")
        (WORKSPACE_PATH / "core.test").write_bytes(b"test\n")
        (ci_tmp / "clickhouse").write_text("""
    #!/bin/bash

    kill -SIGTERM 1
    """)

        subprocess.run(
            [sys.executable, "-m", "ci.praktika", "run", FUZZER_JOB],
        )

        result_file = Path(f"ci/tmp/result_{Utils.normalize_string(FUZZER_JOB)}.json")
        assert result_file.exists(), f"result JSON not found: {result_file}"
        report = json.loads(result_file.read_text())

        files = report.get("files", [])
        assert any(f.endswith(".zst.enc") for f in files), (
            f"no encrypted core (.zst.enc) in report files: {files}"
        )
        assert any(f.endswith(".rsa") for f in files), (
            f"no RSA-wrapped AES key (.rsa) in report files: {files}"
        )
        assert not any(Path(f).name == "aes.key" for f in files), (
            f"raw AES key must not appear in report files: {files}"
        )
    finally:
        if ci_tmp.exists():
            ci_tmp.rename(ci_result)
        if ci_backup.exists():
            ci_backup.rename(ci_tmp)


def test_stress():
    cwd = Utils.cwd()
    temp_path = Path(cwd) / "ci/tmp"
    ci_backup = Path(temp_path.parent / "tmp_backup")
    ci_result = Path(temp_path.parent / "tmp_result")
    shutil.rmtree(ci_backup, ignore_errors=True)
    shutil.rmtree(ci_result, ignore_errors=True)
    if temp_path.exists():
        temp_path.rename(ci_backup)

    try:
        cores_path = temp_path / "cores"
        result_path = temp_path / "result_path"
        server_log_path = temp_path / "server_log"

        cores_path.mkdir(parents=True, exist_ok=True)
        result_path.mkdir(parents=True, exist_ok=True)
        server_log_path.mkdir(parents=True, exist_ok=True)

        (cores_path / "core.test").write_bytes(b"fake core\n")
        (result_path / "test_results.tsv").write_text("test\tOK\t1.0\t\n")

        subprocess.run(
            [sys.executable, "-m", "ci.praktika", "run", STRESS_JOB],
        )

        result_file = Path(f"ci/tmp/result_{Utils.normalize_string(STRESS_JOB)}.json")
        assert result_file.exists(), f"result JSON not found: {result_file}"
        report = json.loads(result_file.read_text())

        files = report.get("files", [])
        assert any(f.endswith(".zst.enc") for f in files), (
            f"no encrypted core (.zst.enc) in report files: {files}"
        )
        assert any(f.endswith(".rsa") for f in files), (
            f"no RSA-wrapped AES key (.rsa) in report files: {files}"
        )
        assert not any(Path(f).name == "aes.key" for f in files), (
            f"raw AES key must not appear in report files: {files}"
        )
    finally:
        if temp_path.exists():
            temp_path.rename(ci_result)
        if ci_backup.exists():
            ci_backup.rename(temp_path)


if __name__ == "__main__":
    import pytest as _pytest

    sys.exit(_pytest.main([__file__, "-v"]))
