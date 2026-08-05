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

from ci.jobs.ast_fuzzer_job import JOB_ARTIFACTS, WORKSPACE_PATH
from ci.praktika.utils import Utils

FUZZER_JOB = "AST fuzzer (amd_debug)"
STRESS_JOB = "Stress test (amd_debug)"


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
