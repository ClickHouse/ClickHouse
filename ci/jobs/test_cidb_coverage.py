"""
Smoke-test job: inserts a single synthetic row into coverage_ci.coverage_data.

This job has no artifact dependencies and runs at the very beginning of the
workflow, so any CIDB connectivity / schema / import problems are caught early
instead of only after the multi-hour LLVM Coverage pipeline finishes.
"""

import json
from datetime import datetime, timezone

from ci.praktika._environment import _Environment
from ci.praktika.mangle import _get_workflows
from ci.praktika.result import Result
import ci.praktika.cidb as CIDB
from ci.praktika.settings import Settings

WORKFLOW = _get_workflows(name=_Environment.get().WORKFLOW_NAME)[0]


def insert_hello_world_row() -> None:
    cidb = CIDB.CIDB(
        WORKFLOW.get_secret(Settings.SECRET_CI_DB_URL).get_value(),
        WORKFLOW.get_secret(Settings.SECRET_CI_DB_USER).get_value(),
        WORKFLOW.get_secret(Settings.SECRET_CI_DB_PASSWORD).get_value(),
    )
    is_ok, error = cidb.check()
    if not is_ok:
        raise RuntimeError(f"CI DB connection check failed: {error}")

    row = json.dumps(
        {
            "check_start_time":          datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
            "pull_request_number":       0,
            "commit_sha":                "hello_world_test",
            "base_commit_sha":           "hello_world_test",
            "branch":                    "test",
            "base_branch":               "test",
            "status":                    "hello_world",
            "baseline_line_cov":         0.0,
            "baseline_func_cov":         0.0,
            "baseline_branch_cov":       0.0,
            "current_line_cov":          0.0,
            "current_func_cov":          0.0,
            "current_branch_cov":        0.0,
            "delta_line_cov":            0.0,
            "coverage_report_url":       "",
            "diff_coverage_report_url":  "",
            "uncovered_code_url":        "",
        }
    )

    _orig_db, _orig_table = Settings.CI_DB_DB_NAME, Settings.CI_DB_TABLE_NAME
    try:
        Settings.CI_DB_DB_NAME = "coverage_ci"
        Settings.CI_DB_TABLE_NAME = "coverage_data"
        cidb.insert_rows([row])
    finally:
        Settings.CI_DB_DB_NAME, Settings.CI_DB_TABLE_NAME = _orig_db, _orig_table

    print("Hello world row inserted into coverage_ci.coverage_data successfully.")


if __name__ == "__main__":
    Result.from_commands_run(
        name="Test CIDB Coverage Connection",
        command=[insert_hello_world_row],
        with_info_on_failure=True,
    ).complete_job()
