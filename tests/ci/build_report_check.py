#!/usr/bin/env python3

import logging
import os
import sys
from pathlib import Path
from typing import List

from ci_config import CI_CONFIG, Build
from env_helper import (
    GITHUB_JOB_URL,
    GITHUB_REPOSITORY,
    GITHUB_SERVER_URL,
    REPORT_PATH,
    TEMP_PATH,
)
from pr_info import PRInfo
from report import (
    ERROR,
    PENDING,
    SUCCESS,
    BuildResult,
    JobReport,
    create_build_html_report,
    get_worst_status,
)
from stopwatch import Stopwatch

# Old way to read the neads_data
NEEDS_DATA_PATH = os.getenv("NEEDS_DATA_PATH", "")
# Now it's set here. Two-steps migration for backward compatibility
NEEDS_DATA = os.getenv("NEEDS_DATA", "")


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()
    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Reports found:\n %s",
        "\n ".join(p.as_posix() for p in reports_path.rglob("*.json")),
    )

    build_check_name = sys.argv[1]

    pr_info = PRInfo()

    builds_for_check = CI_CONFIG.get_builds_for_report(
        build_check_name,
        release=pr_info.is_release,
        backport=pr_info.head_ref.startswith("backport/"),
    )
    required_builds = len(builds_for_check)
    missing_builds = 0

    # Collect reports from json artifacts
    build_results = []  # type: List[BuildResult]
    for build_name in builds_for_check:
        build_result = BuildResult.load_any(
            build_name, pr_info.number, pr_info.head_ref
        )
        if not build_result:
            if build_name == Build.FUZZERS:
                logging.info("Build [%s] is missing - skip", Build.FUZZERS)
                continue
            logging.warning("Build results for %s is missing", build_name)
            build_result = BuildResult.missing_result("missing")
            build_result.job_name = build_name
            build_result.status = PENDING
            logging.info(
                "There is missing report for %s, created a dummy result %s",
                build_name,
                build_result,
            )
            missing_builds += 1
            build_results.insert(0, build_result)
        else:
            assert (
                pr_info.head_ref == build_result.head_ref or pr_info.number > 0
            ), "BUG. if not a PR, report must be created on the same branch"
            build_results.append(build_result)

    # Calculate artifact groups like packages and binaries
    total_groups = sum(len(br.grouped_urls) for br in build_results)
    ok_groups = sum(
        len(br.grouped_urls) for br in build_results if br.status == SUCCESS
    )
    logging.info("Totally got %s artifact groups", total_groups)
    if total_groups == 0:
        logging.error("No success builds, failing check without creating a status")
        sys.exit(1)

    branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commits/master"
    branch_name = "master"
    if pr_info.number != 0:
        branch_name = f"PR #{pr_info.number}"
        branch_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/pull/{pr_info.number}"
    commit_url = f"{GITHUB_SERVER_URL}/{GITHUB_REPOSITORY}/commit/{pr_info.sha}"
    task_url = GITHUB_JOB_URL()
    report = create_build_html_report(
        build_check_name,
        build_results,
        task_url,
        branch_url,
        branch_name,
        commit_url,
    )

    report_path = temp_path / "report.html"
    report_path.write_text(report, encoding="utf-8")

    # Prepare a commit status
    summary_status = get_worst_status(br.status for br in build_results)

    # Check if there are no builds at all, do not override bad status
    if summary_status == SUCCESS:
        if missing_builds:
            summary_status = PENDING
        elif ok_groups == 0:
            summary_status = ERROR

    addition = ""
    if missing_builds:
        addition = (
            f" ({required_builds - missing_builds} of {required_builds} builds are OK)"
        )

    description = f"{ok_groups}/{total_groups} artifact groups are OK{addition}"

    JobReport(
        description=description,
        test_results=[],
        status=summary_status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[report_path],
    ).dump()

    # We should fail the report job to rerun it in the following attempts
    if summary_status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
