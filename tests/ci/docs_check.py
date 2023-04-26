#!/usr/bin/env python3
import argparse
import logging
import subprocess
import os
import sys
from github import Github

from env_helper import TEMP_PATH, REPO_COPY
from s3_helper import S3Helper
from pr_info import PRInfo
from get_robot_token import get_best_robot_token
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from commit_status_helper import post_commit_status, get_commit
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from stopwatch import Stopwatch
from rerun_helper import RerunHelper
from tee_popen import TeePopen


NAME = "Docs Check"

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to check the docs integrity",
    )
    parser.add_argument(
        "--docs-branch",
        default="",
        help="a branch to get from docs repository",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="check the docs even if there no changes",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY

    pr_info = PRInfo(need_changed_files=True)

    gh = Github(get_best_robot_token(), per_page=100)

    rerun_helper = RerunHelper(gh, pr_info, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    if not pr_info.has_changes_in_documentation() and not args.force:
        logging.info("No changes in documentation")
        commit = get_commit(gh, pr_info.sha)
        commit.create_status(
            context=NAME, description="No changes in docs", state="success"
        )
        sys.exit(0)

    if pr_info.has_changes_in_documentation():
        logging.info("Has changes in docs")
    elif args.force:
        logging.info("Check the docs because of force flag")

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    docker_image = get_image_with_version(temp_path, "clickhouse/docs-builder")

    test_output = os.path.join(temp_path, "docs_check_log")
    if not os.path.exists(test_output):
        os.makedirs(test_output)

    cmd = (
        f"docker run --cap-add=SYS_PTRACE -e GIT_DOCS_BRANCH={args.docs_branch} "
        f"--volume={repo_path}:/ClickHouse --volume={test_output}:/output_path "
        f"{docker_image}"
    )

    run_log_path = os.path.join(test_output, "runlog.log")
    logging.info("Running command: '%s'", cmd)

    with TeePopen(cmd, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            status = "success"
            description = "Docs check passed"
        else:
            description = "Docs check failed (non zero exit code)"
            status = "failure"
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    files = os.listdir(test_output)
    lines = []
    additional_files = []
    if not files:
        logging.error("No output files after docs check")
        description = "No output files after docs check"
        status = "failure"
    else:
        for f in files:
            path = os.path.join(test_output, f)
            additional_files.append(path)
            with open(path, "r", encoding="utf-8") as check_file:
                for line in check_file:
                    if "ERROR" in line:
                        lines.append((line.split(":")[-1], "FAIL"))
        if lines:
            status = "failure"
            description = "Found errors in docs"
        elif status != "failure":
            lines.append(("No errors found", "OK"))
        else:
            lines.append(("Non zero exit code", "FAIL"))

    s3_helper = S3Helper()
    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, lines, additional_files, NAME
    )
    print("::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, NAME, description, status, report_url)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        lines,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        NAME,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)
    if status == "error":
        sys.exit(1)
