#!/usr/bin/env python3
import argparse
import atexit
import logging
import subprocess
import sys
from pathlib import Path

from github import Github

from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import (
    NotSet,
    RerunHelper,
    get_commit,
    post_commit_status,
    update_mergeable_check,
)
from docker_images_helper import get_docker_image, pull_image
from env_helper import TEMP_PATH, REPO_COPY
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults, TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results


NAME = "Docs Check"


def parse_args() -> argparse.Namespace:
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
    parser.add_argument(
        "--tag",
        required=False,
        default="",
        help="tag for docker image",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    pr_info = PRInfo(need_changed_files=True)

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)
    atexit.register(update_mergeable_check, gh, pr_info, NAME)

    if not pr_info.has_changes_in_documentation() and not args.force:
        logging.info("No changes in documentation")
        post_commit_status(
            commit, "success", NotSet, "No changes in docs", NAME, pr_info
        )
        sys.exit(0)

    if pr_info.has_changes_in_documentation():
        logging.info("Has changes in docs")
    elif args.force:
        logging.info("Check the docs because of force flag")

    docker_image = pull_image(get_docker_image("clickhouse/docs-builder"))

    test_output = temp_path / "docs_check_log"
    test_output.mkdir(parents=True, exist_ok=True)

    cmd = (
        f"docker run --cap-add=SYS_PTRACE -e GIT_DOCS_BRANCH={args.docs_branch} "
        f"--volume={repo_path}:/ClickHouse --volume={test_output}:/output_path "
        f"{docker_image}"
    )

    run_log_path = test_output / "run.log"
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
    test_results = []  # type: TestResults
    additional_files = []
    if not any(test_output.iterdir()):
        logging.error("No output files after docs check")
        description = "No output files after docs check"
        status = "failure"
    else:
        for p in test_output.iterdir():
            additional_files.append(p)
            with open(p, "r", encoding="utf-8") as check_file:
                for line in check_file:
                    if "ERROR" in line:
                        test_results.append(TestResult(line.split(":")[-1], "FAIL"))
        if test_results:
            status = "failure"
            description = "Found errors in docs"
        elif status != "failure":
            test_results.append(TestResult("No errors found", "OK"))
        else:
            test_results.append(TestResult("Non zero exit code", "FAIL"))

    s3_helper = S3Helper()
    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, test_results, additional_files, NAME
    )
    print("::notice ::Report url: {report_url}")
    post_commit_status(commit, status, report_url, description, NAME, pr_info)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        NAME,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)
    if status == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
