#!/usr/bin/env python3

import csv
import logging
import subprocess
import os
import sys
from pathlib import Path
from typing import List, Tuple

from github import Github

from build_download_helper import download_all_deb_packages
from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import RerunHelper, get_commit, post_commit_status
from docker_pull_helper import get_image_with_version
from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults, read_test_results
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results


def get_run_command(
    build_path, result_folder, repo_tests_path, server_log_folder, image
):
    cmd = (
        "docker run --cap-add=SYS_PTRACE "
        # a static link, don't use S3_URL or S3_DOWNLOAD
        "-e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' "
        # For dmesg and sysctl
        "--privileged "
        f"--volume={build_path}:/package_folder "
        f"--volume={result_folder}:/test_output "
        f"--volume={repo_tests_path}:/usr/share/clickhouse-test "
        f"--volume={server_log_folder}:/var/log/clickhouse-server {image} "
    )

    return cmd


def process_results(
    result_folder: str, server_log_path: str, run_log_path: str
) -> Tuple[str, str, TestResults, List[str]]:
    test_results = []  # type: TestResults
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content
    # of result_folder.
    if os.path.exists(result_folder):
        test_files = [
            f
            for f in os.listdir(result_folder)
            if os.path.isfile(os.path.join(result_folder, f))
        ]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    if os.path.exists(server_log_path):
        server_log_files = [
            f
            for f in os.listdir(server_log_path)
            if os.path.isfile(os.path.join(server_log_path, f))
        ]
        additional_files = additional_files + [
            os.path.join(server_log_path, f) for f in server_log_files
        ]

    additional_files.append(run_log_path)

    status_path = os.path.join(result_folder, "check_status.tsv")
    if not os.path.exists(status_path):
        return (
            "failure",
            "check_status.tsv doesn't exists",
            test_results,
            additional_files,
        )

    logging.info("Found check_status.tsv")
    with open(status_path, "r", encoding="utf-8") as status_file:
        status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = Path(result_folder) / "test_results.tsv"
        test_results = read_test_results(results_path, True)
        if len(test_results) == 0:
            raise Exception("Empty results")
    except Exception as e:
        return (
            "error",
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files


def run_stress_test(docker_image_name):
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()
    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    repo_tests_path = os.path.join(repo_path, "tests")
    reports_path = REPORTS_PATH

    check_name = sys.argv[1]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    pr_info = PRInfo()

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = get_image_with_version(reports_path, docker_image_name)

    packages_path = os.path.join(temp_path, "packages")
    if not os.path.exists(packages_path):
        os.makedirs(packages_path)

    download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = os.path.join(temp_path, "server_log")
    if not os.path.exists(server_log_path):
        os.makedirs(server_log_path)

    result_path = os.path.join(temp_path, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_log_path = os.path.join(temp_path, "run.log")

    run_command = get_run_command(
        packages_path, result_path, repo_tests_path, server_log_path, docker_image
    )
    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path, timeout=60 * 150) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    s3_helper = S3Helper()
    state, description, test_results, additional_logs = process_results(
        result_path, server_log_path, run_log_path
    )
    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        additional_logs,
        check_name,
    )
    print(f"::notice ::Report url: {report_url}")

    post_commit_status(commit, state, report_url, description, check_name, pr_info)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name,
    )
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state == "failure":
        sys.exit(1)


if __name__ == "__main__":
    run_stress_test("clickhouse/stress-test")
