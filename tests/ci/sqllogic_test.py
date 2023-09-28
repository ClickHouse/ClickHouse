#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path

from github import Github

from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import FORCE_TESTS_LABEL, PRInfo
from build_download_helper import download_all_deb_packages
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from commit_status_helper import (
    RerunHelper,
    get_commit,
    override_status,
    post_commit_status,
)
from report import TestResults, read_test_results

from stopwatch import Stopwatch
from tee_popen import TeePopen


NO_CHANGES_MSG = "Nothing to run"
IMAGE_NAME = "clickhouse/sqllogic-test"


def get_run_command(
    builds_path,
    repo_tests_path,
    result_path,
    server_log_path,
    kill_timeout,
    additional_envs,
    image,
):
    envs = [
        f"-e MAX_RUN_TIME={int(0.9 * kill_timeout)}",
    ]
    envs += [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)

    return (
        f"docker run "
        f"--volume={builds_path}:/package_folder "
        f"--volume={repo_tests_path}:/clickhouse-tests "
        f"--volume={result_path}:/test_output "
        f"--volume={server_log_path}:/var/log/clickhouse-server "
        f"--cap-add=SYS_PTRACE {env_str} {image}"
    )


def __files_in_dir(dir_path):
    return [
        os.path.join(dir_path, f)
        for f in os.listdir(dir_path)
        if os.path.isfile(os.path.join(dir_path, f))
    ]


def read_check_status(result_folder):
    status_path = os.path.join(result_folder, "check_status.tsv")
    if not os.path.exists(status_path):
        return "error", "Not found check_status.tsv"

    logging.info("Found check_status.tsv")
    with open(status_path, "r", encoding="utf-8") as status_file:
        status_rows = list(csv.reader(status_file, delimiter="\t"))

    for row in status_rows:
        if len(row) != 2:
            return "error", "Invalid check_status.tsv"
        if row[0] != "success":
            return row[0], row[1]

    return status_rows[-1][0], status_rows[-1][1]


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    parser.add_argument("kill_timeout", type=int)
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    reports_path = REPORTS_PATH

    args = parse_args()
    check_name = args.check_name
    kill_timeout = args.kill_timeout

    pr_info = PRInfo()
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    repo_tests_path = os.path.join(repo_path, "tests")

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

    run_log_path = os.path.join(result_path, "runlog.log")

    additional_envs = []  # type: ignore

    run_command = get_run_command(  # run script inside docker
        packages_path,
        repo_tests_path,
        result_path,
        server_log_path,
        kill_timeout,
        additional_envs,
        docker_image,
    )
    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    logging.info("Files in result folder %s", os.listdir(result_path))

    s3_helper = S3Helper()

    status = None
    description = None

    additional_logs = []
    if os.path.exists(result_path):
        additional_logs.extend(__files_in_dir(result_path))

    if os.path.exists(server_log_path):
        additional_logs.extend(__files_in_dir(server_log_path))

    status, description = read_check_status(result_path)

    test_results = []  # type: TestResults
    test_results_path = Path(result_path) / "test_results.tsv"
    if test_results_path.exists():
        logging.info("Found test_results.tsv")
        test_results = read_test_results(test_results_path)

    if len(test_results) > 1000:
        test_results = test_results[:1000]

    if len(test_results) == 0:
        status, description = "error", "Empty test_results.tsv"

    assert status is not None
    status = override_status(status, check_name)

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [run_log_path] + additional_logs,
        check_name,
    )

    print(
        f"::notice:: {check_name}"
        f", Result: '{status}'"
        f", Description: '{description}'"
        f", Report url: '{report_url}'"
    )

    # Until it pass all tests, do not block CI, report "success"
    assert description is not None
    post_commit_status(commit, "success", report_url, description, check_name, pr_info)

    if status != "success":
        if FORCE_TESTS_LABEL in pr_info.labels:
            print(f"'{FORCE_TESTS_LABEL}' enabled, will report success")
        else:
            sys.exit(1)
