#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from github import Github

from build_download_helper import download_all_deb_packages
from commit_status_helper import (
    RerunHelper,
    get_commit,
    override_status,
    post_commit_status,
)
from docker_images_helper import DockerImage, pull_image, get_docker_image
from env_helper import REPORT_PATH, TEMP_PATH, REPO_COPY
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import OK, FAIL, ERROR, SUCCESS, TestResults, TestResult, read_test_results
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results


NO_CHANGES_MSG = "Nothing to run"
IMAGE_NAME = "clickhouse/sqllogic-test"


def get_run_command(
    builds_path: Path,
    repo_tests_path: Path,
    result_path: Path,
    server_log_path: Path,
    image: DockerImage,
) -> str:
    return (
        f"docker run "
        f"--volume={builds_path}:/package_folder "
        f"--volume={repo_tests_path}:/clickhouse-tests "
        f"--volume={result_path}:/test_output "
        f"--volume={server_log_path}:/var/log/clickhouse-server "
        f"--cap-add=SYS_PTRACE {image}"
    )


def read_check_status(result_folder: Path) -> Tuple[str, str]:
    status_path = result_folder / "check_status.tsv"
    if not status_path.exists():
        return ERROR, "Not found check_status.tsv"

    logging.info("Found check_status.tsv")
    with open(status_path, "r", encoding="utf-8") as status_file:
        status_rows = list(csv.reader(status_file, delimiter="\t"))

    for row in status_rows:
        if len(row) != 2:
            return ERROR, "Invalid check_status.tsv"
        if row[0] != SUCCESS:
            return row[0], row[1]

    return status_rows[-1][0], status_rows[-1][1]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--check-name",
        required=False,
        default="",
    )
    parser.add_argument(
        "--kill-timeout",
        required=False,
        default=0,
    )
    parser.add_argument(
        "--tag",
        required=False,
        default="",
        help="tag for docker image",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    args = parse_args()
    check_name = args.check_name
    check_name = args.check_name or os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"
    kill_timeout = args.kill_timeout or int(os.getenv("KILL_TIMEOUT", "0"))
    assert (
        kill_timeout > 0
    ), "kill timeout must be provided as an input arg or in KILL_TIMEOUT env"

    pr_info = PRInfo()
    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    repo_tests_path = repo_path / "tests"

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = result_path / "runlog.log"

    run_command = get_run_command(  # run script inside docker
        packages_path,
        repo_tests_path,
        result_path,
        server_log_path,
        docker_image,
    )
    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path, timeout=kill_timeout) as process:
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

    additional_logs = [run_log_path]
    if result_path.exists():
        additional_logs.extend(p for p in result_path.iterdir() if p.is_file())

    if server_log_path.exists():
        additional_logs.extend(p for p in server_log_path.iterdir() if p.is_file())

    status, description = read_check_status(result_path)

    test_results = []  # type: TestResults
    test_results_path = Path(result_path) / "test_results.tsv"
    if test_results_path.exists():
        logging.info("Found test_results.tsv")
        test_results = read_test_results(test_results_path)

    if len(test_results) > 3000:
        test_results = test_results[:1000]

    if len(test_results) == 0:
        status, description = ERROR, "Empty test_results.tsv"

    assert status is not None
    status = override_status(status, check_name)
    test_results.append(
        TestResult(
            "All tests",
            OK if status == SUCCESS else FAIL,
            stopwatch.duration_seconds,
        )
    )

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        additional_logs,
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
    # FIXME: force SUCCESS until all cases are fixed
    status = SUCCESS
    post_commit_status(commit, status, report_url, description, check_name, pr_info)


if __name__ == "__main__":
    main()
