#!/usr/bin/env python3
import csv
import logging
import os
import subprocess
import sys


from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import fail_simple_check, post_commit_status
from docker_pull_helper import get_image_with_version
from env_helper import GITHUB_WORKSPACE, RUNNER_TEMP
from get_robot_token import get_best_robot_token
from github_helper import GitHub
from pr_info import PRInfo
from rerun_helper import RerunHelper
from s3_helper import S3Helper
from stopwatch import Stopwatch
from upload_result_helper import upload_results

NAME = "Style Check (actions)"


def process_result(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible
    # for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [
            f
            for f in os.listdir(result_folder)
            if os.path.isfile(os.path.join(result_folder, f))
        ]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    status = []
    status_path = os.path.join(result_folder, "check_status.tsv")
    if os.path.exists(status_path):
        logging.info("Found test_results.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_folder))
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = os.path.join(result_folder, "test_results.tsv")
        with open(results_path, "r", encoding="utf-8") as fd:
            test_results = list(csv.reader(fd, delimiter="\t"))
        if len(test_results) == 0:
            raise Exception("Empty results")

        return state, description, test_results, additional_files
    except Exception:
        if state == "success":
            state, description = "error", "Failed to read test_results.tsv"
        return state, description, test_results, additional_files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    repo_path = GITHUB_WORKSPACE
    temp_path = os.path.join(RUNNER_TEMP, "style_check")

    pr_info = PRInfo()

    gh = GitHub(get_best_robot_token())

    rerun_helper = RerunHelper(gh, pr_info, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    docker_image = get_image_with_version(temp_path, "clickhouse/style-test")
    s3_helper = S3Helper("https://s3.amazonaws.com")

    cmd = (
        f"docker run -u $(id -u ${{USER}}):$(id -g ${{USER}}) --cap-add=SYS_PTRACE "
        f"--volume={repo_path}:/ClickHouse --volume={temp_path}:/test_output "
        f"{docker_image}"
    )

    logging.info("Is going to run the command: %s", cmd)
    subprocess.check_call(
        cmd,
        shell=True,
    )

    state, description, test_results, additional_files = process_result(temp_path)
    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, NAME, test_results)

    report_url = upload_results(
        s3_helper, pr_info.number, pr_info.sha, test_results, additional_files, NAME
    )
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, NAME, description, state, report_url)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        NAME,
    )
    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state in ["error", "failure"]:
        fail_simple_check(gh, pr_info, f"{NAME} failed")
        sys.exit(1)
