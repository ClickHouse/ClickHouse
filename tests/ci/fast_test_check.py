#!/usr/bin/env python3

import logging
import subprocess
import os
import csv
import sys

from github import Github

from env_helper import CACHES_PATH, TEMP_PATH
from pr_info import FORCE_TESTS_LABEL, PRInfo
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from upload_result_helper import upload_results
from docker_pull_helper import get_image_with_version
from commit_status_helper import (
    post_commit_status,
    fail_simple_check,
)
from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from stopwatch import Stopwatch
from rerun_helper import RerunHelper
from tee_popen import TeePopen
from ccache_utils import get_ccache_if_not_exists, upload_ccache

NAME = "Fast test (actions)"


def get_fasttest_cmd(
    workspace, output_path, ccache_path, repo_path, pr_number, commit_sha, image
):
    return (
        f"docker run --cap-add=SYS_PTRACE "
        f"-e FASTTEST_WORKSPACE=/fasttest-workspace -e FASTTEST_OUTPUT=/test_output "
        f"-e FASTTEST_SOURCE=/ClickHouse --cap-add=SYS_PTRACE "
        f"-e PULL_REQUEST_NUMBER={pr_number} -e COMMIT_SHA={commit_sha} "
        f"-e COPY_CLICKHOUSE_BINARY_TO_OUTPUT=1 "
        f"--volume={workspace}:/fasttest-workspace --volume={repo_path}:/ClickHouse "
        f"--volume={output_path}:/test_output "
        f"--volume={ccache_path}:/fasttest-workspace/ccache {image}"
    )


def process_results(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of
    # result_folder
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

    results_path = os.path.join(result_folder, "test_results.tsv")
    if os.path.exists(results_path):
        with open(results_path, "r", encoding="utf-8") as results_file:
            test_results = list(csv.reader(results_file, delimiter="\t"))
    if len(test_results) == 0:
        return "error", "Empty test_results.tsv", test_results, additional_files

    return state, description, test_results, additional_files


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    pr_info = PRInfo()

    gh = Github(get_best_robot_token())

    rerun_helper = RerunHelper(gh, pr_info, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = get_image_with_version(temp_path, "clickhouse/fasttest")

    s3_helper = S3Helper("https://s3.amazonaws.com")

    workspace = os.path.join(temp_path, "fasttest-workspace")
    if not os.path.exists(workspace):
        os.makedirs(workspace)

    output_path = os.path.join(temp_path, "fasttest-output")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    if not os.path.exists(CACHES_PATH):
        os.makedirs(CACHES_PATH)
    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {CACHES_PATH}", shell=True)
    cache_path = os.path.join(CACHES_PATH, "fasttest")

    logging.info("Will try to fetch cache for our build")
    ccache_for_pr = get_ccache_if_not_exists(
        cache_path, s3_helper, pr_info.number, temp_path
    )
    upload_master_ccache = ccache_for_pr in (-1, 0)

    if not os.path.exists(cache_path):
        logging.info("cache was not fetched, will create empty dir")
        os.makedirs(cache_path)

    repo_path = os.path.join(temp_path, "fasttest-repo")
    if not os.path.exists(repo_path):
        os.makedirs(repo_path)

    run_cmd = get_fasttest_cmd(
        workspace,
        output_path,
        cache_path,
        repo_path,
        pr_info.number,
        pr_info.sha,
        docker_image,
    )
    logging.info("Going to run fasttest with cmd %s", run_cmd)

    logs_path = os.path.join(temp_path, "fasttest-logs")
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)

    run_log_path = os.path.join(logs_path, "runlog.log")
    with TeePopen(run_cmd, run_log_path, timeout=40 * 60) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {cache_path}", shell=True)

    test_output_files = os.listdir(output_path)
    additional_logs = []
    for f in test_output_files:
        additional_logs.append(os.path.join(output_path, f))

    test_log_exists = (
        "test_log.txt" in test_output_files or "test_result.txt" in test_output_files
    )
    test_result_exists = "test_results.tsv" in test_output_files
    test_results = []
    if "submodule_log.txt" not in test_output_files:
        description = "Cannot clone repository"
        state = "failure"
    elif "cmake_log.txt" not in test_output_files:
        description = "Cannot fetch submodules"
        state = "failure"
    elif "build_log.txt" not in test_output_files:
        description = "Cannot finish cmake"
        state = "failure"
    elif "install_log.txt" not in test_output_files:
        description = "Cannot build ClickHouse"
        state = "failure"
    elif not test_log_exists and not test_result_exists:
        description = "Cannot install or start ClickHouse"
        state = "failure"
    else:
        state, description, test_results, additional_logs = process_results(output_path)

    logging.info("Will upload cache")
    upload_ccache(cache_path, s3_helper, pr_info.number, temp_path)
    if upload_master_ccache:
        logging.info("Will upload a fallback cache for master")
        upload_ccache(cache_path, s3_helper, 0, temp_path)

    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, NAME, test_results)

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [run_log_path] + additional_logs,
        NAME,
        True,
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

    # Refuse other checks to run if fast test failed
    if state != "success":
        if FORCE_TESTS_LABEL in pr_info.labels and state != "error":
            print(f"'{FORCE_TESTS_LABEL}' enabled, will report success")
        else:
            fail_simple_check(gh, pr_info, f"{NAME} failed")
            sys.exit(1)
