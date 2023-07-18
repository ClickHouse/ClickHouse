#!/usr/bin/env python3

import logging
import subprocess
import os
import csv
import sys
import atexit
from pathlib import Path
from typing import List, Tuple

from github import Github

from build_check import get_release_or_pr
from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    RerunHelper,
    get_commit,
    post_commit_status,
    update_mergeable_check,
)
from docker_pull_helper import get_image_with_version
from env_helper import S3_BUILDS_BUCKET, TEMP_PATH
from get_robot_token import get_best_robot_token
from pr_info import FORCE_TESTS_LABEL, PRInfo
from report import TestResults, read_test_results
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results
from version_helper import get_version_from_repo

NAME = "Fast test"

# Will help to avoid errors like _csv.Error: field larger than field limit (131072)
csv.field_size_limit(sys.maxsize)


def get_fasttest_cmd(workspace, output_path, repo_path, pr_number, commit_sha, image):
    return (
        f"docker run --cap-add=SYS_PTRACE "
        "--network=host "  # required to get access to IAM credentials
        f"-e FASTTEST_WORKSPACE=/fasttest-workspace -e FASTTEST_OUTPUT=/test_output "
        f"-e FASTTEST_SOURCE=/ClickHouse --cap-add=SYS_PTRACE "
        f"-e FASTTEST_CMAKE_FLAGS='-DCOMPILER_CACHE=sccache' "
        f"-e PULL_REQUEST_NUMBER={pr_number} -e COMMIT_SHA={commit_sha} "
        f"-e COPY_CLICKHOUSE_BINARY_TO_OUTPUT=1 "
        f"-e SCCACHE_BUCKET={S3_BUILDS_BUCKET} -e SCCACHE_S3_KEY_PREFIX=ccache/sccache "
        f"--volume={workspace}:/fasttest-workspace --volume={repo_path}:/ClickHouse "
        f"--volume={output_path}:/test_output {image}"
    )


def process_results(result_folder: str) -> Tuple[str, str, TestResults, List[str]]:
    test_results = []  # type: TestResults
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

    try:
        results_path = Path(result_folder) / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            return "error", "Empty test_results.tsv", test_results, additional_files
    except Exception as e:
        return (
            "error",
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    pr_info = PRInfo()

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    atexit.register(update_mergeable_check, gh, pr_info, NAME)

    rerun_helper = RerunHelper(commit, NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        status = rerun_helper.get_finished_status()
        if status is not None and status.state != "success":
            sys.exit(1)
        sys.exit(0)

    docker_image = get_image_with_version(temp_path, "clickhouse/fasttest")

    s3_helper = S3Helper()

    workspace = os.path.join(temp_path, "fasttest-workspace")
    if not os.path.exists(workspace):
        os.makedirs(workspace)

    output_path = os.path.join(temp_path, "fasttest-output")
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    repo_path = os.path.join(temp_path, "fasttest-repo")
    if not os.path.exists(repo_path):
        os.makedirs(repo_path)

    run_cmd = get_fasttest_cmd(
        workspace,
        output_path,
        repo_path,
        pr_info.number,
        pr_info.sha,
        docker_image,
    )
    logging.info("Going to run fasttest with cmd %s", run_cmd)

    logs_path = os.path.join(temp_path, "fasttest-logs")
    if not os.path.exists(logs_path):
        os.makedirs(logs_path)

    run_log_path = os.path.join(logs_path, "run.log")
    with TeePopen(run_cmd, run_log_path, timeout=90 * 60) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    test_output_files = os.listdir(output_path)
    additional_logs = []
    for f in test_output_files:
        additional_logs.append(os.path.join(output_path, f))

    test_log_exists = (
        "test_log.txt" in test_output_files or "test_result.txt" in test_output_files
    )
    test_result_exists = "test_results.tsv" in test_output_files
    test_results = []  # type: TestResults
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

    ch_helper = ClickHouseHelper()
    s3_path_prefix = os.path.join(
        get_release_or_pr(pr_info, get_version_from_repo())[0],
        pr_info.sha,
        "fast_tests",
    )
    build_urls = s3_helper.upload_build_folder_to_s3(
        os.path.join(output_path, "binaries"),
        s3_path_prefix,
        keep_dirs_in_s3_path=False,
        upload_symlinks=False,
    )

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [run_log_path] + additional_logs,
        NAME,
        build_urls,
    )
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(commit, state, report_url, description, NAME, pr_info)

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
        if state == "error":
            print("The status is 'error', report failure disregard the labels")
            sys.exit(1)
        elif FORCE_TESTS_LABEL in pr_info.labels:
            print(f"'{FORCE_TESTS_LABEL}' enabled, reporting success")
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
