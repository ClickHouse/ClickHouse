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
from docker_pull_helper import get_image_with_version, DockerImage
from env_helper import S3_BUILDS_BUCKET, TEMP_PATH, REPORTS_PATH
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


def get_fasttest_cmd(
    workspace: Path,
    output_path: Path,
    repo_path: Path,
    pr_number: int,
    commit_sha: str,
    image: DockerImage,
) -> str:
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


def process_results(result_directory: Path) -> Tuple[str, str, TestResults]:
    test_results = []  # type: TestResults
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible for content of
    # result_directory

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found test_results.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return "error", "Invalid check_status.tsv", test_results
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            return "error", "Empty test_results.tsv", test_results
    except Exception as e:
        return ("error", f"Cannot parse test_results.tsv ({e})", test_results)

    return state, description, test_results


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    reports_path = Path(REPORTS_PATH)
    reports_path.mkdir(parents=True, exist_ok=True)

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

    docker_image = get_image_with_version(reports_path, "clickhouse/fasttest")

    s3_helper = S3Helper()

    workspace = temp_path / "fasttest-workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    output_path = temp_path / "fasttest-output"
    output_path.mkdir(parents=True, exist_ok=True)

    repo_path = temp_path / "fasttest-repo"
    repo_path.mkdir(parents=True, exist_ok=True)

    run_cmd = get_fasttest_cmd(
        workspace,
        output_path,
        repo_path,
        pr_info.number,
        pr_info.sha,
        docker_image,
    )
    logging.info("Going to run fasttest with cmd %s", run_cmd)

    logs_path = temp_path / "fasttest-logs"
    logs_path.mkdir(parents=True, exist_ok=True)

    run_log_path = logs_path / "run.log"
    with TeePopen(run_cmd, run_log_path, timeout=90 * 60) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    test_output_files = os.listdir(output_path)
    additional_logs = [f for f in output_path.iterdir() if f.is_file()]
    additional_logs.append(run_log_path)

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
        state, description, test_results = process_results(output_path)

    ch_helper = ClickHouseHelper()
    s3_path_prefix = "/".join(
        (
            get_release_or_pr(pr_info, get_version_from_repo())[0],
            pr_info.sha,
            "fast_tests",
        )
    )
    build_urls = s3_helper.upload_build_directory_to_s3(
        output_path / "binaries",
        s3_path_prefix,
        keep_dirs_in_s3_path=False,
        upload_symlinks=False,
    )

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        additional_logs,
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
