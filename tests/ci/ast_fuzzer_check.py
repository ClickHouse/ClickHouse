#!/usr/bin/env python3

import logging
import subprocess
import os
import sys

from github import Github

from build_download_helper import get_build_name_for_check, read_build_urls
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import format_description, post_commit_status
from docker_pull_helper import get_image_with_version
from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_RUN_URL,
    REPORTS_PATH,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResult
from rerun_helper import RerunHelper
from s3_helper import S3Helper
from stopwatch import Stopwatch

IMAGE_NAME = "clickhouse/fuzzer"


def get_run_command(pr_number, sha, download_url, workspace_path, image):
    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        f"--volume={workspace_path}:/workspace "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
        f'-e PR_TO_TEST={pr_number} -e SHA_TO_TEST={sha} -e BINARY_URL_TO_DOWNLOAD="{download_url}" '
        f"{image}"
    )


def get_commit(gh, commit_sha):
    repo = gh.get_repo(GITHUB_REPOSITORY)
    commit = repo.get_commit(commit_sha)
    return commit


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    reports_path = REPORTS_PATH

    check_name = sys.argv[1]

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    pr_info = PRInfo()

    gh = Github(get_best_robot_token(), per_page=100)

    rerun_helper = RerunHelper(gh, pr_info, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = get_image_with_version(reports_path, IMAGE_NAME)

    build_name = get_build_name_for_check(check_name)
    print(build_name)
    urls = read_build_urls(build_name, reports_path)
    if not urls:
        raise Exception("No build URLs found")

    for url in urls:
        if url.endswith("/clickhouse"):
            build_url = url
            break
    else:
        raise Exception("Cannot binary clickhouse among build results")

    logging.info("Got build url %s", build_url)

    workspace_path = os.path.join(temp_path, "workspace")
    if not os.path.exists(workspace_path):
        os.makedirs(workspace_path)

    run_command = get_run_command(
        pr_info.number, pr_info.sha, build_url, workspace_path, docker_image
    )
    logging.info("Going to run %s", run_command)

    run_log_path = os.path.join(temp_path, "run.log")
    with open(run_log_path, "w", encoding="utf-8") as log:
        with subprocess.Popen(
            run_command, shell=True, stderr=log, stdout=log
        ) as process:
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    check_name_lower = (
        check_name.lower().replace("(", "").replace(")", "").replace(" ", "")
    )
    s3_prefix = f"{pr_info.number}/{pr_info.sha}/fuzzer_{check_name_lower}/"
    paths = {
        "run.log": run_log_path,
        "main.log": os.path.join(workspace_path, "main.log"),
        "server.log.zst": os.path.join(workspace_path, "server.log.zst"),
        "fuzzer.log": os.path.join(workspace_path, "fuzzer.log"),
        "report.html": os.path.join(workspace_path, "report.html"),
        "core.zst": os.path.join(workspace_path, "core.zst"),
        "dmesg.log": os.path.join(workspace_path, "dmesg.log"),
    }

    s3_helper = S3Helper()
    for f in paths:
        try:
            paths[f] = s3_helper.upload_test_report_to_s3(paths[f], s3_prefix + f)
        except Exception as ex:
            logging.info("Exception uploading file %s text %s", f, ex)
            paths[f] = ""

    report_url = GITHUB_RUN_URL
    if paths["report.html"]:
        report_url = paths["report.html"]

    # Try to get status message saved by the fuzzer
    try:
        with open(
            os.path.join(workspace_path, "status.txt"), "r", encoding="utf-8"
        ) as status_f:
            status = status_f.readline().rstrip("\n")

        with open(
            os.path.join(workspace_path, "description.txt"), "r", encoding="utf-8"
        ) as desc_f:
            description = desc_f.readline().rstrip("\n")
    except:
        status = "failure"
        description = "Task failed: $?=" + str(retcode)

    description = format_description(description)

    test_result = TestResult(description, "OK")
    if "fail" in status:
        test_result.status = "FAIL"

    ch_helper = ClickHouseHelper()

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        [test_result],
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    logging.info("Result: '%s', '%s', '%s'", status, description, report_url)
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, check_name, description, status, report_url)


if __name__ == "__main__":
    main()
