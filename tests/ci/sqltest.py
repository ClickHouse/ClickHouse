#!/usr/bin/env python3

import logging
import subprocess
import os
import sys
from pathlib import Path
from typing import Dict

from github import Github

from build_download_helper import get_build_name_for_check, read_build_urls
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from commit_status_helper import (
    RerunHelper,
    get_commit,
    post_commit_status,
)
from docker_images_helper import pull_image, get_docker_image
from env_helper import (
    GITHUB_RUN_URL,
    REPORT_PATH,
    TEMP_PATH,
)
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResult
from s3_helper import S3Helper
from stopwatch import Stopwatch

IMAGE_NAME = "clickhouse/sqltest"


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


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    temp_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, check_name)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

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
        raise Exception("Cannot find the clickhouse binary among build results")

    logging.info("Got build url %s", build_url)

    workspace_path = temp_path / "workspace"
    if not os.path.exists(workspace_path):
        os.makedirs(workspace_path)

    run_command = get_run_command(
        pr_info.number, pr_info.sha, build_url, workspace_path, docker_image
    )
    logging.info("Going to run %s", run_command)

    run_log_path = temp_path / "run.log"
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
    s3_prefix = f"{pr_info.number}/{pr_info.sha}/sqltest_{check_name_lower}/"
    paths = {
        "run.log": run_log_path,
        "server.log.zst": workspace_path / "server.log.zst",
        "server.err.log.zst": workspace_path / "server.err.log.zst",
        "report.html": workspace_path / "report.html",
        "test.log": workspace_path / "test.log",
    }
    path_urls = {}  # type: Dict[str, str]

    s3_helper = S3Helper()
    for f in paths:
        try:
            path_urls[f] = s3_helper.upload_test_report_to_s3(paths[f], s3_prefix + f)
        except Exception as ex:
            logging.info("Exception uploading file %s text %s", f, ex)
            path_urls[f] = ""

    report_url = GITHUB_RUN_URL
    if path_urls["report.html"]:
        report_url = path_urls["report.html"]

    status = "success"
    description = "See the report"
    test_result = TestResult(description, "OK")

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
    post_commit_status(commit, status, report_url, description, check_name, pr_info)


if __name__ == "__main__":
    main()
