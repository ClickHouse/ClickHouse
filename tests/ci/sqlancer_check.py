#!/usr/bin/env python3

import logging
import subprocess
import os
import sys
from typing import List, Tuple

from github import Github

from env_helper import (
    GITHUB_REPOSITORY,
    GITHUB_RUN_URL,
    REPORTS_PATH,
    REPO_COPY,
    TEMP_PATH,
)
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import get_build_name_for_check, read_build_urls
from docker_pull_helper import get_image_with_version
from commit_status_helper import post_commit_status
from clickhouse_helper import ClickHouseHelper, prepare_tests_results_for_clickhouse
from upload_result_helper import upload_results
from stopwatch import Stopwatch
from rerun_helper import RerunHelper

IMAGE_NAME = "clickhouse/sqlancer-test"


def get_run_command(download_url, workspace_path, image):
    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        f"--volume={workspace_path}:/workspace "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
        f'-e BINARY_URL_TO_DOWNLOAD="{download_url}" '
        f"{image}"
    )


def get_commit(gh, commit_sha):
    repo = gh.get_repo(GITHUB_REPOSITORY)
    commit = repo.get_commit(commit_sha)
    return commit


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
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
        raise Exception("Cannot find binary clickhouse among build results")

    logging.info("Got build url %s", build_url)

    workspace_path = os.path.join(temp_path, "workspace")
    if not os.path.exists(workspace_path):
        os.makedirs(workspace_path)

    run_command = get_run_command(build_url, workspace_path, docker_image)
    logging.info("Going to run %s", run_command)

    run_log_path = os.path.join(workspace_path, "runlog.log")
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
    s3_prefix = f"{pr_info.number}/{pr_info.sha}/{check_name_lower}/"

    tests = [
        "TLPGroupBy",
        "TLPHaving",
        "TLPWhere",
        "TLPDistinct",
        "TLPAggregate",
        "NoREC",
    ]

    paths = [
        run_log_path,
        os.path.join(workspace_path, "clickhouse-server.log"),
        os.path.join(workspace_path, "stderr.log"),
        os.path.join(workspace_path, "stdout.log"),
    ]
    for t in tests:
        err_name = f"{t}.err"
        log_name = f"{t}.out"
        paths.append(os.path.join(workspace_path, err_name))
        paths.append(os.path.join(workspace_path, log_name))

    s3_helper = S3Helper()
    report_url = GITHUB_RUN_URL

    status = "success"
    test_results = []  # type: List[Tuple[str, str]]
    # Try to get status message saved by the SQLancer
    try:
        # with open(
        #     os.path.join(workspace_path, "status.txt"), "r", encoding="utf-8"
        # ) as status_f:
        #     status = status_f.readline().rstrip("\n")
        if os.path.exists(os.path.join(workspace_path, "server_crashed.log")):
            test_results.append(("Server crashed", "FAIL"))
        with open(
            os.path.join(workspace_path, "summary.tsv"), "r", encoding="utf-8"
        ) as summary_f:
            for line in summary_f:
                l = line.rstrip("\n").split("\t")
                test_results.append((l[0], l[1]))

        with open(
            os.path.join(workspace_path, "description.txt"), "r", encoding="utf-8"
        ) as desc_f:
            description = desc_f.readline().rstrip("\n")[:140]
    except:
        # status = "failure"
        description = "Task failed: $?=" + str(retcode)

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        paths,
        check_name,
        False,
    )

    post_commit_status(gh, pr_info.sha, check_name, description, status, report_url)

    print(f"::notice:: {check_name} Report url: {report_url}")

    ch_helper = ClickHouseHelper()

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        status,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    print(f"::notice Result: '{status}', '{description}', '{report_url}'")
    post_commit_status(gh, pr_info.sha, check_name, description, status, report_url)
