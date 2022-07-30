#!/usr/bin/env python3

from distutils.version import StrictVersion
import logging
import os
import subprocess
import sys

from github import Github

from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_builds_filter
from upload_result_helper import upload_results
from docker_pull_helper import get_images_with_versions
from commit_status_helper import post_commit_status
from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from stopwatch import Stopwatch
from rerun_helper import RerunHelper

IMAGE_UBUNTU = "clickhouse/test-old-ubuntu"
IMAGE_CENTOS = "clickhouse/test-old-centos"
MAX_GLIBC_VERSION = "2.4"
DOWNLOAD_RETRIES_COUNT = 5
CHECK_NAME = "Compatibility check"


def process_os_check(log_path):
    name = os.path.basename(log_path)
    with open(log_path, "r") as log:
        line = log.read().split("\n")[0].strip()
        if line != "OK":
            return (name, "FAIL")
        else:
            return (name, "OK")


def process_glibc_check(log_path):
    bad_lines = []
    with open(log_path, "r") as log:
        for line in log:
            if line.strip():
                columns = line.strip().split(" ")
                symbol_with_glibc = columns[-2]  # sysconf@GLIBC_2.2.5
                _, version = symbol_with_glibc.split("@GLIBC_")
                if version == "PRIVATE":
                    bad_lines.append((symbol_with_glibc, "FAIL"))
                elif StrictVersion(version) > MAX_GLIBC_VERSION:
                    bad_lines.append((symbol_with_glibc, "FAIL"))
    if not bad_lines:
        bad_lines.append(("glibc check", "OK"))
    return bad_lines


def process_result(result_folder, server_log_folder):
    summary = process_glibc_check(os.path.join(result_folder, "glibc.log"))

    status = "success"
    description = "Compatibility check passed"
    if len(summary) > 1 or summary[0][1] != "OK":
        status = "failure"
        description = "glibc check failed"

    if status == "success":
        for operating_system in ("ubuntu:12.04", "centos:5"):
            result = process_os_check(os.path.join(result_folder, operating_system))
            if result[1] != "OK":
                status = "failure"
                description = f"Old {operating_system} failed"
                summary += [result]
                break
            summary += [result]

    server_log_path = os.path.join(server_log_folder, "clickhouse-server.log")
    stderr_log_path = os.path.join(server_log_folder, "stderr.log")
    client_stderr_log_path = os.path.join(server_log_folder, "clientstderr.log")
    result_logs = []
    if os.path.exists(server_log_path):
        result_logs.append(server_log_path)

    if os.path.exists(stderr_log_path):
        result_logs.append(stderr_log_path)

    if os.path.exists(client_stderr_log_path):
        result_logs.append(client_stderr_log_path)

    return status, description, summary, result_logs


def get_run_commands(
    build_path, result_folder, server_log_folder, image_centos, image_ubuntu
):
    return [
        f"readelf -s {build_path}/usr/bin/clickhouse | grep '@GLIBC_' > {result_folder}/glibc.log",
        f"readelf -s {build_path}/usr/bin/clickhouse-odbc-bridge | grep '@GLIBC_' >> {result_folder}/glibc.log",
        f"docker run --network=host --volume={build_path}/usr/bin/clickhouse:/clickhouse "
        f"--volume={build_path}/etc/clickhouse-server:/config "
        f"--volume={server_log_folder}:/var/log/clickhouse-server {image_ubuntu} > {result_folder}/ubuntu:12.04",
        f"docker run --network=host --volume={build_path}/usr/bin/clickhouse:/clickhouse "
        f"--volume={build_path}/etc/clickhouse-server:/config "
        f"--volume={server_log_folder}:/var/log/clickhouse-server {image_centos} > {result_folder}/centos:5",
    ]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    reports_path = REPORTS_PATH

    pr_info = PRInfo()

    gh = Github(get_best_robot_token())

    rerun_helper = RerunHelper(gh, pr_info, CHECK_NAME)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_images = get_images_with_versions(reports_path, [IMAGE_CENTOS, IMAGE_UBUNTU])

    packages_path = os.path.join(temp_path, "packages")
    if not os.path.exists(packages_path):
        os.makedirs(packages_path)

    def url_filter(url):
        return url.endswith(".deb") and (
            "clickhouse-common-static_" in url or "clickhouse-server_" in url
        )

    download_builds_filter(CHECK_NAME, reports_path, packages_path, url_filter)

    for f in os.listdir(packages_path):
        if ".deb" in f:
            full_path = os.path.join(packages_path, f)
            subprocess.check_call(
                f"dpkg -x {full_path} {packages_path} && rm {full_path}", shell=True
            )

    server_log_path = os.path.join(temp_path, "server_log")
    if not os.path.exists(server_log_path):
        os.makedirs(server_log_path)

    result_path = os.path.join(temp_path, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_commands = get_run_commands(
        packages_path, result_path, server_log_path, docker_images[0], docker_images[1]
    )

    state = "success"
    for run_command in run_commands:
        try:
            logging.info("Running command %s", run_command)
            subprocess.check_call(run_command, shell=True)
        except subprocess.CalledProcessError as ex:
            logging.info("Exception calling command %s", ex)
            state = "failure"

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    s3_helper = S3Helper("https://s3.amazonaws.com")
    state, description, test_results, additional_logs = process_result(
        result_path, server_log_path
    )

    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, CHECK_NAME, test_results)

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        additional_logs,
        CHECK_NAME,
    )
    print(f"::notice ::Report url: {report_url}")
    post_commit_status(gh, pr_info.sha, CHECK_NAME, description, state, report_url)

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        CHECK_NAME,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state == "error":
        sys.exit(1)
