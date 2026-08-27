#!/usr/bin/env python3

import logging
import os
import subprocess
import sys
from pathlib import Path

from build_download_helper import read_build_urls
from ci_config import CI
from docker_images_helper import get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from pr_info import PRInfo
from report import SUCCESS, JobReport, TestResult
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

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    build_name = CI.get_required_build_name(check_name)
    print(build_name)
    urls = read_build_urls(build_name, reports_path)
    if not urls:
        raise ValueError("No build URLs found")

    for url in urls:
        if url.endswith("/clickhouse"):
            build_url = url
            break
    else:
        raise ValueError("Cannot find the clickhouse binary among build results")

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
    report_file_path = workspace_path / "report.html"

    paths = {
        "run.log": run_log_path,
        "server.log.zst": workspace_path / "server.log.zst",
        "server.err.log.zst": workspace_path / "server.err.log.zst",
        "report.html": report_file_path,
        "test.log": workspace_path / "test.log",
    }
    status = SUCCESS
    description = "See the report"
    test_results = [TestResult(description, "OK", log_files=[report_file_path])]

    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[v for _, v in paths.items()],
    ).dump()


if __name__ == "__main__":
    main()
