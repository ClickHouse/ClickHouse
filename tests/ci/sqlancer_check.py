#!/usr/bin/env python3

import logging
import os
import subprocess
import sys
from pathlib import Path

from build_download_helper import read_build_urls
from ci_config import CI
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from report import FAILURE, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen

IMAGE_NAME = "clickhouse/sqlancer-test"


def get_run_command(download_url: str, workspace_path: Path, image: DockerImage) -> str:
    return (
        # For sysctl
        "docker run --privileged --network=host "
        f"--volume={workspace_path}:/workspace "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
        f'-e BINARY_URL_TO_DOWNLOAD="{download_url}" '
        f"{image}"
    )


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    reports_path = Path(REPORT_PATH)

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    build_name = CI.get_required_build_name(check_name)
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
    workspace_path.mkdir(parents=True, exist_ok=True)

    run_command = get_run_command(build_url, workspace_path, docker_image)
    logging.info("Going to run %s", run_command)

    run_log_path = workspace_path / "run.log"
    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

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
        workspace_path / "clickhouse-server.log",
        workspace_path / "stderr.log",
        workspace_path / "stdout.log",
    ]
    paths += [workspace_path / f"{t}.err" for t in tests]
    paths += [workspace_path / f"{t}.out" for t in tests]

    status = SUCCESS
    test_results = []  # type: TestResults
    # Try to get status message saved by the SQLancer
    try:
        with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
            status = status_f.readline().rstrip("\n")
        if (workspace_path / "server_crashed.log").exists():
            test_results.append(TestResult("Server crashed", "FAIL"))
        with open(workspace_path / "summary.tsv", "r", encoding="utf-8") as summary_f:
            for line in summary_f:
                l = line.rstrip("\n").split("\t")
                test_results.append(TestResult(l[0], l[1]))
        with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
            description = desc_f.readline().rstrip("\n")
    except:
        status = FAILURE
        description = "Task failed: $?=" + str(retcode)

    if not test_results:
        test_results = [TestResult(name=__file__, status=status)]

    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=paths,
    ).dump()


if __name__ == "__main__":
    main()
