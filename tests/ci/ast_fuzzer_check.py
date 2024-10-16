#!/usr/bin/env python3

import logging
import os
import subprocess
import sys
from pathlib import Path

from build_download_helper import read_build_urls
from ci_config import CI
from clickhouse_helper import CiLogsCredentials
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from pr_info import PRInfo
from report import FAIL, FAILURE, OK, SUCCESS, JobReport, TestResult
from stopwatch import Stopwatch
from tee_popen import TeePopen

IMAGE_NAME = "clickhouse/fuzzer"


def get_run_command(
    pr_info: PRInfo,
    build_url: str,
    workspace_path: Path,
    ci_logs_args: str,
    image: DockerImage,
) -> str:
    envs = [
        f"-e PR_TO_TEST={pr_info.number}",
        f"-e SHA_TO_TEST={pr_info.sha}",
        f"-e BINARY_URL_TO_DOWNLOAD='{build_url}'",
    ]

    env_str = " ".join(envs)

    return (
        f"docker run "
        # For sysctl
        "--privileged "
        "--network=host "
        f"{ci_logs_args}"
        f"--volume={workspace_path}:/workspace "
        f"{env_str} "
        "--cap-add syslog --cap-add sys_admin --cap-add=SYS_PTRACE "
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

    pr_info = PRInfo()

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

    ci_logs_credentials = CiLogsCredentials(temp_path / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    run_command = get_run_command(
        pr_info,
        build_url,
        workspace_path,
        ci_logs_args,
        docker_image,
    )
    logging.info("Going to run %s", run_command)

    run_log_path = temp_path / "run.log"
    main_log_path = workspace_path / "main.log"

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    ci_logs_credentials.clean_ci_logs_from_credentials(run_log_path)

    paths = {
        "run.log": run_log_path,
        "main.log": main_log_path,
        "report.html": workspace_path / "report.html",
        "core.zst": workspace_path / "core.zst",
        "dmesg.log": workspace_path / "dmesg.log",
        "fatal.log": workspace_path / "fatal.log",
        "stderr.log": workspace_path / "stderr.log",
    }

    compressed_server_log_path = workspace_path / "server.log.zst"
    if compressed_server_log_path.exists():
        paths["server.log.zst"] = compressed_server_log_path
    else:
        # The script can fail before the invocation of `zstd`, but we are still interested in its log:
        not_compressed_server_log_path = workspace_path / "server.log"
        if not_compressed_server_log_path.exists():
            paths["server.log"] = not_compressed_server_log_path

    # Same idea but with the fuzzer log
    compressed_fuzzer_log_path = workspace_path / "fuzzer.log.zst"
    if compressed_fuzzer_log_path.exists():
        paths["fuzzer.log.zst"] = compressed_fuzzer_log_path
    else:
        not_compressed_fuzzer_log_path = workspace_path / "fuzzer.log"
        if not_compressed_fuzzer_log_path.exists():
            paths["fuzzer.log"] = not_compressed_fuzzer_log_path

    # Try to get status message saved by the fuzzer
    try:
        with open(workspace_path / "status.txt", "r", encoding="utf-8") as status_f:
            status = status_f.readline().rstrip("\n")

        with open(workspace_path / "description.txt", "r", encoding="utf-8") as desc_f:
            description = desc_f.readline().rstrip("\n")
    except:
        status = FAILURE
        description = "Task failed: $?=" + str(retcode)

    test_result = TestResult(description, OK)
    if "fail" in status:
        test_result.status = FAIL

    JobReport(
        description=description,
        test_results=[test_result],
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        # test generates its own report.html
        additional_files=[v for _, v in paths.items()],
    ).dump()

    logging.info("Result: '%s', '%s'", status, description)
    if status != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
