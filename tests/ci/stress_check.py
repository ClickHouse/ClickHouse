#!/usr/bin/env python3

import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from build_download_helper import download_all_deb_packages
from clickhouse_helper import CiLogsCredentials

from docker_images_helper import DockerImage, pull_image, get_docker_image
from env_helper import REPORT_PATH, TEMP_PATH, REPO_COPY
from pr_info import PRInfo
from report import JobReport, TestResult, TestResults, read_test_results
from stopwatch import Stopwatch
from tee_popen import TeePopen


def get_additional_envs() -> List[str]:
    result = []
    # some cloud-specific features require feature flags enabled
    # so we need this ENV to be able to disable the randomization
    # of feature flags
    result.append("RANDOMIZE_KEEPER_FEATURE_FLAGS=1")

    return result


def get_run_command(
    build_path: Path,
    result_path: Path,
    repo_tests_path: Path,
    server_log_path: Path,
    additional_envs: List[str],
    ci_logs_args: str,
    image: DockerImage,
) -> str:
    envs = [f"-e {e}" for e in additional_envs]
    env_str = " ".join(envs)

    cmd = (
        "docker run --cap-add=SYS_PTRACE "
        # For dmesg and sysctl
        "--privileged "
        # a static link, don't use S3_URL or S3_DOWNLOAD
        "-e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' "
        f"{ci_logs_args}"
        f"--volume={build_path}:/package_folder "
        f"--volume={result_path}:/test_output "
        f"--volume={repo_tests_path}:/usr/share/clickhouse-test "
        f"--volume={server_log_path}:/var/log/clickhouse-server {env_str} {image} "
    )

    return cmd


def process_results(
    result_directory: Path, server_log_path: Path, run_log_path: Path
) -> Tuple[str, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content
    # of result_folder.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    if server_log_path.exists():
        additional_files = additional_files + [
            p for p in server_log_path.iterdir() if p.is_file()
        ]

    additional_files.append(run_log_path)

    status_path = result_directory / "check_status.tsv"
    if not status_path.exists():
        return (
            "failure",
            "check_status.tsv doesn't exists",
            test_results,
            additional_files,
        )

    logging.info("Found check_status.tsv")
    with open(status_path, "r", encoding="utf-8") as status_file:
        status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path, True)
        if len(test_results) == 0:
            raise Exception("Empty results")
    except Exception as e:
        return (
            "error",
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files


def run_stress_test(docker_image_name: str) -> None:
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()
    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)
    repo_tests_path = repo_path / "tests"

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    pr_info = PRInfo()

    docker_image = pull_image(get_docker_image(docker_image_name))

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = temp_path / "run.log"
    ci_logs_credentials = CiLogsCredentials(temp_path / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    additional_envs = get_additional_envs()

    run_command = get_run_command(
        packages_path,
        result_path,
        repo_tests_path,
        server_log_path,
        additional_envs,
        ci_logs_args,
        docker_image,
    )
    logging.info("Going to run stress test: %s", run_command)

    timeout_expired = False
    timeout = 60 * 150
    with TeePopen(run_command, run_log_path, timeout=timeout) as process:
        retcode = process.wait()
        if process.timeout_exceeded:
            logging.info("Timeout expired for command: %s", run_command)
            timeout_expired = True
        elif retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    ci_logs_credentials.clean_ci_logs_from_credentials(run_log_path)

    state, description, test_results, additional_logs = process_results(
        result_path, server_log_path, run_log_path
    )

    if timeout_expired:
        test_results.append(TestResult.create_check_timeout_expired(timeout))
        state = "failure"
        description = test_results[-1].name

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_logs,
    ).dump()

    if state == "failure":
        sys.exit(1)


if __name__ == "__main__":
    run_stress_test("clickhouse/stress-test")
