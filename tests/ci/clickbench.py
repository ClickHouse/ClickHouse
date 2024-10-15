#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from build_download_helper import download_all_deb_packages
from clickhouse_helper import CiLogsCredentials
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from pr_info import PRInfo
from report import ERROR, SUCCESS, JobReport, StatusType, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen


def get_image_name() -> str:
    return "clickhouse/clickbench"


def get_run_command(
    builds_path: Path,
    result_path: Path,
    server_log_path: Path,
    additional_envs: List[str],
    ci_logs_args: str,
    image: DockerImage,
) -> str:
    envs = [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)

    return (
        f"docker run --shm-size=16g --volume={builds_path}:/package_folder "
        f"{ci_logs_args}"
        f"--volume={result_path}:/test_output "
        f"--volume={server_log_path}:/var/log/clickhouse-server "
        "--security-opt seccomp=unconfined "  # required to issue io_uring sys-calls
        f"--cap-add=SYS_PTRACE {env_str} {image}"
    )


def process_results(
    result_directory: Path,
    server_log_path: Path,
) -> Tuple[StatusType, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []  # type: List[Path]
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible for content of result_directory.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    if server_log_path.exists():
        additional_files = additional_files + [
            p for p in server_log_path.iterdir() if p.is_file()
        ]

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found check_status.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"

        if results_path.exists():
            logging.info("Found %s", results_path.name)
        else:
            logging.info("Files in result folder %s", os.listdir(result_directory))
            return ERROR, "Not found test_results.tsv", test_results, additional_files

    except Exception as e:
        return (
            ERROR,
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files  # type: ignore


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    reports_path = Path(REPORT_PATH)

    args = parse_args()
    check_name = args.check_name

    pr_info = PRInfo()

    image_name = get_image_name()
    docker_image = pull_image(get_docker_image(image_name))

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = result_path / "run.log"

    additional_envs = []  # type: List[str]

    ci_logs_credentials = CiLogsCredentials(temp_path / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    run_command = get_run_command(
        packages_path,
        result_path,
        server_log_path,
        additional_envs,
        ci_logs_args,
        docker_image,
    )
    logging.info("Going to run ClickBench: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    try:
        subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    except subprocess.CalledProcessError:
        logging.warning("Failed to change files owner in %s, ignoring it", temp_path)

    ci_logs_credentials.clean_ci_logs_from_credentials(run_log_path)

    state, description, test_results, additional_logs = process_results(
        result_path, server_log_path
    )

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[run_log_path] + additional_logs,
    ).dump()

    if state != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
