#!/usr/bin/env python3

import csv
import logging
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import List, Tuple

from build_download_helper import download_all_deb_packages
from ci_utils import Shell
from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPO_COPY, REPORT_PATH, TEMP_PATH
from get_robot_token import get_parameter_from_ssm
from pr_info import PRInfo
from report import ERROR, JobReport, TestResults, read_test_results
from stopwatch import Stopwatch
from tee_popen import TeePopen


class SensitiveFormatter(logging.Formatter):
    @staticmethod
    def _filter(s):
        return re.sub(
            r"(.*)(AZURE_CONNECTION_STRING.*\')(.*)", r"\1AZURE_CONNECTION_STRING\3", s
        )

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self._filter(original)


def get_additional_envs(check_name: str) -> List[str]:
    result = []
    azure_connection_string = get_parameter_from_ssm("azure_connection_string")
    result.append(f"AZURE_CONNECTION_STRING='{azure_connection_string}'")
    # some cloud-specific features require feature flags enabled
    # so we need this ENV to be able to disable the randomization
    # of feature flags
    result.append("RANDOMIZE_KEEPER_FEATURE_FLAGS=1")
    if "azure" in check_name:
        result.append("USE_AZURE_STORAGE_FOR_MERGE_TREE=1")

    if "s3" in check_name:
        result.append("USE_S3_STORAGE_FOR_MERGE_TREE=1")

    return result


def get_run_command(
    build_path: Path,
    result_path: Path,
    repo_tests_path: Path,
    server_log_path: Path,
    additional_envs: List[str],
    image: DockerImage,
    upgrade_check: bool,
) -> str:
    envs = [f"-e {e}" for e in additional_envs]
    env_str = " ".join(envs)

    if upgrade_check:
        run_script = "/repo/tests/docker_scripts/upgrade_runner.sh"
    else:
        run_script = "/repo/tests/docker_scripts/stress_runner.sh"

    cmd = (
        "docker run --cap-add=SYS_PTRACE "
        # For dmesg and sysctl
        "--privileged "
        # a static link, don't use S3_URL or S3_DOWNLOAD
        "-e S3_URL='https://s3.amazonaws.com/clickhouse-datasets' "
        "--tmpfs /tmp/clickhouse "
        f"--volume={build_path}:/package_folder "
        f"--volume={result_path}:/test_output "
        f"--volume={repo_tests_path}/..:/repo "
        f"--volume={server_log_path}:/var/log/clickhouse-server {env_str} {image} {run_script}"
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
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path, True)
        if len(test_results) == 0:
            raise ValueError("Empty results")
    except Exception as e:
        return (
            ERROR,
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files


def run_stress_test(upgrade_check: bool = False) -> None:
    logging.basicConfig(level=logging.INFO)
    for handler in logging.root.handlers:
        # pylint: disable=protected-access
        handler.setFormatter(SensitiveFormatter(handler.formatter._fmt))  # type: ignore

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

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    if check_name.startswith("amd_") or check_name.startswith("arm_"):
        # this is praktika based CI
        print("Copy input *.deb artifacts")
        assert Shell.check(f"cp {REPO_COPY}/ci/tmp/*.deb {packages_path}", verbose=True)
    else:
        download_all_deb_packages(check_name, reports_path, packages_path)

    docker_image = pull_image(get_docker_image("clickhouse/stress-test"))

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = temp_path / "run.log"

    additional_envs = get_additional_envs(check_name)

    run_command = get_run_command(
        packages_path,
        result_path,
        repo_tests_path,
        server_log_path,
        additional_envs,
        docker_image,
        upgrade_check,
    )
    logging.info("Going to run stress test: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    state, description, test_results, additional_logs = process_results(
        result_path, server_log_path, run_log_path
    )

    Shell.check("pwd", verbose=True)
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
    run_stress_test()
