#!/usr/bin/env python3
import argparse
import logging
import subprocess
import os
import csv
import sys
from pathlib import Path
from typing import Tuple

from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import S3_BUILDS_BUCKET, TEMP_PATH, REPO_COPY
from pr_info import FORCE_TESTS_LABEL, PRInfo
from report import JobReport, TestResult, TestResults, read_test_results
from stopwatch import Stopwatch
from tee_popen import TeePopen

NAME = "Fast test"

# Will help to avoid errors like _csv.Error: field larger than field limit (131072)
csv.field_size_limit(sys.maxsize)


def get_fasttest_cmd(
    workspace: Path,
    output_path: Path,
    repo_path: Path,
    pr_number: int,
    commit_sha: str,
    image: DockerImage,
) -> str:
    return (
        f"docker run --cap-add=SYS_PTRACE --user={os.geteuid()}:{os.getegid()} "
        "--network=host "  # required to get access to IAM credentials
        f"-e FASTTEST_WORKSPACE=/fasttest-workspace -e FASTTEST_OUTPUT=/test_output "
        f"-e FASTTEST_SOURCE=/ClickHouse --cap-add=SYS_PTRACE "
        f"-e FASTTEST_CMAKE_FLAGS='-DCOMPILER_CACHE=sccache' "
        f"-e PULL_REQUEST_NUMBER={pr_number} -e COMMIT_SHA={commit_sha} "
        f"-e COPY_CLICKHOUSE_BINARY_TO_OUTPUT=1 "
        f"-e SCCACHE_BUCKET={S3_BUILDS_BUCKET} -e SCCACHE_S3_KEY_PREFIX=ccache/sccache "
        "-e stage=clone_submodules "
        f"--volume={workspace}:/fasttest-workspace --volume={repo_path}:/ClickHouse "
        f"--volume={output_path}:/test_output {image}"
    )


def process_results(result_directory: Path) -> Tuple[str, str, TestResults]:
    test_results = []  # type: TestResults
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible for content of
    # result_directory

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found %s", status_path.name)
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))
    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return "error", "Invalid check_status.tsv", test_results
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            return "error", "Empty test_results.tsv", test_results
    except Exception as e:
        return ("error", f"Cannot parse test_results.tsv ({e})", test_results)

    return state, description, test_results


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="FastTest script",
    )

    parser.add_argument(
        "--timeout",
        type=int,
        # Fast tests in most cases done within 10 min and 40 min timout should be sufficient,
        # though due to cold cache build time can be much longer
        # https://pastila.nl/?146195b6/9bb99293535e3817a9ea82c3f0f7538d.link#5xtClOjkaPLEjSuZ92L2/g==
        default=40,
        help="Timeout in minutes",
    )
    args = parser.parse_args()
    args.timeout = args.timeout * 60
    return args


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()
    args = parse_args()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    pr_info = PRInfo()

    docker_image = pull_image(get_docker_image("clickhouse/fasttest"))

    workspace = temp_path / "fasttest-workspace"
    workspace.mkdir(parents=True, exist_ok=True)

    output_path = temp_path / "fasttest-output"
    output_path.mkdir(parents=True, exist_ok=True)

    repo_path = Path(REPO_COPY)

    run_cmd = get_fasttest_cmd(
        workspace,
        output_path,
        repo_path,
        pr_info.number,
        pr_info.sha,
        docker_image,
    )
    logging.info("Going to run fasttest with cmd %s", run_cmd)

    logs_path = temp_path / "fasttest-logs"
    logs_path.mkdir(parents=True, exist_ok=True)

    run_log_path = logs_path / "run.log"
    timeout_expired = False

    with TeePopen(run_cmd, run_log_path, timeout=args.timeout) as process:
        retcode = process.wait()
        if process.timeout_exceeded:
            logging.info("Timeout expired for command: %s", run_cmd)
            timeout_expired = True
        elif retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    test_output_files = os.listdir(output_path)
    additional_logs = [f for f in output_path.iterdir() if f.is_file()]
    additional_logs.append(run_log_path)

    test_log_exists = (
        "test_log.txt" in test_output_files or "test_result.txt" in test_output_files
    )
    test_result_exists = "test_results.tsv" in test_output_files
    test_results = []  # type: TestResults
    if "submodule_log.txt" not in test_output_files:
        description = "Cannot clone repository"
        state = "failure"
    elif "cmake_log.txt" not in test_output_files:
        description = "Cannot fetch submodules"
        state = "failure"
    elif "build_log.txt" not in test_output_files:
        description = "Cannot finish cmake"
        state = "failure"
    elif "install_log.txt" not in test_output_files:
        description = "Cannot build ClickHouse"
        state = "failure"
    elif not test_log_exists and not test_result_exists:
        description = "Cannot install or start ClickHouse"
        state = "failure"
    else:
        state, description, test_results = process_results(output_path)

    if timeout_expired:
        test_results.append(TestResult.create_check_timeout_expired(args.timeout))
        state = "failure"
        description = test_results[-1].name

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_logs,
        build_dir_for_upload=str(output_path / "binaries"),
    ).dump()

    # Refuse other checks to run if fast test failed
    if state != "success":
        if state == "error":
            print("The status is 'error', report failure disregard the labels")
            sys.exit(1)
        elif FORCE_TESTS_LABEL in pr_info.labels:
            print(f"'{FORCE_TESTS_LABEL}' enabled, reporting success")
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
