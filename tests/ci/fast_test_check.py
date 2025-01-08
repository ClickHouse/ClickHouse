#!/usr/bin/env python3
import csv
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Tuple

from docker_images_helper import DockerImage, get_docker_image, pull_image
from env_helper import REPO_COPY, S3_BUILDS_BUCKET, TEMP_PATH
from pr_info import PRInfo
from report import ERROR, FAILURE, SUCCESS, JobReport, TestResults, read_test_results
from stopwatch import Stopwatch
from tee_popen import TeePopen

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
        "--security-opt seccomp=unconfined "  # required to issue io_uring sys-calls
        "--network=host "  # required to get access to IAM credentials
        f"-e FASTTEST_WORKSPACE=/fasttest-workspace -e FASTTEST_OUTPUT=/test_output "
        f"-e FASTTEST_SOURCE=/repo "
        f"-e FASTTEST_CMAKE_FLAGS='-DCOMPILER_CACHE=sccache' "
        f"-e PULL_REQUEST_NUMBER={pr_number} -e COMMIT_SHA={commit_sha} "
        f"-e COPY_CLICKHOUSE_BINARY_TO_OUTPUT=1 "
        f"-e SCCACHE_BUCKET={S3_BUILDS_BUCKET} -e SCCACHE_S3_KEY_PREFIX=ccache/sccache "
        "-e stage=clone_submodules "
        f"--volume={workspace}:/fasttest-workspace --volume={repo_path}:/repo "
        f"--volume={output_path}:/test_output {image} /repo/tests/docker_scripts/fasttest_runner.sh"
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
        return ERROR, "Invalid check_status.tsv", test_results
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path)
        if len(test_results) == 0:
            return ERROR, "Empty test_results.tsv", test_results
    except Exception as e:
        return (ERROR, f"Cannot parse test_results.tsv ({e})", test_results)

    return state, description, test_results


def main():
    logging.basicConfig(level=logging.INFO)
    stopwatch = Stopwatch()

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

    with TeePopen(run_cmd, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
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
        state = FAILURE
    elif "cmake_log.txt" not in test_output_files:
        description = "Cannot fetch submodules"
        state = FAILURE
    elif "build_log.txt" not in test_output_files:
        description = "Cannot finish cmake"
        state = FAILURE
    elif "install_log.txt" not in test_output_files:
        description = "Cannot build ClickHouse"
        state = FAILURE
    elif not test_log_exists and not test_result_exists:
        description = "Cannot install or start ClickHouse"
        state = FAILURE
    else:
        state, description, test_results = process_results(output_path)

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
    if state != SUCCESS:
        sys.exit(1)


if __name__ == "__main__":
    main()
