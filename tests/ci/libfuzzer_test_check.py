#!/usr/bin/env python3

import argparse
import logging
import os
import subprocess
import sys
import atexit
import zipfile
from pathlib import Path
from typing import List

from github import Github

from build_download_helper import download_fuzzers
from clickhouse_helper import (
    CiLogsCredentials,
)
from commit_status_helper import (
    RerunHelper,
    get_commit,
    update_mergeable_check,
)
from docker_pull_helper import DockerImage, get_image_with_version

from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults

from stopwatch import Stopwatch

from tee_popen import TeePopen


NO_CHANGES_MSG = "Nothing to run"


def get_additional_envs(check_name, run_by_hash_num, run_by_hash_total):
    result = []
    if "DatabaseReplicated" in check_name:
        result.append("USE_DATABASE_REPLICATED=1")
    if "DatabaseOrdinary" in check_name:
        result.append("USE_DATABASE_ORDINARY=1")
    if "wide parts enabled" in check_name:
        result.append("USE_POLYMORPHIC_PARTS=1")
    if "ParallelReplicas" in check_name:
        result.append("USE_PARALLEL_REPLICAS=1")
    if "s3 storage" in check_name:
        result.append("USE_S3_STORAGE_FOR_MERGE_TREE=1")
    if "analyzer" in check_name:
        result.append("USE_NEW_ANALYZER=1")

    if run_by_hash_total != 0:
        result.append(f"RUN_BY_HASH_NUM={run_by_hash_num}")
        result.append(f"RUN_BY_HASH_TOTAL={run_by_hash_total}")

    return result


def get_run_command(
    fuzzers_path: Path,
    repo_path: Path,
    result_path: Path,
    kill_timeout: int,
    additional_envs: List[str],
    ci_logs_args: str,
    image: DockerImage,
) -> str:
    additional_options = ["--hung-check"]
    additional_options.append("--print-time")

    additional_options_str = (
        '-e ADDITIONAL_OPTIONS="' + " ".join(additional_options) + '"'
    )

    envs = [
        f"-e MAX_RUN_TIME={int(0.9 * kill_timeout)}",
        # a static link, don't use S3_URL or S3_DOWNLOAD
        '-e S3_URL="https://s3.amazonaws.com/clickhouse-datasets"',
    ]

    envs += [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)

    return (
        f"docker run "
        f"{ci_logs_args} "
        f"--workdir=/fuzzers "
        f"--volume={fuzzers_path}:/fuzzers "
        f"--volume={repo_path}/tests:/usr/share/clickhouse-test "
        f"--volume={result_path}:/test_output "
        f"--cap-add=SYS_PTRACE {env_str} {additional_options_str} {image}"
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
    parser.add_argument("kill_timeout", type=int)
    parser.add_argument(
        "--validate-bugfix",
        action="store_true",
        help="Check that added tests failed on latest stable",
    )
    parser.add_argument(
        "--post-commit-status",
        default="commit_status",
        choices=["commit_status", "file"],
        help="Where to public post commit status",
    )
    return parser.parse_args()


def docker_build_image(image_name: str, filepath: Path) -> DockerImage:
    # context = filepath.parent
    docker_image = DockerImage(image_name)
    build_cmd = f"docker build --network=host -t {image_name} {filepath}"
    logging.info("Will build image with cmd: '%s'", build_cmd)
    subprocess.check_call(
        build_cmd,
        shell=True,
    )
    return docker_image


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    reports_path = REPORTS_PATH

    args = parse_args()
    check_name = args.check_name
    kill_timeout = args.kill_timeout
    validate_bugfix_check = args.validate_bugfix

    run_changed_tests = validate_bugfix_check
    gh = Github(get_best_robot_token(), per_page=100)

    # For validate_bugfix_check we need up to date information about labels, so pr_event_from_api is used
    pr_info = PRInfo(
        need_changed_files=run_changed_tests, pr_event_from_api=validate_bugfix_check
    )

    commit = get_commit(gh, pr_info.sha)
    atexit.register(update_mergeable_check, gh, pr_info, check_name)

    if not Path(temp_path).exists():
        os.makedirs(temp_path)

    if "RUN_BY_HASH_NUM" in os.environ:
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        run_by_hash_num = 0
        run_by_hash_total = 0
        check_name_with_group = check_name

    rerun_helper = RerunHelper(commit, check_name_with_group)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    docker_image = get_image_with_version(reports_path, "clickhouse/libfuzzer")

    fuzzers_path = Path(temp_path) / "fuzzers"
    if not fuzzers_path.exists():
        os.makedirs(fuzzers_path)

    download_fuzzers(check_name, reports_path, fuzzers_path)

    for file in os.listdir(fuzzers_path):
        if file.endswith("_fuzzer"):
            os.chmod(fuzzers_path / file, 0o777)
        elif file.endswith("_seed_corpus.zip"):
            corpus_path = fuzzers_path / (file.removesuffix("_seed_corpus.zip") + ".in")
            zipfile.ZipFile(fuzzers_path / file, "r").extractall(corpus_path)

    result_path = Path(temp_path) / "result_path"
    if not result_path.exists():
        os.makedirs(result_path)

    run_log_path = result_path / "run.log"

    additional_envs = get_additional_envs(
        check_name, run_by_hash_num, run_by_hash_total
    )

    ci_logs_credentials = CiLogsCredentials(Path(temp_path) / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    run_command = get_run_command(
        fuzzers_path,
        Path(repo_path),
        result_path,
        kill_timeout,
        additional_envs,
        ci_logs_args,
        docker_image,
    )
    logging.info("Going to run libFuzzer tests: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    sys.exit(0)


if __name__ == "__main__":
    main()
