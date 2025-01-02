#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import os
import sys
from pathlib import Path
from typing import Dict, List, Tuple

import integration_tests_runner as runner
from build_download_helper import download_all_deb_packages
from ci_config import CI
from ci_utils import Utils
from docker_images_helper import DockerImage, get_docker_image
from download_release_packages import download_last_release
from env_helper import REPO_COPY, REPORT_PATH, TEMP_PATH
from integration_test_images import IMAGES
from pr_info import PRInfo
from report import (
    ERROR,
    FAILURE,
    SUCCESS,
    JobReport,
    StatusType,
    TestResult,
    TestResults,
    read_test_results,
)
from stopwatch import Stopwatch


def get_json_params_dict(
    check_name: str,
    pr_info: PRInfo,
    docker_images: List[DockerImage],
    run_by_hash_total: int,
    run_by_hash_num: int,
) -> dict:
    return {
        "context_name": check_name,
        "commit": pr_info.sha,
        "pull_request": pr_info.number,
        "pr_info": {"changed_files": list(pr_info.changed_files)},
        "docker_images_with_versions": {d.name: d.version for d in docker_images},
        "shuffle_test_groups": False,
        "use_tmpfs": False,
        "disable_net_host": True,
        "run_by_hash_total": run_by_hash_total,
        "run_by_hash_num": run_by_hash_num,
    }


def get_env_for_runner(
    check_name: str,
    build_path: Path,
    repo_path: Path,
    result_path: Path,
    work_path: Path,
) -> Dict[str, str]:
    binary_path = build_path / "clickhouse"
    odbc_bridge_path = build_path / "clickhouse-odbc-bridge"
    library_bridge_path = build_path / "clickhouse-library-bridge"

    my_env = os.environ.copy()
    my_env["CLICKHOUSE_TESTS_BUILD_PATH"] = build_path.as_posix()
    my_env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = binary_path.as_posix()
    my_env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = binary_path.as_posix()
    my_env["CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH"] = odbc_bridge_path.as_posix()
    my_env["CLICKHOUSE_TESTS_LIBRARY_BRIDGE_BIN_PATH"] = library_bridge_path.as_posix()
    my_env["CLICKHOUSE_TESTS_REPO_PATH"] = repo_path.as_posix()
    my_env["CLICKHOUSE_TESTS_RESULT_PATH"] = result_path.as_posix()
    my_env["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = f"{repo_path}/programs/server"
    my_env["CLICKHOUSE_TESTS_JSON_PARAMS_PATH"] = f"{work_path}/params.json"
    my_env["CLICKHOUSE_TESTS_RUNNER_RESTART_DOCKER"] = "0"

    if "analyzer" in check_name.lower():
        my_env["CLICKHOUSE_USE_OLD_ANALYZER"] = "1"

    return my_env


def process_results(
    result_directory: Path,
) -> Tuple[StatusType, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []
    # Just upload all files from result_directory.
    # If task provides processed results, then it's responsible for content of
    # result_directory.
    if result_directory.exists():
        additional_files = [p for p in result_directory.iterdir() if p.is_file()]

    status = []
    status_path = result_directory / "check_status.tsv"
    if status_path.exists():
        logging.info("Found %s", status_path.name)
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path, False)
        if len(test_results) == 0:
            return ERROR, "Empty test_results.tsv", test_results, additional_files
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
    parser.add_argument(
        "--run-tests", nargs="*", help="List of tests to run", default=None
    )
    parser.add_argument(
        "--validate-bugfix",
        action="store_true",
        help="Check that added tests failed on latest stable",
    )
    parser.add_argument(
        "--report-to-file",
        type=str,
        default="",
        help="Path to write script report to (for --validate-bugfix)",
    )
    return parser.parse_args()


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    repo_path = Path(REPO_COPY)

    args = parse_args()
    check_name = args.check_name or os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided in --check-name input option or in CHECK_NAME env"
    validate_bugfix_check = args.validate_bugfix

    if "RUN_BY_HASH_NUM" in os.environ:
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
    else:
        run_by_hash_num = 0
        run_by_hash_total = 0

    is_flaky_check = "flaky" in check_name

    assert (
        not validate_bugfix_check or args.report_to_file
    ), "--report-to-file must be provided for --validate-bugfix"

    # For validate_bugfix_check we need up to date information about labels, so
    # pr_event_from_api is used
    pr_info = PRInfo(need_changed_files=is_flaky_check or validate_bugfix_check)

    images = [get_docker_image(image_) for image_ in IMAGES]

    result_path = temp_path / "output_dir"
    result_path.mkdir(parents=True, exist_ok=True)

    work_path = temp_path / "workdir"
    work_path.mkdir(parents=True, exist_ok=True)

    build_path = temp_path / "build"
    build_path.mkdir(parents=True, exist_ok=True)

    if validate_bugfix_check:
        download_last_release(build_path, debug=True)
    else:
        download_all_deb_packages(check_name, reports_path, build_path)

    my_env = get_env_for_runner(
        check_name, build_path, repo_path, result_path, work_path
    )

    json_path = work_path / "params.json"
    with open(json_path, "w", encoding="utf-8") as json_params:
        params_text = json.dumps(
            get_json_params_dict(
                check_name,
                pr_info,
                images,
                run_by_hash_total,
                run_by_hash_num,
            )
        )
        json_params.write(params_text)
        logging.info("Parameters file %s is written: %s", json_path, params_text)

    for k, v in my_env.items():
        os.environ[k] = v
    logging.info(
        "ENV parameters for runner:\n%s",
        "\n".join(
            [f"{k}={v}" for k, v in my_env.items() if k.startswith("CLICKHOUSE_")]
        ),
    )

    try:
        runner.run()
    except Exception as e:
        logging.error("Exception: %s", e)
        state, description, test_results, additional_logs = ERROR, "infrastructure error", [TestResult("infrastructure error", ERROR, stopwatch.duration_seconds)], []  # type: ignore
    else:
        state, description, test_results, additional_logs = process_results(result_path)

    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_logs,
    ).dump(to_file=args.report_to_file if args.report_to_file else None)

    should_block_ci = False
    if state != SUCCESS:
        should_block_ci = True

    if state == FAILURE and CI.is_required(check_name):
        failed_cnt = Utils.get_failed_tests_number(description)
        print(
            f"Job status is [{state}] with [{failed_cnt}] failed test cases. status description [{description}]"
        )
        if (
            failed_cnt
            and failed_cnt <= CI.MAX_TOTAL_FAILURES_PER_JOB_BEFORE_BLOCKING_CI
        ):
            print("Won't block the CI workflow")
            should_block_ci = False

    if should_block_ci:
        sys.exit(1)


if __name__ == "__main__":
    main()
