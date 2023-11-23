#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Dict, List, Tuple

from github import Github

from build_download_helper import download_all_deb_packages
from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    RerunHelper,
    get_commit,
    override_status,
    post_commit_status,
    post_commit_status_to_file,
)
from docker_pull_helper import get_images_with_versions, DockerImage
from download_release_packages import download_last_release
from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from report import TestResults, read_test_results
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results


# When update, update
# integration/ci-runner.py:ClickhouseIntegrationTestsRunner.get_images_names too
IMAGES = [
    "clickhouse/integration-tests-runner",
    "clickhouse/mysql-golang-client",
    "clickhouse/mysql-java-client",
    "clickhouse/mysql-js-client",
    "clickhouse/mysql-php-client",
    "clickhouse/postgresql-java-client",
    "clickhouse/integration-test",
    "clickhouse/kerberos-kdc",
    "clickhouse/kerberized-hadoop",
    "clickhouse/integration-helper",
    "clickhouse/dotnet-client",
]


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

    return my_env


def process_results(
    result_directory: Path,
) -> Tuple[str, str, TestResults, List[Path]]:
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
        logging.info("Found test_results.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    try:
        results_path = result_directory / "test_results.tsv"
        test_results = read_test_results(results_path, False)
        if len(test_results) == 0:
            return "error", "Empty test_results.tsv", test_results, additional_files
    except Exception as e:
        return (
            "error",
            f"Cannot parse test_results.tsv ({e})",
            test_results,
            additional_files,
        )

    return state, description, test_results, additional_files


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("check_name")
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


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    post_commit_path = temp_path / "integration_commit_status.tsv"
    repo_path = Path(REPO_COPY)
    reports_path = Path(REPORTS_PATH)

    args = parse_args()
    check_name = args.check_name
    validate_bugfix_check = args.validate_bugfix

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

    is_flaky_check = "flaky" in check_name

    # For validate_bugfix_check we need up to date information about labels, so
    # pr_event_from_api is used
    pr_info = PRInfo(
        need_changed_files=is_flaky_check or validate_bugfix_check,
        pr_event_from_api=validate_bugfix_check,
    )

    if validate_bugfix_check and "pr-bugfix" not in pr_info.labels:
        if args.post_commit_status == "file":
            post_commit_status_to_file(
                post_commit_path,
                f"Skipped (no pr-bugfix in {pr_info.labels})",
                "success",
                "null",
            )
        logging.info("Skipping '%s' (no pr-bugfix in '%s')", check_name, pr_info.labels)
        sys.exit(0)

    gh = Github(get_best_robot_token(), per_page=100)
    commit = get_commit(gh, pr_info.sha)

    rerun_helper = RerunHelper(commit, check_name_with_group)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    images = get_images_with_versions(reports_path, IMAGES)
    result_path = temp_path / "output_dir"
    result_path.mkdir(parents=True, exist_ok=True)

    work_path = temp_path / "workdir"
    work_path.mkdir(parents=True, exist_ok=True)

    build_path = temp_path / "build"
    build_path.mkdir(parents=True, exist_ok=True)

    if validate_bugfix_check:
        download_last_release(build_path)
    else:
        download_all_deb_packages(check_name, reports_path, build_path)

    my_env = get_env_for_runner(build_path, repo_path, result_path, work_path)

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

    output_path_log = result_path / "main_script_log.txt"

    runner_path = repo_path / "tests" / "integration" / "ci-runner.py"
    run_command = f"sudo -E {runner_path}"
    logging.info("Going to run command: `%s`", run_command)
    logging.info(
        "ENV parameters for runner:\n%s",
        "\n".join(
            [f"{k}={v}" for k, v in my_env.items() if k.startswith("CLICKHOUSE_")]
        ),
    )

    with TeePopen(run_command, output_path_log, my_env) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run tests successfully")
        elif retcode == 13:
            logging.warning(
                "There were issues with infrastructure. Not writing status report to restart job."
            )
            sys.exit(1)
        else:
            logging.info("Some tests failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    state, description, test_results, additional_logs = process_results(result_path)
    state = override_status(state, check_name, invert=validate_bugfix_check)

    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, check_name, test_results)

    s3_helper = S3Helper()
    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [output_path_log] + additional_logs,
        check_name_with_group,
    )

    print(f"::notice:: {check_name} Report url: {report_url}")
    if args.post_commit_status == "commit_status":
        post_commit_status(
            commit, state, report_url, description, check_name_with_group, pr_info
        )
    elif args.post_commit_status == "file":
        post_commit_status_to_file(post_commit_path, description, state, report_url)
    else:
        raise Exception(
            f'Unknown post_commit_status option "{args.post_commit_status}"'
        )

    prepared_events = prepare_tests_results_for_clickhouse(
        pr_info,
        test_results,
        state,
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        report_url,
        check_name_with_group,
    )

    ch_helper.insert_events_into(db="default", table="checks", events=prepared_events)

    if state == "failure":
        sys.exit(1)


if __name__ == "__main__":
    main()
