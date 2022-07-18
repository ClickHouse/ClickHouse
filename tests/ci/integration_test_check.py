#!/usr/bin/env python3

import argparse
import csv
import json
import logging
import os
import subprocess
import sys

from github import Github

from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from s3_helper import S3Helper
from get_robot_token import get_best_robot_token
from pr_info import PRInfo
from build_download_helper import download_all_deb_packages
from download_previous_release import download_previous_release
from upload_result_helper import upload_results
from docker_pull_helper import get_images_with_versions
from commit_status_helper import (
    post_commit_status,
    override_status,
    post_commit_status_to_file,
)
from clickhouse_helper import (
    ClickHouseHelper,
    mark_flaky_tests,
    prepare_tests_results_for_clickhouse,
)
from stopwatch import Stopwatch
from rerun_helper import RerunHelper
from tee_popen import TeePopen


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
    check_name, pr_info, docker_images, run_by_hash_total, run_by_hash_num
):
    return {
        "context_name": check_name,
        "commit": pr_info.sha,
        "pull_request": pr_info.number,
        "pr_info": {"changed_files": list(pr_info.changed_files)},
        "docker_images_with_versions": docker_images,
        "shuffle_test_groups": False,
        "use_tmpfs": False,
        "disable_net_host": True,
        "run_by_hash_total": run_by_hash_total,
        "run_by_hash_num": run_by_hash_num,
    }


def get_env_for_runner(build_path, repo_path, result_path, work_path):
    binary_path = os.path.join(build_path, "clickhouse")
    odbc_bridge_path = os.path.join(build_path, "clickhouse-odbc-bridge")
    library_bridge_path = os.path.join(build_path, "clickhouse-library-bridge")

    my_env = os.environ.copy()
    my_env["CLICKHOUSE_TESTS_BUILD_PATH"] = build_path
    my_env["CLICKHOUSE_TESTS_SERVER_BIN_PATH"] = binary_path
    my_env["CLICKHOUSE_TESTS_CLIENT_BIN_PATH"] = binary_path
    my_env["CLICKHOUSE_TESTS_ODBC_BRIDGE_BIN_PATH"] = odbc_bridge_path
    my_env["CLICKHOUSE_TESTS_LIBRARY_BRIDGE_BIN_PATH"] = library_bridge_path
    my_env["CLICKHOUSE_TESTS_REPO_PATH"] = repo_path
    my_env["CLICKHOUSE_TESTS_RESULT_PATH"] = result_path
    my_env["CLICKHOUSE_TESTS_BASE_CONFIG_DIR"] = f"{repo_path}/programs/server"
    my_env["CLICKHOUSE_TESTS_JSON_PARAMS_PATH"] = os.path.join(work_path, "params.json")
    my_env["CLICKHOUSE_TESTS_RUNNER_RESTART_DOCKER"] = "0"

    return my_env


def process_results(result_folder):
    test_results = []
    additional_files = []
    # Just upload all files from result_folder.
    # If task provides processed results, then it's responsible for content of result_folder.
    if os.path.exists(result_folder):
        test_files = [
            f
            for f in os.listdir(result_folder)
            if os.path.isfile(os.path.join(result_folder, f))
        ]
        additional_files = [os.path.join(result_folder, f) for f in test_files]

    status = []
    status_path = os.path.join(result_folder, "check_status.tsv")
    if os.path.exists(status_path):
        logging.info("Found test_results.tsv")
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_folder))
        return "error", "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]

    results_path = os.path.join(result_folder, "test_results.tsv")
    if os.path.exists(results_path):
        with open(results_path, "r", encoding="utf-8") as results_file:
            test_results = list(csv.reader(results_file, delimiter="\t"))
    if len(test_results) == 0:
        return "error", "Empty test_results.tsv", test_results, additional_files

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


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    reports_path = REPORTS_PATH

    args = parse_args()
    check_name = args.check_name
    validate_bugix_check = args.validate_bugfix

    if "RUN_BY_HASH_NUM" in os.environ:
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM"))
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL"))
        check_name_with_group = (
            check_name + f" [{run_by_hash_num + 1}/{run_by_hash_total}]"
        )
    else:
        run_by_hash_num = 0
        run_by_hash_total = 0
        check_name_with_group = check_name

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    is_flaky_check = "flaky" in check_name
    pr_info = PRInfo(need_changed_files=is_flaky_check or validate_bugix_check)

    if validate_bugix_check and "pr-bugfix" not in pr_info.labels:
        if args.post_commit_status == "file":
            post_commit_status_to_file(
                os.path.join(temp_path, "post_commit_status.tsv"),
                "Skipped (no pr-bugfix)",
                "success",
                "null",
            )
        logging.info("Skipping '%s' (no pr-bugfix)", check_name)
        sys.exit(0)

    gh = Github(get_best_robot_token())

    rerun_helper = RerunHelper(gh, pr_info, check_name_with_group)
    if rerun_helper.is_already_finished_by_status():
        logging.info("Check is already finished according to github status, exiting")
        sys.exit(0)

    images = get_images_with_versions(reports_path, IMAGES)
    images_with_versions = {i.name: i.version for i in images}
    result_path = os.path.join(temp_path, "output_dir")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    work_path = os.path.join(temp_path, "workdir")
    if not os.path.exists(work_path):
        os.makedirs(work_path)

    build_path = os.path.join(temp_path, "build")
    if not os.path.exists(build_path):
        os.makedirs(build_path)

    if validate_bugix_check:
        download_previous_release(build_path)
    else:
        download_all_deb_packages(check_name, reports_path, build_path)

    my_env = get_env_for_runner(build_path, repo_path, result_path, work_path)

    json_path = os.path.join(work_path, "params.json")
    with open(json_path, "w", encoding="utf-8") as json_params:
        params_text = json.dumps(
            get_json_params_dict(
                check_name,
                pr_info,
                images_with_versions,
                run_by_hash_total,
                run_by_hash_num,
            )
        )
        json_params.write(params_text)
        logging.info("Parameters file %s is written: %s", json_path, params_text)

    output_path_log = os.path.join(result_path, "main_script_log.txt")

    runner_path = os.path.join(repo_path, "tests/integration", "ci-runner.py")
    run_command = f"sudo -E {runner_path} | tee {output_path_log}"
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
        else:
            logging.info("Some tests failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)

    state, description, test_results, additional_logs = process_results(result_path)
    state = override_status(state, check_name, validate_bugix_check)

    ch_helper = ClickHouseHelper()
    mark_flaky_tests(ch_helper, check_name, test_results)

    s3_helper = S3Helper("https://s3.amazonaws.com")
    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [output_path_log] + additional_logs,
        check_name_with_group,
        False,
    )

    print(f"::notice:: {check_name} Report url: {report_url}")
    if args.post_commit_status == "commit_status":
        post_commit_status(
            gh, pr_info.sha, check_name_with_group, description, state, report_url
        )
    elif args.post_commit_status == "file":
        post_commit_status_to_file(
            os.path.join(temp_path, "post_commit_status.tsv"),
            description,
            state,
            report_url,
        )
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

    if state == "error":
        sys.exit(1)
