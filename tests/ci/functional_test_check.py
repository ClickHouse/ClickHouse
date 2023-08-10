#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import re
import subprocess
import sys
import atexit
from pathlib import Path
from typing import List, Tuple

from github import Github

from build_download_helper import download_all_deb_packages
from clickhouse_helper import (
    ClickHouseHelper,
    prepare_tests_results_for_clickhouse,
)
from commit_status_helper import (
    NotSet,
    RerunHelper,
    get_commit,
    override_status,
    post_commit_status,
    post_commit_status_to_file,
    update_mergeable_check,
)
from docker_pull_helper import get_image_with_version
from download_release_packages import download_last_release
from env_helper import TEMP_PATH, REPO_COPY, REPORTS_PATH
from get_robot_token import get_best_robot_token
from pr_info import FORCE_TESTS_LABEL, PRInfo
from report import TestResults, read_test_results
from s3_helper import S3Helper
from stopwatch import Stopwatch
from tee_popen import TeePopen
from upload_result_helper import upload_results

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


def get_image_name(check_name):
    if "stateless" in check_name.lower():
        return "clickhouse/stateless-test"
    if "stateful" in check_name.lower():
        return "clickhouse/stateful-test"
    else:
        raise Exception(f"Cannot deduce image name based on check name {check_name}")


def get_run_command(
    check_name,
    builds_path,
    repo_tests_path,
    result_path,
    server_log_path,
    kill_timeout,
    additional_envs,
    image,
    flaky_check,
    tests_to_run,
):
    additional_options = ["--hung-check"]
    additional_options.append("--print-time")

    if tests_to_run:
        additional_options += tests_to_run

    additional_options_str = (
        '-e ADDITIONAL_OPTIONS="' + " ".join(additional_options) + '"'
    )

    envs = [
        f"-e MAX_RUN_TIME={int(0.9 * kill_timeout)}",
        # a static link, don't use S3_URL or S3_DOWNLOAD
        '-e S3_URL="https://s3.amazonaws.com/clickhouse-datasets"',
    ]

    if flaky_check:
        envs += ["-e NUM_TRIES=100", "-e MAX_RUN_TIME=1800"]

    envs += [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)
    volume_with_broken_test = (
        f"--volume={repo_tests_path}/analyzer_tech_debt.txt:/analyzer_tech_debt.txt"
        if "analyzer" in check_name
        else ""
    )

    return (
        f"docker run --volume={builds_path}:/package_folder "
        f"--volume={repo_tests_path}:/usr/share/clickhouse-test "
        f"{volume_with_broken_test} "
        f"--volume={result_path}:/test_output --volume={server_log_path}:/var/log/clickhouse-server "
        f"--cap-add=SYS_PTRACE {env_str} {additional_options_str} {image}"
    )


def get_tests_to_run(pr_info):
    result = set()

    if pr_info.changed_files is None:
        return []

    for fpath in pr_info.changed_files:
        if re.match(r"tests/queries/0_stateless/[0-9]{5}", fpath):
            logging.info("File '%s' is changed and seems like a test", fpath)
            fname = fpath.split("/")[3]
            fname_without_ext = os.path.splitext(fname)[0]
            # add '.' to the end of the test name not to run all tests with the same prefix
            # e.g. we changed '00001_some_name.reference'
            # and we have ['00001_some_name.sh', '00001_some_name_2.sql']
            # so we want to run only '00001_some_name.sh'
            result.add(fname_without_ext + ".")
        elif "tests/queries/" in fpath:
            # log suspicious changes from tests/ for debugging in case of any problems
            logging.info("File '%s' is changed, but it doesn't look like a test", fpath)
    return list(result)


def process_results(
    result_folder: str,
    server_log_path: str,
) -> Tuple[str, str, TestResults, List[str]]:
    test_results = []  # type: TestResults
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

    if os.path.exists(server_log_path):
        server_log_files = [
            f
            for f in os.listdir(server_log_path)
            if os.path.isfile(os.path.join(server_log_path, f))
        ]
        additional_files = additional_files + [
            os.path.join(server_log_path, f) for f in server_log_files
        ]

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

    try:
        results_path = Path(result_folder) / "test_results.tsv"

        if results_path.exists():
            logging.info("Found test_results.tsv")
        else:
            logging.info("Files in result folder %s", os.listdir(result_folder))
            return "error", "Not found test_results.tsv", test_results, additional_files

        test_results = read_test_results(results_path)
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


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = TEMP_PATH
    repo_path = REPO_COPY
    reports_path = REPORTS_PATH
    post_commit_path = os.path.join(temp_path, "functional_commit_status.tsv")

    args = parse_args()
    check_name = args.check_name
    kill_timeout = args.kill_timeout
    validate_bugfix_check = args.validate_bugfix

    flaky_check = "flaky" in check_name.lower()

    run_changed_tests = flaky_check or validate_bugfix_check
    gh = Github(get_best_robot_token(), per_page=100)

    # For validate_bugfix_check we need up to date information about labels, so pr_event_from_api is used
    pr_info = PRInfo(
        need_changed_files=run_changed_tests, pr_event_from_api=validate_bugfix_check
    )

    commit = get_commit(gh, pr_info.sha)
    atexit.register(update_mergeable_check, gh, pr_info, check_name)

    if not os.path.exists(temp_path):
        os.makedirs(temp_path)

    if validate_bugfix_check and "pr-bugfix" not in pr_info.labels:
        if args.post_commit_status == "file":
            post_commit_status_to_file(
                post_commit_path,
                f"Skipped (no pr-bugfix in {pr_info.labels})",
                "success",
                "null",
            )
        logging.info("Skipping '%s' (no pr-bugfix in %s)", check_name, pr_info.labels)
        sys.exit(0)

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

    tests_to_run = []
    if run_changed_tests:
        tests_to_run = get_tests_to_run(pr_info)
        if not tests_to_run:
            state = override_status("success", check_name, validate_bugfix_check)
            if args.post_commit_status == "commit_status":
                post_commit_status(
                    commit,
                    state,
                    NotSet,
                    NO_CHANGES_MSG,
                    check_name_with_group,
                    pr_info,
                )
            elif args.post_commit_status == "file":
                post_commit_status_to_file(
                    post_commit_path,
                    description=NO_CHANGES_MSG,
                    state=state,
                    report_url="null",
                )
            sys.exit(0)

    image_name = get_image_name(check_name)
    docker_image = get_image_with_version(reports_path, image_name)

    repo_tests_path = os.path.join(repo_path, "tests")

    packages_path = os.path.join(temp_path, "packages")
    if not os.path.exists(packages_path):
        os.makedirs(packages_path)

    if validate_bugfix_check:
        download_last_release(packages_path)
    else:
        download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = os.path.join(temp_path, "server_log")
    if not os.path.exists(server_log_path):
        os.makedirs(server_log_path)

    result_path = os.path.join(temp_path, "result_path")
    if not os.path.exists(result_path):
        os.makedirs(result_path)

    run_log_path = os.path.join(result_path, "run.log")

    additional_envs = get_additional_envs(
        check_name, run_by_hash_num, run_by_hash_total
    )
    if validate_bugfix_check:
        additional_envs.append("GLOBAL_TAGS=no-random-settings")

    run_command = get_run_command(
        check_name,
        packages_path,
        repo_tests_path,
        result_path,
        server_log_path,
        kill_timeout,
        additional_envs,
        docker_image,
        flaky_check,
        tests_to_run,
    )
    logging.info("Going to run func tests: %s", run_command)

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

    s3_helper = S3Helper()

    state, description, test_results, additional_logs = process_results(
        result_path, server_log_path
    )
    state = override_status(state, check_name, invert=validate_bugfix_check)

    ch_helper = ClickHouseHelper()

    report_url = upload_results(
        s3_helper,
        pr_info.number,
        pr_info.sha,
        test_results,
        [run_log_path] + additional_logs,
        check_name_with_group,
    )

    print(f"::notice:: {check_name} Report url: {report_url}")
    if args.post_commit_status == "commit_status":
        post_commit_status(
            commit, state, report_url, description, check_name_with_group, pr_info
        )
    elif args.post_commit_status == "file":
        post_commit_status_to_file(
            post_commit_path,
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

    if state != "success":
        if FORCE_TESTS_LABEL in pr_info.labels:
            print(f"'{FORCE_TESTS_LABEL}' enabled, will report success")
        else:
            sys.exit(1)


if __name__ == "__main__":
    main()
