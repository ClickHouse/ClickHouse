#!/usr/bin/env python3

import argparse
import csv
import logging
import os
import re
import signal
import subprocess
import sys
from pathlib import Path
from typing import List, Optional, Tuple

from build_download_helper import download_all_deb_packages
from ci_config import CI
from ci_utils import Shell, Utils
from clickhouse_helper import CiLogsCredentials
from docker_images_helper import DockerImage, get_docker_image, pull_image
from download_release_packages import download_last_release
from env_helper import REPO_COPY, REPORT_PATH, TEMP_PATH
from get_robot_token import get_parameter_from_ssm
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
from tee_popen import TeePopen

NO_CHANGES_MSG = "Nothing to run"


class SensitiveFormatter(logging.Formatter):
    @staticmethod
    def _filter(s):
        return re.sub(
            r"(.*)(AZURE_CONNECTION_STRING.*\')(.*)", r"\1AZURE_CONNECTION_STRING\3", s
        )

    def format(self, record):
        original = logging.Formatter.format(self, record)
        return self._filter(original)


def get_additional_envs(
    check_name: str, run_by_hash_num: int, run_by_hash_total: int
) -> List[str]:
    result = []
    azure_connection_string = get_parameter_from_ssm("azure_connection_string")
    result.append(f"AZURE_CONNECTION_STRING='{azure_connection_string}'")
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
        result.append("RANDOMIZE_OBJECT_KEY_TYPE=1")
    if "analyzer" in check_name:
        result.append("USE_OLD_ANALYZER=1")
    if "azure" in check_name:
        assert "USE_S3_STORAGE_FOR_MERGE_TREE=1" not in result
        result.append("USE_AZURE_STORAGE_FOR_MERGE_TREE=1")

    if run_by_hash_total != 0:
        result.append(f"RUN_BY_HASH_NUM={run_by_hash_num}")
        result.append(f"RUN_BY_HASH_TOTAL={run_by_hash_total}")

    return result


def get_image_name(check_name: str) -> str:
    if "stateless" in check_name.lower():
        return "clickhouse/stateless-test"
    if "stateful" in check_name.lower():
        return "clickhouse/stateful-test"
    raise ValueError(f"Cannot deduce image name based on check name {check_name}")


def get_run_command(
    check_name: str,
    builds_path: Path,
    repo_path: Path,
    result_path: Path,
    server_log_path: Path,
    additional_envs: List[str],
    ci_logs_args: str,
    image: DockerImage,
    flaky_check: bool,
    tests_to_run: List[str],
) -> str:
    additional_options = ["--hung-check"]
    additional_options.append("--print-time")

    if tests_to_run:
        additional_options += tests_to_run

    additional_options_str = (
        '-e ADDITIONAL_OPTIONS="' + " ".join(additional_options) + '"'
    )

    envs = [
        # a static link, don't use S3_URL or S3_DOWNLOAD
        '-e S3_URL="https://s3.amazonaws.com/clickhouse-datasets"',
    ]

    if flaky_check:
        envs.append("-e NUM_TRIES=50")

    envs += [f"-e {e}" for e in additional_envs]

    env_str = " ".join(envs)

    if "stateful" in check_name.lower():
        run_script = "/repo/tests/docker_scripts/stateful_runner.sh"
    elif "stateless" in check_name.lower():
        run_script = "/repo/tests/docker_scripts/stateless_runner.sh"
    else:
        assert False

    return (
        f"docker run --rm --name func-tester --volume={builds_path}:/package_folder "
        # For dmesg and sysctl
        "--privileged "
        f"{ci_logs_args} "
        f"--volume={repo_path}:/repo "
        f"--volume={result_path}:/test_output "
        f"--volume={server_log_path}:/var/log/clickhouse-server "
        "--security-opt seccomp=unconfined "  # required to issue io_uring sys-calls
        f"--cap-add=SYS_PTRACE {env_str} {additional_options_str} {image} {run_script}"
    )


def _get_statless_tests_to_run(pr_info: PRInfo) -> List[str]:
    result = set()

    if pr_info.changed_files is None:
        return []

    for fpath in pr_info.changed_files:
        if re.match(r"tests/queries/0_stateless/[0-9]{5}", fpath):
            path_ = Path(REPO_COPY + "/" + fpath)
            if not path_.exists():
                logging.info("File '%s' is removed - skip", fpath)
                continue
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
    ret_code: int,
    result_directory: Path,
    server_log_path: Path,
) -> Tuple[StatusType, str, TestResults, List[Path]]:
    test_results = []  # type: TestResults
    additional_files = []
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
        logging.info("Found %s", status_path.name)
        with open(status_path, "r", encoding="utf-8") as status_file:
            status = list(csv.reader(status_file, delimiter="\t"))

    if len(status) != 1 or len(status[0]) != 2:
        logging.info("Files in result folder %s", os.listdir(result_directory))
        return ERROR, "Invalid check_status.tsv", test_results, additional_files
    state, description = status[0][0], status[0][1]
    if ret_code != 0:
        state = ERROR
        description = f"Job failed, exit code: {ret_code}. " + description

    try:
        results_path = result_directory / "test_results.tsv"

        if results_path.exists():
            logging.info("Found test_results.tsv")
        else:
            logging.info("Files in result folder %s", os.listdir(result_directory))
            return ERROR, "Not found test_results.tsv", test_results, additional_files

        test_results = read_test_results(results_path)
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


test_process = None  # type: Optional[TeePopen]
timeout_expired = False


def handle_sigterm(signum, _frame):
    # TODO: think on how to process it without globals
    print(f"WARNING: Received signal {signum}")
    global timeout_expired  # pylint:disable=global-statement
    timeout_expired = True
    Shell.check("docker exec func-tester pkill -f clickhouse-test", verbose=True)


def main():
    signal.signal(signal.SIGTERM, handle_sigterm)
    logging.basicConfig(level=logging.INFO)
    for handler in logging.root.handlers:
        # pylint: disable=protected-access
        handler.setFormatter(SensitiveFormatter(handler.formatter._fmt))  # type: ignore

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    reports_path = Path(REPORT_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    reports_path.mkdir(parents=True, exist_ok=True)

    repo_path = Path(REPO_COPY)

    args = parse_args()
    check_name = args.check_name or os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"
    validate_bugfix_check = args.validate_bugfix

    flaky_check = "flaky" in check_name.lower()

    run_changed_tests = flaky_check or validate_bugfix_check
    pr_info = PRInfo(need_changed_files=run_changed_tests)
    tests_to_run = []
    assert (
        not validate_bugfix_check or args.report_to_file
    ), "JobReport file path must be provided with --validate-bugfix"
    if run_changed_tests:
        tests_to_run = _get_statless_tests_to_run(pr_info)

    if "RUN_BY_HASH_NUM" in os.environ:
        run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
    else:
        run_by_hash_num = 0
        run_by_hash_total = 0

    image_name = get_image_name(check_name)

    docker_image = pull_image(get_docker_image(image_name))

    packages_path = temp_path / "packages"
    packages_path.mkdir(parents=True, exist_ok=True)

    if validate_bugfix_check:
        download_last_release(packages_path, debug=True)
    else:
        download_all_deb_packages(check_name, reports_path, packages_path)

    server_log_path = temp_path / "server_log"
    server_log_path.mkdir(parents=True, exist_ok=True)

    result_path = temp_path / "result_path"
    result_path.mkdir(parents=True, exist_ok=True)

    run_log_path = result_path / "run.log"

    additional_envs = get_additional_envs(
        check_name, run_by_hash_num, run_by_hash_total
    )
    if validate_bugfix_check:
        additional_envs.append("GLOBAL_TAGS=no-random-settings")
        additional_envs.append("BUGFIX_VALIDATE_CHECK=1")

    ci_logs_credentials = CiLogsCredentials(temp_path / "export-logs-config.sh")
    ci_logs_args = ci_logs_credentials.get_docker_arguments(
        pr_info, stopwatch.start_time_str, check_name
    )

    if (not validate_bugfix_check and not flaky_check) or tests_to_run:
        run_command = get_run_command(
            check_name,
            packages_path,
            repo_path,
            result_path,
            server_log_path,
            additional_envs,
            ci_logs_args,
            docker_image,
            flaky_check,
            tests_to_run,
        )
        logging.info("Going to run func tests: %s", run_command)

        with TeePopen(run_command, run_log_path) as process:
            global test_process  # pylint:disable=global-statement
            test_process = process
            retcode = process.wait()
            if retcode == 0:
                logging.info("Run successfully")
            else:
                logging.info("Run failed, exit code %s", retcode)

        try:
            subprocess.check_call(
                f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True
            )
        except subprocess.CalledProcessError:
            logging.warning(
                "Failed to change files owner in %s, ignoring it", temp_path
            )

        ci_logs_credentials.clean_ci_logs_from_credentials(run_log_path)

        state, description, test_results, additional_logs = process_results(
            retcode, result_path, server_log_path
        )
        if timeout_expired:
            description = "Timeout expired"
            state = FAILURE
            test_results.insert(
                0, TestResult.create_check_timeout_expired(stopwatch.duration_seconds)
            )

    else:
        print(
            "This is validate bugfix or flaky check run, but no changes test to run - skip with success"
        )
        state, description, test_results, additional_logs = (
            SUCCESS,
            "No tests to run",
            [],
            [],
        )

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
