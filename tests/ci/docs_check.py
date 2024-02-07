#!/usr/bin/env python3
import argparse
import logging
import subprocess
import sys
from pathlib import Path

from docker_images_helper import get_docker_image, pull_image
from env_helper import REPO_COPY, TEMP_PATH
from pr_info import PRInfo
from report import FAILURE, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description="Script to check the docs integrity",
    )
    parser.add_argument(
        "--docs-branch",
        default="",
        help="a branch to get from docs repository",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="check the docs even if there no changes",
    )
    return parser.parse_args()


def main():
    args = parse_args()

    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)
    repo_path = Path(REPO_COPY)

    pr_info = PRInfo(need_changed_files=True)

    if not pr_info.has_changes_in_documentation() and not args.force:
        logging.info("No changes in documentation")
        JobReport(
            description="No changes in docs",
            test_results=[],
            status=SUCCESS,
            start_time=stopwatch.start_time_str,
            duration=stopwatch.duration_seconds,
            additional_files=[],
        ).dump()
        sys.exit(0)

    if pr_info.has_changes_in_documentation():
        logging.info("Has changes in docs")
    elif args.force:
        logging.info("Check the docs because of force flag")

    docker_image = pull_image(get_docker_image("clickhouse/docs-builder"))

    test_output = temp_path / "docs_check_log"
    test_output.mkdir(parents=True, exist_ok=True)

    cmd = (
        f"docker run --cap-add=SYS_PTRACE -e GIT_DOCS_BRANCH={args.docs_branch} "
        f"--volume={repo_path}:/ClickHouse --volume={test_output}:/output_path "
        f"{docker_image}"
    )

    run_log_path = test_output / "run.log"
    logging.info("Running command: '%s'", cmd)

    with TeePopen(cmd, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            status = SUCCESS
            description = "Docs check passed"
        else:
            description = "Docs check failed (non zero exit code)"
            status = FAILURE
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {temp_path}", shell=True)
    test_results = []  # type: TestResults
    additional_files = []
    if not any(test_output.iterdir()):
        logging.error("No output files after docs check")
        description = "No output files after docs check"
        status = FAILURE
    else:
        for p in test_output.iterdir():
            additional_files.append(p)
            with open(p, "r", encoding="utf-8") as check_file:
                for line in check_file:
                    if "ERROR" in line:
                        test_results.append(TestResult(line.split(":")[-1], "FAIL"))
        if test_results:
            status = FAILURE
            description = "Found errors in docs"
        elif status != FAILURE:
            test_results.append(TestResult("No errors found", "OK"))
        else:
            test_results.append(TestResult("Non zero exit code", "FAIL"))

    JobReport(
        description=description,
        test_results=test_results,
        status=status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_files,
    ).dump()

    if status == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
