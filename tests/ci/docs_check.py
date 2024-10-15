#!/usr/bin/env python3
import argparse
import logging
import os
import sys
from pathlib import Path

from docker_images_helper import get_docker_image, pull_image
from env_helper import REPO_COPY, TEMP_PATH
from pr_info import PRInfo
from report import FAIL, FAILURE, OK, SUCCESS, JobReport, TestResult, TestResults
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
    parser.add_argument("--pull-image", default=True, help=argparse.SUPPRESS)
    parser.add_argument(
        "--no-pull-image",
        dest="pull_image",
        action="store_false",
        default=argparse.SUPPRESS,
        help="do not pull docker image, use existing",
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

    docker_image = get_docker_image("clickhouse/docs-builder")
    if args.pull_image:
        docker_image = pull_image(docker_image)

    test_output = temp_path / "docs_check"
    test_output.mkdir(parents=True, exist_ok=True)

    cmd = (
        f"docker run --cap-add=SYS_PTRACE --user={os.geteuid()}:{os.getegid()} "
        f"-e GIT_DOCS_BRANCH={args.docs_branch} "
        f"--volume={repo_path}:/ClickHouse --volume={test_output}:/output_path "
        f"{docker_image}"
    )

    run_log_path = test_output / "run.log"
    logging.info("Running command: '%s'", cmd)

    test_results = []  # type: TestResults

    test_sw = Stopwatch()
    with TeePopen(f"{cmd} --out-dir /output_path/build", run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            job_status = SUCCESS
            build_status = OK
            description = "Docs check passed"
        else:
            description = "Docs check failed (non zero exit code)"
            job_status = FAILURE
            build_status = FAIL
            logging.info("Run failed")

    if build_status == OK:
        with open(run_log_path, "r", encoding="utf-8") as lfd:
            for line in lfd:
                if "ERROR" in line:
                    build_status = FAIL
                    job_status = FAIL
                    break

    test_results.append(
        TestResult("Docs build", build_status, test_sw.duration_seconds, [run_log_path])
    )

    htmltest_log = test_output / "htmltest.log"

    # FIXME: after all issues in htmltest will be fixed, consider the failure as a
    # failed job
    test_sw.reset()
    with TeePopen(
        f"{cmd} htmltest -c /ClickHouse/docs/.htmltest.yml /output_path/build",
        htmltest_log,
    ) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
            test_results.append(
                TestResult("htmltest", OK, test_sw.duration_seconds, [htmltest_log])
            )
        else:
            logging.info("Run failed")
            test_results.append(
                TestResult(
                    "htmltest", "FLAKY", test_sw.duration_seconds, [htmltest_log]
                )
            )

    JobReport(
        description=description,
        test_results=test_results,
        status=job_status,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=[],
    ).dump()

    if job_status == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
