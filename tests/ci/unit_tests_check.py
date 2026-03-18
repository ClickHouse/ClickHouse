#!/usr/bin/env python3

import json
import logging
import os
import subprocess
import sys
from pathlib import Path
from typing import Tuple

from build_download_helper import download_unit_tests
from docker_images_helper import get_docker_image, pull_image
from env_helper import REPORT_PATH, TEMP_PATH
from report import ERROR, FAIL, FAILURE, OK, SUCCESS, JobReport, TestResult, TestResults
from stopwatch import Stopwatch
from tee_popen import TeePopen

IMAGE_NAME = "clickhouse/unit-test"


def get_test_name(line):
    elements = reversed(line.split(" "))
    for element in elements:
        if "(" not in element and ")" not in element:
            return element
    raise ValueError(f"No test name in line '{line}'")


def process_results(
    result_directory: Path,
) -> Tuple[str, str, TestResults]:
    """The json is described by the next proto3 scheme:
    (It's wrong, but that's a copy/paste from
    https://google.github.io/googletest/advanced.html#generating-a-json-report)

    syntax = "proto3";

    package googletest;

    import "google/protobuf/timestamp.proto";
    import "google/protobuf/duration.proto";

    message UnitTest {
      int32 tests = 1;
      int32 failures = 2;
      int32 disabled = 3;
      int32 errors = 4;
      google.protobuf.Timestamp timestamp = 5;
      google.protobuf.Duration time = 6;
      string name = 7;
      repeated TestCase testsuites = 8;
    }

    message TestCase {
      string name = 1;
      int32 tests = 2;
      int32 failures = 3;
      int32 disabled = 4;
      int32 errors = 5;
      google.protobuf.Duration time = 6;
      repeated TestInfo testsuite = 7;
    }

    message TestInfo {
      string name = 1;
      string file = 6;
      int32 line = 7;
      enum Status {
        RUN = 0;
        NOTRUN = 1;
      }
      Status status = 2;
      google.protobuf.Duration time = 3;
      string classname = 4;
      message Failure {
        string failures = 1;
        string type = 2;
      }
      repeated Failure failures = 5;
    }"""

    test_results = []  # type: TestResults
    report_path = result_directory / "test_result.json"
    if not report_path.exists():
        logging.info("No output log on path %s", report_path)
        return ERROR, "No output log", test_results

    with open(report_path, "r", encoding="utf-8") as j:
        report = json.load(j)

    total_counter = report["tests"]
    failed_counter = report["failures"]
    error_counter = report["errors"]

    description = ""
    SEGFAULT = "Segmentation fault. "
    SIGNAL = "Exit on signal. "
    for suite in report["testsuites"]:
        suite_name = suite["name"]
        for test_case in suite["testsuite"]:
            case_name = test_case["name"]
            test_time = float(test_case["time"][:-1])
            raw_logs = None
            if "failures" in test_case:
                raw_logs = ""
                for failure in test_case["failures"]:
                    raw_logs += failure[FAILURE]
                if (
                    "Segmentation fault" in raw_logs  # type: ignore
                    and SEGFAULT not in description
                ):
                    description += SEGFAULT
                if (
                    "received signal SIG" in raw_logs  # type: ignore
                    and SIGNAL not in description
                ):
                    description += SIGNAL
            if test_case["status"] == "NOTRUN":
                test_status = "SKIPPED"
            elif raw_logs is None:
                test_status = OK
            else:
                test_status = FAIL

            test_results.append(
                TestResult(
                    f"{suite_name}.{case_name}",
                    test_status,
                    test_time,
                    raw_logs=raw_logs,
                )
            )

    check_status = SUCCESS
    tests_status = OK
    tests_time = float(report["time"][:-1])
    if failed_counter:
        check_status = FAILURE
        test_status = FAIL
    if error_counter:
        check_status = ERROR
        test_status = ERROR
    test_results.append(TestResult(report["name"], tests_status, tests_time))

    if not description:
        description += (
            f"fail: {failed_counter + error_counter}, "
            f"passed: {total_counter - failed_counter - error_counter}"
        )

    return check_status, description, test_results


def main():
    logging.basicConfig(level=logging.INFO)

    stopwatch = Stopwatch()

    check_name = sys.argv[1] if len(sys.argv) > 1 else os.getenv("CHECK_NAME")
    assert (
        check_name
    ), "Check name must be provided as an input arg or in CHECK_NAME env"

    temp_path = Path(TEMP_PATH)
    temp_path.mkdir(parents=True, exist_ok=True)

    docker_image = pull_image(get_docker_image(IMAGE_NAME))

    download_unit_tests(check_name, REPORT_PATH, temp_path)

    tests_binary = temp_path / "unit_tests_dbms"
    os.chmod(tests_binary, 0o777)

    test_output = temp_path / "test_output"
    test_output.mkdir(parents=True, exist_ok=True)

    # Don't run ASAN under gdb since that breaks leak detection
    gdb_enabled = "NO_GDB" if "asan" in check_name else "GDB"

    run_command = (
        f"docker run --cap-add=SYS_PTRACE --volume={tests_binary}:/unit_tests_dbms "
        "--security-opt seccomp=unconfined "  # required to issue io_uring sys-calls
        f"--volume={test_output}:/test_output {docker_image} {gdb_enabled}"
    )

    run_log_path = test_output / "run.log"

    logging.info("Going to run func tests: %s", run_command)

    with TeePopen(run_command, run_log_path) as process:
        retcode = process.wait()
        if retcode == 0:
            logging.info("Run successfully")
        else:
            logging.info("Run failed")

    subprocess.check_call(f"sudo chown -R ubuntu:ubuntu {TEMP_PATH}", shell=True)

    state, description, test_results = process_results(test_output)
    if retcode != 0 and state == SUCCESS:
        # The process might have failed without reporting it in the test_output (e.g. LeakSanitizer)
        state = FAILURE
        description = "Invalid return code. Check run.log"

    additional_files = [run_log_path] + [
        p for p in test_output.iterdir() if not p.is_dir()
    ]
    JobReport(
        description=description,
        test_results=test_results,
        status=state,
        start_time=stopwatch.start_time_str,
        duration=stopwatch.duration_seconds,
        additional_files=additional_files,
    ).dump()

    if state == FAILURE:
        sys.exit(1)


if __name__ == "__main__":
    main()
