#!/usr/bin/env python3

import configparser
import logging
import os
import re
import signal
import subprocess
from pathlib import Path
from time import sleep

from botocore.exceptions import ClientError

DEBUGGER = os.getenv("DEBUGGER", "")
FUZZER_ARGS = os.getenv("FUZZER_ARGS", "")


def report(source: str, reason: str, call_stack: list, test_unit: str):
    logging.info("########### REPORT: %s %s %s", source, reason, test_unit)
    logging.info("".join(call_stack))
    logging.info("########### END OF REPORT ###########")


# pylint: disable=unused-argument
def process_fuzzer_output(output: str):
    pass


def process_error(error: str) -> list:
    ERROR = r"^==\d+==\s?ERROR: (\S+): (.*)"
    error_source = ""
    error_reason = ""
    test_unit = ""
    TEST_UNIT_LINE = r"artifact_prefix='.*\/'; Test unit written to (.*)"
    error_info = []
    is_error = False

    # pylint: disable=unused-variable
    for line_num, line in enumerate(error.splitlines(), 1):
        if is_error:
            error_info.append(line)
            match = re.search(TEST_UNIT_LINE, line)
            if match:
                test_unit = match.group(1)
            continue

        match = re.search(ERROR, line)
        if match:
            error_info.append(line)
            error_source = match.group(1)
            error_reason = match.group(2)
            is_error = True

    report(error_source, error_reason, error_info, test_unit)
    return error_info


def kill_fuzzer(fuzzer: str):
    with subprocess.Popen(["ps", "-A", "u"], stdout=subprocess.PIPE) as p:
        out, _ = p.communicate()
        for line in out.splitlines():
            if fuzzer.encode("utf-8") in line:
                pid = int(line.split(None, 2)[1])
                logging.info("Killing fuzzer %s, pid %d", fuzzer, pid)
                os.kill(pid, signal.SIGKILL)


def run_fuzzer(fuzzer: str, timeout: int):
    s3 = S3Helper()

    logging.info("Running fuzzer %s...", fuzzer)

    seed_corpus_dir = f"{fuzzer}.in"
    with Path(seed_corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            seed_corpus_dir = ""

    active_corpus_dir = f"{fuzzer}.corpus"
    try:
        s3.download_files(
            bucket=S3_BUILDS_BUCKET,
            s3_path=f"fuzzer/corpus/{fuzzer}/",
            file_suffix="",
            local_directory=active_corpus_dir,
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            logging.debug("No active corpus exists for %s", fuzzer)
        else:
            raise

    new_corpus_dir = f"{fuzzer}.corpus_new"
    if not os.path.exists(new_corpus_dir):
        os.makedirs(new_corpus_dir)

    options_file = f"{fuzzer}.options"
    custom_libfuzzer_options = ""
    fuzzer_arguments = ""

    with Path(options_file) as path:
        if path.exists() and path.is_file():
            parser = configparser.ConfigParser()
            parser.read(path)

            if parser.has_section("asan"):
                os.environ["ASAN_OPTIONS"] = (
                    f"{os.environ['ASAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['asan'].items())}"
                )

            if parser.has_section("msan"):
                os.environ["MSAN_OPTIONS"] = (
                    f"{os.environ['MSAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['msan'].items())}"
                )

            if parser.has_section("ubsan"):
                os.environ["UBSAN_OPTIONS"] = (
                    f"{os.environ['UBSAN_OPTIONS']}:{':'.join(f'{key}={value}' for key, value in parser['ubsan'].items())}"
                )

            if parser.has_section("libfuzzer"):
                custom_libfuzzer_options = " ".join(
                    f"-{key}={value}"
                    for key, value in parser["libfuzzer"].items()
                    if key != "jobs"
                )

            if parser.has_section("fuzzer_arguments"):
                fuzzer_arguments = " ".join(
                    (f"{key}") if value == "" else (f"{key}={value}")
                    for key, value in parser["fuzzer_arguments"].items()
                )

    cmd_line = f"{DEBUGGER} ./{fuzzer} {FUZZER_ARGS} {new_corpus_dir} {active_corpus_dir} {seed_corpus_dir}"

    if custom_libfuzzer_options:
        cmd_line += f" {custom_libfuzzer_options}"
    if fuzzer_arguments:
        cmd_line += f" {fuzzer_arguments}"

    if not "-dict=" in cmd_line and Path(f"{fuzzer}.dict").exists():
        cmd_line += f" -dict={fuzzer}.dict"

    cmd_line += " < /dev/null"

    logging.info("...will execute: %s", cmd_line)

    test_result = TestResult(fuzzer, "OK")
    stopwatch = Stopwatch()
    try:
        result = subprocess.run(
            cmd_line,
            stderr=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            text=True,
            check=True,
            shell=True,
            errors="replace",
            timeout=timeout,
        )
    except subprocess.CalledProcessError as e:
        # print("Command failed with error:", e)
        logging.info("Stderr output: %s", e.stderr)
        test_result = TestResult(
            fuzzer,
            "FAIL",
            stopwatch.duration_seconds,
            "",
            "\n".join(process_error(e.stderr)),
        )
    except subprocess.TimeoutExpired as e:
        logging.info("Timeout for %s", cmd_line)
        kill_fuzzer(fuzzer)
        sleep(10)
        process_fuzzer_output(e.stderr)
        test_result = TestResult(
            fuzzer,
            "Timeout",
            stopwatch.duration_seconds,
            "",
            "",
        )
    else:
        process_fuzzer_output(result.stderr)
        test_result.time = stopwatch.duration_seconds

    s3.upload_build_directory_to_s3(
        Path(new_corpus_dir), f"fuzzer/corpus/{fuzzer}", False
    )

    logging.info("test_result: %s", test_result)
    return test_result


def main():
    logging.basicConfig(level=logging.INFO)

    subprocess.check_call("ls -al", shell=True)

    timeout = 30

    match = re.search(r"(^|\s+)-max_total_time=(\d+)($|\s)", FUZZER_ARGS)
    if match:
        timeout += int(match.group(2))

    test_results = []
    stopwatch = Stopwatch()
    with Path() as current:
        for fuzzer in current.iterdir():
            if (current / fuzzer).is_file() and os.access(current / fuzzer, os.X_OK):
                test_results.append(run_fuzzer(fuzzer.name, timeout))

    prepared_results = prepare_tests_results_for_clickhouse(
        PRInfo(),
        test_results,
        "failure",
        stopwatch.duration_seconds,
        stopwatch.start_time_str,
        "",
        "libFuzzer",
    )
    # ch_helper = ClickHouseHelper()
    # ch_helper.insert_events_into(db="default", table="checks", events=prepared_results)
    logging.info("prepared_results: %s", prepared_results)


if __name__ == "__main__":
    from os import path, sys

    ACTIVE_DIR = path.dirname(path.abspath(__file__))
    sys.path.append((Path(path.dirname(ACTIVE_DIR)) / "ci").as_posix())
    from clickhouse_helper import (  # pylint: disable=import-error,no-name-in-module,unused-import
        ClickHouseHelper,
        prepare_tests_results_for_clickhouse,
    )
    from env_helper import (  # pylint: disable=import-error,no-name-in-module
        S3_BUILDS_BUCKET,
    )
    from pr_info import PRInfo  # pylint: disable=import-error,no-name-in-module
    from report import TestResult  # pylint: disable=import-error,no-name-in-module
    from s3_helper import S3Helper  # pylint: disable=import-error,no-name-in-module
    from stopwatch import Stopwatch  # pylint: disable=import-error,no-name-in-module

    main()
