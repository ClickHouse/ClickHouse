#!/usr/bin/env python3

import configparser
import logging
import os
import re
import subprocess
from pathlib import Path

DEBUGGER = os.getenv("DEBUGGER", "")
FUZZER_ARGS = os.getenv("FUZZER_ARGS", "")


def report(source: str, reason: str, call_stack: list, test_unit: str):
    print(f"########### REPORT: {source} {reason} {test_unit}")
    for line in call_stack:
        print(f"    {line}")
    print("########### END OF REPORT ###########")


# pylint: disable=unused-argument
def process_fuzzer_output(output: str):
    pass


def process_error(error: str):
    ERROR = r"^==\d+==\s?ERROR: (\S+): (.*)"
    error_source = ""
    error_reason = ""
    TEST_UNIT_LINE = r"artifact_prefix='.*/'; Test unit written to (.*)"
    call_stack = []
    is_call_stack = False

    # pylint: disable=unused-variable
    for line_num, line in enumerate(error.splitlines(), 1):
        if is_call_stack:
            if re.search(r"^==\d+==", line):
                is_call_stack = False
                continue
            call_stack.append(line)
            continue

        if call_stack:
            match = re.search(TEST_UNIT_LINE, line)
            if match:
                report(error_source, error_reason, call_stack, match.group(1))
                call_stack.clear()
            continue

        match = re.search(ERROR, line)
        if match:
            error_source = match.group(1)
            error_reason = match.group(2)
            is_call_stack = True


def run_fuzzer(fuzzer: str, timeout: int):
    s3 = S3Helper()

    logging.info("Running fuzzer %s...", fuzzer)

    seed_corpus_dir = f"{fuzzer}.in"
    with Path(seed_corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            seed_corpus_dir = ""

    active_corpus_dir = f"{fuzzer}.corpus"
    s3.download_files(
        bucket=S3_BUILDS_BUCKET,
        s3_path=f"fuzzer/corpus/{fuzzer}/",
        file_suffix="",
        local_directory=active_corpus_dir,
    )

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
                    f"-{key}={value}" for key, value in parser["libfuzzer"].items()
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
    # subprocess.check_call(cmd_line, shell=True)

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
        print("Stderr output: ", e.stderr)
        process_error(e.stderr)
    except subprocess.TimeoutExpired as e:
        print("Timeout for ", cmd_line)
        process_fuzzer_output(e.stderr)
    else:
        process_fuzzer_output(result.stderr)

    with open(f"{new_corpus_dir}/testfile", "a", encoding="ascii") as f:
        f.write("Now the file has more content!")

    s3.upload_build_directory_to_s3(new_corpus_dir, "fuzzer/corpus/")


def main():
    logging.basicConfig(level=logging.INFO)

    subprocess.check_call("ls -al", shell=True)

    timeout = 30

    match = re.search(r"(^|\s+)-max_total_time=(\d+)($|\s)", FUZZER_ARGS)
    if match:
        timeout += int(match.group(2))

    with Path() as current:
        for fuzzer in current.iterdir():
            if (current / fuzzer).is_file() and os.access(current / fuzzer, os.X_OK):
                run_fuzzer(fuzzer, timeout)


if __name__ == "__main__":
    from os import path, sys

    ACTIVE_DIR = path.dirname(path.abspath(__file__))
    sys.path.append(path.dirname(ACTIVE_DIR))
    from ci.env_helper import S3_BUILDS_BUCKET  # pylint: disable=import-error,no-name-in-module
    from ci.s3_helper import S3Helper  # pylint: disable=import-error,no-name-in-module

    main()
