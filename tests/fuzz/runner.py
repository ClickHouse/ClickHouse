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
    ERROR = r"^==\d+== ERROR: (\S+): (.*)"
    error_source = ""
    error_reason = ""
    SUMMARY = r"^SUMMARY: "
    TEST_UNIT_LINE = r"artifact_prefix='.*/'; Test unit written to (.*)"
    test_unit = ""
    CALL_STACK_LINE = r"^\s+(#\d+.*)"
    call_stack = []
    is_call_stack = False

    # pylint: disable=unused-variable
    for line_num, line in enumerate(error.splitlines(), 1):

        if is_call_stack:
            match = re.search(CALL_STACK_LINE, line)
            if match:
                call_stack.append(match.group(1))
                continue

            if re.search(SUMMARY, line):
                is_call_stack = False
            continue

        if not call_stack and not is_call_stack:
            match = re.search(ERROR, line)
            if match:
                error_source = match.group(1)
                error_reason = match.group(2)
                is_call_stack = True
                continue

        match = re.search(TEST_UNIT_LINE, line)
        if match:
            test_unit = match.group(1)

    report(error_source, error_reason, call_stack, test_unit)


def run_fuzzer(fuzzer: str, timeout: int):
    logging.info("Running fuzzer %s...", fuzzer)

    seed_corpus_dir = f"{fuzzer}.in"
    with Path(seed_corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            seed_corpus_dir = ""

    active_corpus_dir = f"{fuzzer}.corpus"
    if not os.path.exists(active_corpus_dir):
        os.makedirs(active_corpus_dir)

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

    cmd_line = (
        f"{DEBUGGER} ./{fuzzer} {FUZZER_ARGS} {active_corpus_dir} {seed_corpus_dir}"
    )
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
        print("Stderr output:", e.stderr)
        process_error(e.stderr)
    else:
        process_fuzzer_output(result.stderr)


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
    main()
