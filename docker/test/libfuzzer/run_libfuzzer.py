#!/usr/bin/env python3

import logging
import os
from pathlib import Path
import subprocess
from parse_options import parse_options

DEBUGGER = os.getenv("DEBUGGER", "")
FUZZER_ARGS = os.getenv("FUZZER_ARGS", "")


def run_fuzzer(fuzzer: str):
    logging.info(f"Running fuzzer {fuzzer}...")

    corpus_dir = f"{fuzzer}.in"
    with Path(corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            corpus_dir = ""

    options_file = f"{fuzzer}.options"
    custom_libfuzzer_options = ""

    with Path(options_file) as path:
        if path.exists() and path.is_file():
            custom_asan_options = parse_options(options_file, "asan")
            if custom_asan_options:
                os.environ[
                    "ASAN_OPTIONS"
                ] = f"{os.environ['ASAN_OPTIONS']}:{custom_asan_options}"

            custom_msan_options = parse_options(options_file, "msan")
            if custom_msan_options:
                os.environ[
                    "MSAN_OPTIONS"
                ] = f"{os.environ['MSAN_OPTIONS']}:{custom_msan_options}"

            custom_ubsan_options = parse_options(options_file, "ubsan")
            if custom_ubsan_options:
                os.environ[
                    "UBSAN_OPTIONS"
                ] = f"{os.environ['UBSAN_OPTIONS']}:{custom_ubsan_options}"

            custom_libfuzzer_options = parse_options(options_file, "libfuzzer")

    cmd_line = f"{DEBUGGER} ./{fuzzer} {FUZZER_ARGS} {corpus_dir}"
    if custom_libfuzzer_options:
        cmd_line += f" {custom_libfuzzer_options}"

    if not "-dict=" in cmd_line and Path(f"{fuzzer}.dict").exists():
        cmd_line += f" -dict={fuzzer}.dict"

    cmd_line += " < /dev/null"

    logging.info(f"...will execute: {cmd_line}")
    subprocess.check_call(cmd_line, shell=True)


def main():
    logging.basicConfig(level=logging.INFO)

    subprocess.check_call("ls -al", shell=True)

    with Path() as current:
        for fuzzer in current.iterdir():
            if (current / fuzzer).is_file() and os.access(current / fuzzer, os.X_OK):
                run_fuzzer(fuzzer)

    exit(0)


if __name__ == "__main__":
    main()
