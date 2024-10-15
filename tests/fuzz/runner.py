#!/usr/bin/env python3

import configparser
import logging
import os
import subprocess
from pathlib import Path

DEBUGGER = os.getenv("DEBUGGER", "")
FUZZER_ARGS = os.getenv("FUZZER_ARGS", "")


def run_fuzzer(fuzzer: str):
    logging.info("Running fuzzer %s...", fuzzer)

    corpus_dir = f"{fuzzer}.in"
    with Path(corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            corpus_dir = ""

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

    cmd_line = f"{DEBUGGER} ./{fuzzer} {FUZZER_ARGS} {corpus_dir}"
    if custom_libfuzzer_options:
        cmd_line += f" {custom_libfuzzer_options}"
    if fuzzer_arguments:
        cmd_line += f" {fuzzer_arguments}"

    if not "-dict=" in cmd_line and Path(f"{fuzzer}.dict").exists():
        cmd_line += f" -dict={fuzzer}.dict"

    cmd_line += " < /dev/null"

    logging.info("...will execute: %s", cmd_line)
    subprocess.check_call(cmd_line, shell=True)


def main():
    logging.basicConfig(level=logging.INFO)

    subprocess.check_call("ls -al", shell=True)

    with Path() as current:
        for fuzzer in current.iterdir():
            if (current / fuzzer).is_file() and os.access(current / fuzzer, os.X_OK):
                run_fuzzer(fuzzer)


if __name__ == "__main__":
    main()
