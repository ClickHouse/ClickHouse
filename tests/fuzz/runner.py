#!/usr/bin/env python3

import configparser
import datetime
import logging
import os
import re
import signal
import subprocess
from pathlib import Path
from time import sleep

DEBUGGER = os.getenv("DEBUGGER", "")
FUZZER_ARGS = os.getenv("FUZZER_ARGS", "")
OUTPUT = "/test_output"


class Stopwatch:
    def __init__(self):
        self.reset()

    @property
    def duration_seconds(self) -> float:
        return (datetime.datetime.utcnow() - self.start_time).total_seconds()

    @property
    def start_time_str(self) -> str:
        return self.start_time_str_value

    def reset(self) -> None:
        self.start_time = datetime.datetime.utcnow()
        self.start_time_str_value = self.start_time.strftime("%Y-%m-%d %H:%M:%S")


def kill_fuzzer(fuzzer: str):
    with subprocess.Popen(["ps", "-A", "u"], stdout=subprocess.PIPE) as p:
        out, _ = p.communicate()
        for line in out.splitlines():
            if fuzzer.encode("utf-8") in line:
                pid = int(line.split(None, 2)[1])
                logging.info("Killing fuzzer %s, pid %d", fuzzer, pid)
                os.kill(pid, signal.SIGKILL)


def run_fuzzer(fuzzer: str, timeout: int):
    logging.info("Running fuzzer %s...", fuzzer)

    seed_corpus_dir = f"{fuzzer}.in"
    with Path(seed_corpus_dir) as path:
        if not path.exists() or not path.is_dir():
            seed_corpus_dir = ""

    active_corpus_dir = f"corpus/{fuzzer}"
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
                    f"-{key}={value}"
                    for key, value in parser["libfuzzer"].items()
                    if key not in ("jobs", "exact_artifact_path")
                )

            if parser.has_section("fuzzer_arguments"):
                fuzzer_arguments = " ".join(
                    (f"{key}") if value == "" else (f"{key}={value}")
                    for key, value in parser["fuzzer_arguments"].items()
                )

    exact_artifact_path = f"{OUTPUT}/{fuzzer}.unit"
    status_path = f"{OUTPUT}/{fuzzer}.status"
    out_path = f"{OUTPUT}/{fuzzer}.out"

    cmd_line = (
        f"{DEBUGGER} ./{fuzzer} {FUZZER_ARGS} {active_corpus_dir} {seed_corpus_dir}"
    )

    cmd_line += f" -exact_artifact_path={exact_artifact_path}"

    if custom_libfuzzer_options:
        cmd_line += f" {custom_libfuzzer_options}"
    if fuzzer_arguments:
        cmd_line += f" {fuzzer_arguments}"

    if not "-dict=" in cmd_line and Path(f"{fuzzer}.dict").exists():
        cmd_line += f" -dict={fuzzer}.dict"

    logging.info("...will execute: %s", cmd_line)

    stopwatch = Stopwatch()
    try:
        with open(out_path, "wb") as out:
            subprocess.run(
                cmd_line.split(),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=out,
                text=True,
                check=True,
                shell=False,
                errors="replace",
                timeout=timeout,
            )
    except subprocess.CalledProcessError:
        logging.info("Fail running %s", fuzzer)
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"FAIL\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )
    except subprocess.TimeoutExpired:
        logging.info("Timeout running %s", fuzzer)
        kill_fuzzer(fuzzer)
        sleep(10)
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"Timeout\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )
    else:
        logging.info("Successful running %s", fuzzer)
        with open(status_path, "w", encoding="utf-8") as status:
            status.write(
                f"OK\n{stopwatch.start_time_str}\n{stopwatch.duration_seconds}\n"
            )
        os.remove(out_path)


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
                run_fuzzer(fuzzer.name, timeout)

    subprocess.check_call(f"ls -al {OUTPUT}", shell=True)


if __name__ == "__main__":
    main()
