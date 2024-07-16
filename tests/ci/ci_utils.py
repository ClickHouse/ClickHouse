import os
import re
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Union, Optional


class WithIter(type):
    def __iter__(cls):
        return (v for k, v in cls.__dict__.items() if not k.startswith("_"))


@contextmanager
def cd(path: Union[Path, str]) -> Iterator[None]:
    oldpwd = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(oldpwd)


def is_hex(s):
    try:
        int(s, 16)
        return True
    except ValueError:
        return False


def normalize_string(string: str) -> str:
    res = string.lower()
    for r in ((" ", "_"), ("(", "_"), (")", "_"), (",", "_"), ("/", "_"), ("-", "_")):
        res = res.replace(*r)
    return res


class GHActions:
    @staticmethod
    def print_in_group(group_name: str, lines: Union[Any, List[Any]]) -> None:
        lines = list(lines)
        print(f"::group::{group_name}")
        for line in lines:
            print(line)
        print("::endgroup::")


class Shell:
    @classmethod
    def run_strict(cls, command):
        res = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )
        return res.stdout.strip()

    @classmethod
    def run(cls, command):
        res = ""
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        if result.returncode == 0:
            res = result.stdout
        return res.strip()

    @classmethod
    def check(cls, command):
        result = subprocess.run(
            command + " 2>&1",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=False,
        )
        return result.returncode == 0


class Utils:
    @staticmethod
    def get_failed_tests_number(description: str) -> Optional[int]:
        description = description.lower()

        pattern = r"fail:\s*(\d+)\s*(?=,|$)"
        match = re.search(pattern, description)
        if match:
            return int(match.group(1))
        return None

    @staticmethod
    def is_killed_with_oom():
        if Shell.check(
            "sudo dmesg -T | grep -q -e 'Out of memory: Killed process' -e 'oom_reaper: reaped process' -e 'oom-kill:constraint=CONSTRAINT_NONE'"
        ):
            return True
        return False

    @staticmethod
    def clear_dmesg():
        Shell.run("sudo dmesg --clear ||:")
