import os
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator, List, Union


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
        subprocess.run(
            command + " 2>&1",
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=True,
        )

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
