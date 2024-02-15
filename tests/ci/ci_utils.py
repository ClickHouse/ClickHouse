from contextlib import contextmanager
import os
from typing import Any, List, Union, Iterator
from pathlib import Path


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
    lowercase_string = string.lower()
    normalized_string = (
        lowercase_string.replace(" ", "_")
        .replace("-", "_")
        .replace("/", "_")
        .replace("(", "")
        .replace(")", "")
        .replace(",", "")
    )
    return normalized_string


class GHActions:
    @staticmethod
    def print_in_group(group_name: str, lines: Union[Any, List[Any]]) -> None:
        lines = list(lines)
        print(f"::group::{group_name}")
        for line in lines:
            print(line)
        print("::endgroup::")
