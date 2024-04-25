from contextlib import contextmanager
import os
import signal
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


def set_job_timeout():
    def timeout_handler(_signum, _frame):
        print("Timeout expired")
        raise TimeoutError("Job's KILL_TIMEOUT expired")

    kill_timeout = int(os.getenv("KILL_TIMEOUT", "0"))
    assert kill_timeout > 0, "kill timeout must be provided in KILL_TIMEOUT env"
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(kill_timeout)
