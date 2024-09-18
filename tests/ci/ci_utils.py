import json
import logging
import os
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Sequence, Tuple, Union

import requests

logger = logging.getLogger(__name__)


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


def kill_ci_runner(message: str) -> None:
    """The function to kill the current process with all parents when it's possible.
    Works only when run with the set `CI` environment"""
    if not os.getenv("CI", ""):  # cycle import env_helper
        logger.info("Running outside the CI, won't kill the runner")
        return
    print(f"::error::{message}")

    def get_ppid_name(pid: int) -> Tuple[int, str]:
        # Avoid using psutil, it's not in stdlib
        stats = Path(f"/proc/{pid}/stat").read_text(encoding="utf-8").split()
        return int(stats[3]), stats[1]

    pid = os.getpid()
    pids = {}  # type: Dict[str, str]
    while pid:
        ppid, name = get_ppid_name(pid)
        pids[str(pid)] = name
        pid = ppid
    logger.error(
        "Sleeping 5 seconds and killing all possible processes from following:\n %s",
        "\n ".join(f"{p}: {n}" for p, n in pids.items()),
    )
    time.sleep(5)
    # The current process will be killed too
    subprocess.run(f"kill -9 {' '.join(pids.keys())}", check=False, shell=True)


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
