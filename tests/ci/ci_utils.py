import logging
import os
import signal
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Dict, Iterator, List, Tuple, Union

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


def set_job_timeout():
    def timeout_handler(_signum, _frame):
        print("Timeout expired")
        raise TimeoutError("Job's KILL_TIMEOUT expired")

    kill_timeout = int(os.getenv("KILL_TIMEOUT", "0"))
    assert kill_timeout > 0, "kill timeout must be provided in KILL_TIMEOUT env"
    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(kill_timeout)
