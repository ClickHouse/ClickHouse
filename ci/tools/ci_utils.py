import os
import platform
import re
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional, Union


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


class Shell:
    @classmethod
    def run(cls, command, verbose=False) -> int:
        """Run command and return its integer exit code (no retries, no assert)."""
        if verbose:
            print(f"Run command [{command}]")
        return subprocess.run(command, shell=True).returncode

    @classmethod
    def get_output_or_raise(cls, command):
        return cls.get_output(command, strict=True)

    @classmethod
    def get_output(cls, command, strict=False):
        res = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            check=strict,
        )
        return res.stdout.strip()

    @classmethod
    def check(
        cls,
        command,
        strict=False,
        verbose=False,
        dry_run=False,
        stdin_str=None,
        retries=0,
        retry_delay=2,
        retry_backoff=2,
        **kwargs,
    ):
        if dry_run:
            print(f"Dry-ryn. Would run command [{command}]")
            return True
        if verbose:
            print(f"Run command [{command}]")
        attempts = max(1, retries + 1)
        retcode = 1
        for attempt in range(attempts):
            with subprocess.Popen(
                command,
                shell=True,
                stderr=subprocess.STDOUT,
                stdout=subprocess.PIPE,
                stdin=subprocess.PIPE if stdin_str else None,
                universal_newlines=True,
                start_new_session=True,
                bufsize=1,
                errors="backslashreplace",
                **kwargs,
            ) as proc:
                if stdin_str:
                    proc.communicate(input=stdin_str)
                elif proc.stdout:
                    for line in proc.stdout:
                        sys.stdout.write(line)
                proc.wait()
                retcode = proc.returncode
            if retcode == 0:
                break
            if attempt + 1 < attempts:
                delay = retry_delay * (retry_backoff**attempt)
                print(
                    f"Command failed with exit code {retcode}, "
                    f"retrying in {delay}s "
                    f"(attempt {attempt + 2}/{attempts}): {command}"
                )
                time.sleep(delay)
        if strict:
            assert retcode == 0, f"Command failed with exit code {retcode}: {command}"
        return retcode == 0


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
        Shell.check("sudo dmesg --clear", verbose=True)

    @staticmethod
    def is_hex(s):
        try:
            int(s, 16)
            return True
        except ValueError:
            return False

    @staticmethod
    def is_arm():
        arch = platform.machine()
        if "arm" in arch.lower() or "aarch" in arch.lower():
            return True
        return False

    @staticmethod
    def normalize_string(string: str) -> str:
        res = string.lower()
        for r in (
            (" ", "_"),
            ("(", "_"),
            (")", "_"),
            (",", "_"),
            ("/", "_"),
            ("-", "_"),
            (":", "_"),
        ):
            res = res.replace(*r)
        return res
