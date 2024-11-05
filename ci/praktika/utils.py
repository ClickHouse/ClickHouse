import base64
import dataclasses
import glob
import json
import multiprocessing
import os
import re
import signal
import subprocess
import sys
import time
from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from typing import Any, Dict, Iterator, List, Optional, Type, TypeVar, Union

from praktika._settings import _Settings

T = TypeVar("T", bound="Serializable")


class MetaClasses:
    class WithIter(type):
        def __iter__(cls):
            return (v for k, v in cls.__dict__.items() if not k.startswith("_"))

    @dataclasses.dataclass
    class Serializable(ABC):
        @classmethod
        def to_dict(cls, obj):
            if dataclasses.is_dataclass(obj):
                return {k: cls.to_dict(v) for k, v in dataclasses.asdict(obj).items()}
            elif isinstance(obj, SimpleNamespace):
                return {k: cls.to_dict(v) for k, v in vars(obj).items()}
            elif isinstance(obj, list):
                return [cls.to_dict(i) for i in obj]
            elif isinstance(obj, dict):
                return {k: cls.to_dict(v) for k, v in obj.items()}
            else:
                return obj

        @classmethod
        def from_dict(cls: Type[T], obj: Dict[str, Any]) -> T:
            return cls(**obj)

        @classmethod
        def from_fs(cls: Type[T], name) -> T:
            with open(cls.file_name_static(name), "r", encoding="utf8") as f:
                try:
                    return cls.from_dict(json.load(f))
                except json.decoder.JSONDecodeError as ex:
                    print(f"ERROR: failed to parse json, ex [{ex}]")
                    print(f"JSON content [{cls.file_name_static(name)}]")
                    Shell.check(f"cat {cls.file_name_static(name)}")
                    raise ex

        @classmethod
        @abstractmethod
        def file_name_static(cls, name):
            pass

        def file_name(self):
            return self.file_name_static(self.name)

        def dump(self):
            with open(self.file_name(), "w", encoding="utf8") as f:
                json.dump(self.to_dict(self), f, indent=4)
            return self

        @classmethod
        def exist(cls, name):
            return Path(cls.file_name_static(name)).is_file()

        def to_json(self, pretty=False):
            return json.dumps(dataclasses.asdict(self), indent=4 if pretty else None)


class ContextManager:
    @staticmethod
    @contextmanager
    def cd(to: Optional[Union[Path, str]] = None) -> Iterator[None]:
        """
        changes current working directory to @path or `git root` if @path is None
        :param to:
        :return:
        """
        if not to:
            try:
                to = Shell.get_output_or_raise("git rev-parse --show-toplevel")
            except:
                pass
            if not to:
                if Path(_Settings.DOCKER_WD).is_dir():
                    to = _Settings.DOCKER_WD
            if not to:
                assert False, "FIX IT"
            assert to
        old_pwd = os.getcwd()
        os.chdir(to)
        try:
            yield
        finally:
            os.chdir(old_pwd)


class Shell:
    @classmethod
    def get_output_or_raise(cls, command, verbose=False):
        return cls.get_output(command, verbose=verbose, strict=True).strip()

    @classmethod
    def get_output(cls, command, strict=False, verbose=False):
        if verbose:
            print(f"Run command [{command}]")
        res = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if res.stderr:
            print(f"WARNING: stderr: {res.stderr.strip()}")
        if strict and res.returncode != 0:
            raise RuntimeError(f"command failed with {res.returncode}")
        return res.stdout.strip()

    @classmethod
    def get_res_stdout_stderr(cls, command, verbose=True):
        if verbose:
            print(f"Run command [{command}]")
        res = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        return res.returncode, res.stdout.strip(), res.stderr.strip()

    @classmethod
    def check(
        cls,
        command,
        log_file=None,
        strict=False,
        verbose=False,
        dry_run=False,
        stdin_str=None,
        timeout=None,
        retries=0,
        **kwargs,
    ):
        return (
            cls.run(
                command,
                log_file,
                strict,
                verbose,
                dry_run,
                stdin_str,
                retries=retries,
                timeout=timeout,
                **kwargs,
            )
            == 0
        )

    @classmethod
    def run(
        cls,
        command,
        log_file=None,
        strict=False,
        verbose=False,
        dry_run=False,
        stdin_str=None,
        timeout=None,
        retries=0,
        **kwargs,
    ):
        def _check_timeout(timeout, process) -> None:
            if not timeout:
                return
            time.sleep(timeout)
            print(
                f"WARNING: Timeout exceeded [{timeout}], sending SIGTERM to process group [{process.pid}]"
            )
            try:
                os.killpg(process.pid, signal.SIGTERM)
            except ProcessLookupError:
                print("Process already terminated.")
                return

            time_wait = 0
            wait_interval = 5

            # Wait for process to terminate
            while process.poll() is None and time_wait < 100:
                print("Waiting for process to exit...")
                time.sleep(wait_interval)
                time_wait += wait_interval

            # Force kill if still running
            if process.poll() is None:
                print(f"WARNING: Process still running after SIGTERM, sending SIGKILL")
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    print("Process already terminated.")

        # Dry-run
        if dry_run:
            print(f"Dry-run. Would run command [{command}]")
            return 0  # Return success for dry-run

        if verbose:
            print(f"Run command: [{command}]")

        log_file = log_file or "/dev/null"
        proc = None
        for retry in range(retries + 1):
            try:
                with open(log_file, "w") as log_fp:
                    proc = subprocess.Popen(
                        command,
                        shell=True,
                        stderr=subprocess.STDOUT,
                        stdout=subprocess.PIPE,
                        stdin=subprocess.PIPE if stdin_str else None,
                        universal_newlines=True,
                        start_new_session=True,  # Start a new process group for signal handling
                        bufsize=1,  # Line-buffered
                        errors="backslashreplace",
                        **kwargs,
                    )

                    # Start the timeout thread if specified
                    if timeout:
                        t = Thread(target=_check_timeout, args=(timeout, proc))
                        t.daemon = True
                        t.start()

                    # Write stdin if provided
                    if stdin_str:
                        proc.stdin.write(stdin_str)
                        proc.stdin.close()

                    # Process output in real-time
                    if proc.stdout:
                        for line in proc.stdout:
                            sys.stdout.write(line)
                            log_fp.write(line)

                    proc.wait()  # Wait for the process to finish

                    if proc.returncode == 0:
                        break  # Exit retry loop if success
                    else:
                        if verbose:
                            print(
                                f"ERROR: command [{command}] failed, exit code: {proc.returncode}, retry: {retry}/{retries}"
                            )
            except Exception as e:
                if verbose:
                    print(
                        f"ERROR: command failed, exception: {e}, retry: {retry}/{retries}"
                    )
                if proc:
                    proc.kill()

        # Handle strict mode (ensure process success or fail)
        if strict:
            assert (
                proc and proc.returncode == 0
            ), f"Command failed with return code {proc.returncode}"

        return proc.returncode if proc else 1  # Return 1 if process never started

    @classmethod
    def run_async(
        cls,
        command,
        stdin_str=None,
        verbose=False,
        suppress_output=False,
        **kwargs,
    ):
        if verbose:
            print(f"Run command in background [{command}]")
        proc = subprocess.Popen(
            command,
            shell=True,
            stderr=subprocess.STDOUT if not suppress_output else subprocess.DEVNULL,
            stdout=subprocess.PIPE if not suppress_output else subprocess.DEVNULL,
            stdin=subprocess.PIPE if stdin_str else None,
            universal_newlines=True,
            start_new_session=True,
            bufsize=1,
            errors="backslashreplace",
            **kwargs,
        )
        if proc.stdout:
            for line in proc.stdout:
                print(line, end="")
        return proc


class Utils:
    @staticmethod
    def terminate_process_group(pid, force=False):
        if not force:
            os.killpg(os.getpgid(pid), signal.SIGTERM)
        else:
            os.killpg(os.getpgid(pid), signal.SIGKILL)

    @staticmethod
    def set_env(key, val):
        os.environ[key] = val

    @staticmethod
    def print_formatted_error(error_message, stdout="", stderr=""):
        stdout_lines = stdout.splitlines() if stdout else []
        stderr_lines = stderr.splitlines() if stderr else []
        print(f"ERROR: {error_message}")
        if stdout_lines:
            print("  Out:")
            for line in stdout_lines:
                print(f"     | {line}")
        if stderr_lines:
            print("  Err:")
            for line in stderr_lines:
                print(f"     | {line}")

    @staticmethod
    def sleep(seconds):
        time.sleep(seconds)

    @staticmethod
    def cwd():
        return Path.cwd()

    @staticmethod
    def cpu_count():
        return multiprocessing.cpu_count()

    @staticmethod
    def raise_with_error(error_message, stdout="", stderr="", ex=None):
        Utils.print_formatted_error(error_message, stdout, stderr)
        raise ex or RuntimeError()

    @staticmethod
    def timestamp():
        return datetime.utcnow().timestamp()

    @staticmethod
    def timestamp_to_str(timestamp):
        return datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

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
    def to_base64(value):
        assert isinstance(value, str), f"TODO: not supported for {type(value)}"
        string_bytes = value.encode("utf-8")
        base64_bytes = base64.b64encode(string_bytes)
        base64_string = base64_bytes.decode("utf-8")
        return base64_string

    @staticmethod
    def is_hex(s):
        try:
            int(s, 16)
            return True
        except ValueError:
            return False

    @staticmethod
    def normalize_string(string: str) -> str:
        res = string.lower()
        for r in (
            (" ", "_"),
            ("(", ""),
            (")", ""),
            ("{", ""),
            ("}", ""),
            ("'", ""),
            ("[", ""),
            ("]", ""),
            (",", ""),
            ("/", "_"),
            ("-", "_"),
            (":", ""),
            ('"', ""),
        ):
            res = res.replace(*r)
        return res

    @staticmethod
    def traverse_path(path, file_suffixes=None, sorted=False, not_exists_ok=False):
        res = []

        def is_valid_file(file):
            if file_suffixes is None:
                return True
            return any(file.endswith(suffix) for suffix in file_suffixes)

        if os.path.isfile(path):
            if is_valid_file(path):
                res.append(path)
        elif os.path.isdir(path):
            for root, dirs, files in os.walk(path):
                for file in files:
                    full_path = os.path.join(root, file)
                    if is_valid_file(full_path):
                        res.append(full_path)
        elif "*" in str(path):
            res.extend(
                [
                    f
                    for f in glob.glob(path, recursive=True)
                    if os.path.isfile(f) and is_valid_file(f)
                ]
            )
        else:
            if not_exists_ok:
                pass
            else:
                assert False, f"File does not exist or not valid [{path}]"

        if sorted:
            res.sort(reverse=True)

        return res

    @classmethod
    def traverse_paths(
        cls,
        include_paths,
        exclude_paths,
        file_suffixes=None,
        sorted=False,
        not_exists_ok=False,
    ) -> List["str"]:
        included_files_ = set()
        for path in include_paths:
            included_files_.update(cls.traverse_path(path, file_suffixes=file_suffixes))

        excluded_files = set()
        for path in exclude_paths:
            res = cls.traverse_path(path, not_exists_ok=not_exists_ok)
            if not res:
                print(
                    f"WARNING: Utils.traverse_paths excluded 0 files by path [{path}] in exclude_paths"
                )
            else:
                excluded_files.update(res)
        res = [f for f in included_files_ if f not in excluded_files]
        if sorted:
            res.sort(reverse=True)
        return res

    @classmethod
    def add_to_PATH(cls, path):
        path_cur = os.getenv("PATH", "")
        if path_cur:
            path += ":" + path_cur
        os.environ["PATH"] = path

    class Stopwatch:
        def __init__(self):
            self.start_time = datetime.utcnow().timestamp()

        @property
        def duration(self) -> float:
            return datetime.utcnow().timestamp() - self.start_time


class TeePopen:
    def __init__(
        self,
        command: str,
        log_file: Union[str, Path] = "",
        env: Optional[dict] = None,
        timeout: Optional[int] = None,
    ):
        self.command = command
        self.log_file_name = log_file
        self.log_file = None
        self.env = env or os.environ.copy()
        self.process = None  # type: Optional[subprocess.Popen]
        self.timeout = timeout
        self.timeout_exceeded = False
        self.terminated_by_sigterm = False
        self.terminated_by_sigkill = False

    def _check_timeout(self) -> None:
        if self.timeout is None:
            return
        time.sleep(self.timeout)
        print(
            f"WARNING: Timeout exceeded [{self.timeout}], send SIGTERM to [{self.process.pid}] and give a chance for graceful termination"
        )
        self.send_signal(signal.SIGTERM)
        time_wait = 0
        self.terminated_by_sigterm = True
        self.timeout_exceeded = True
        while self.process.poll() is None and time_wait < 100:
            print("wait...")
            wait = 5
            time.sleep(wait)
            time_wait += wait
        while self.process.poll() is None:
            print(f"WARNING: Still running, send SIGKILL to [{self.process.pid}]")
            self.send_signal(signal.SIGKILL)
            self.terminated_by_sigkill = True
            time.sleep(2)

    def __enter__(self) -> "TeePopen":
        if self.log_file_name:
            self.log_file = open(self.log_file_name, "w", encoding="utf-8")
        self.process = subprocess.Popen(
            self.command,
            shell=True,
            universal_newlines=True,
            env=self.env,
            start_new_session=True,  # signall will be sent to all children
            stderr=subprocess.STDOUT,
            stdout=subprocess.PIPE,
            bufsize=1,
            errors="backslashreplace",
        )
        time.sleep(1)
        print(f"Subprocess started, pid [{self.process.pid}]")
        if self.timeout is not None and self.timeout > 0:
            t = Thread(target=self._check_timeout)
            t.daemon = True  # does not block the program from exit
            t.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()
        if self.log_file:
            self.log_file.close()

    def wait(self) -> int:
        if self.process.stdout is not None:
            for line in self.process.stdout:
                sys.stdout.write(line)
                if self.log_file:
                    self.log_file.write(line)

        return self.process.wait()

    def poll(self):
        return self.process.poll()

    def send_signal(self, signal_num):
        os.killpg(self.process.pid, signal_num)


if __name__ == "__main__":

    @dataclasses.dataclass
    class Test(MetaClasses.Serializable):
        name: str

        @staticmethod
        def file_name_static(name):
            return f"/tmp/{Utils.normalize_string(name)}.json"

    Test(name="dsada").dump()
    t = Test.from_fs("dsada")
    print(t)
