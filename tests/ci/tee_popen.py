#!/usr/bin/env python3

import logging
import os
import signal
import sys
from io import TextIOWrapper
from pathlib import Path
from subprocess import PIPE, STDOUT, Popen
from threading import Thread
from time import sleep
from typing import Optional, Union


# Very simple tee logic implementation. You can specify a shell command, output
# logfile and env variables. After TeePopen is created you can only wait until
# it finishes. stderr and stdout will be redirected both to specified file and
# stdout.
class TeePopen:
    def __init__(
        self,
        command: str,
        log_file: Union[str, Path],
        env: Optional[dict] = None,
        timeout: Optional[int] = None,
    ):
        self.command = command
        self._log_file_name = log_file
        self._log_file = None  # type: Optional[TextIOWrapper]
        self.env = env or os.environ.copy()
        self._process = None  # type: Optional[Popen]
        self.timeout = timeout
        self.timeout_exceeded = False
        self.terminated_by_sigterm = False
        self.terminated_by_sigkill = False

    def _check_timeout(self) -> None:
        if self.timeout is None:
            return
        sleep(self.timeout)
        logging.warning(
            "Timeout exceeded. Send SIGTERM to process %s, timeout %s",
            self.process.pid,
            self.timeout,
        )
        os.killpg(self.process.pid, signal.SIGTERM)
        time_wait = 0
        self.terminated_by_sigterm = True
        self.timeout_exceeded = True
        while self.process.poll() is None and time_wait < 30:
            print("wait...")
            wait = 5
            sleep(wait)
            time_wait += wait
        while self.process.poll() is None:
            logging.error(
                "Process is still running. Send SIGKILL",
            )
            os.killpg(self.process.pid, signal.SIGKILL)
            self.terminated_by_sigkill = True
            sleep(5)

    def __enter__(self) -> "TeePopen":
        self.process = Popen(
            self.command,
            shell=True,
            universal_newlines=True,
            env=self.env,
            start_new_session=True,  # signall will be sent to all children
            stderr=STDOUT,
            stdout=PIPE,
            bufsize=1,
            errors="backslashreplace",
        )
        print(f"Subprocess started, pid [{self.process.pid}]")
        if self.timeout is not None and self.timeout > 0:
            t = Thread(target=self._check_timeout)
            t.daemon = True  # does not block the program from exit
            t.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()
        if self.timeout_exceeded:
            exceeded_log = (
                f"Command `{self.command}` has failed, "
                f"timeout {self.timeout}s is exceeded"
            )
            if self.process.stdout is not None:
                sys.stdout.write(exceeded_log)

            self.log_file.write(exceeded_log)

        self.log_file.close()

    def wait(self) -> int:
        if self.process.stdout is not None:
            for line in self.process.stdout:
                sys.stdout.write(line)
                self.log_file.write(line)

        return self.process.wait()

    def poll(self):
        return self.process.poll()

    def send_signal(self, signal):
        return self.process.send_signal(signal)

    @property
    def process(self) -> Popen:
        if self._process is not None:
            return self._process
        raise AttributeError("process is not created yet")

    @process.setter
    def process(self, process: Popen) -> None:
        self._process = process

    @property
    def log_file(self) -> TextIOWrapper:
        if self._log_file is None:
            # pylint:disable-next=consider-using-with
            self._log_file = open(self._log_file_name, "w", encoding="utf-8")
        return self._log_file
