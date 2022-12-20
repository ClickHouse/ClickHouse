#!/usr/bin/env python3

from io import TextIOWrapper
from subprocess import Popen, PIPE, STDOUT
from threading import Thread
from time import sleep
from typing import Optional
import logging
import os
import sys


# Very simple tee logic implementation. You can specify shell command, output
# logfile and env variables. After TeePopen is created you can only wait until
# it finishes. stderr and stdout will be redirected both to specified file and
# stdout.
class TeePopen:
    def __init__(
        self,
        command: str,
        log_file: str,
        env: Optional[dict] = None,
        timeout: Optional[int] = None,
    ):
        self.command = command
        self._log_file_name = log_file
        self._log_file = None  # type: Optional[TextIOWrapper]
        self.env = env or os.environ.copy()
        self._process = None  # type: Optional[Popen]
        self.timeout = timeout

    def _check_timeout(self) -> None:
        if self.timeout is None:
            return
        sleep(self.timeout)
        while self.process.poll() is None:
            logging.warning(
                "Killing process %s, timeout %s exceeded",
                self.process.pid,
                self.timeout,
            )
            os.killpg(self.process.pid, 9)
            sleep(10)

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
        )
        if self.timeout is not None and self.timeout > 0:
            t = Thread(target=self._check_timeout)
            t.daemon = True  # does not block the program from exit
            t.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.wait()
        self.log_file.close()

    def wait(self):
        if self.process.stdout is not None:
            for line in self.process.stdout:
                sys.stdout.write(line)
                self.log_file.write(line)

        return self.process.wait()

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
            self._log_file = open(self._log_file_name, "w", encoding="utf-8")
        return self._log_file
