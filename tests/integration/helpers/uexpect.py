# Copyright (c) 2019 Vitaliy Zakaznikov
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import os
import pty
import re
import time
from queue import Empty, Queue
from subprocess import Popen
from threading import Event, Thread
from typing import Any, Callable


class TimeoutError(Exception):
    def __init__(self, timeout: float) -> None:
        self.timeout: float = timeout

    def __str__(self) -> str:
        return "Timeout %.3fs" % float(self.timeout)


class ExpectTimeoutError(Exception):
    def __init__(self, pattern: re.Pattern[str], timeout: float, buffer: str | None) -> None:
        self.pattern: re.Pattern[str] = pattern
        self.timeout: float = timeout
        self.buffer: str | None = buffer

    def __str__(self) -> str:
        s = "Timeout %.3fs " % float(self.timeout)
        if self.pattern:
            s += "for %s " % repr(self.pattern.pattern)
        if self.buffer:
            s += "buffer %s " % repr(self.buffer[:])
            s += "or '%s'" % ",".join(["%x" % ord(c) for c in self.buffer[:]])
        return s


class IO:
    class EOF:
        pass

    class Timeout:
        pass

    EOF = EOF
    TIMEOUT = Timeout

    class Logger:
        def __init__(self, logger: Any, prefix: str = "") -> None:  # pyright: ignore[reportAny, reportExplicitAny]
            self._logger: Any = logger  # pyright: ignore[reportAny, reportExplicitAny]
            self._prefix: str = prefix

        def write(self, data: str) -> None:
            self._logger.write(("\n" + data).replace("\n", "\n" + self._prefix))

        def flush(self) -> None:
            self._logger.flush()

    def __init__(self, process: Popen[bytes], master: int, queue: Queue[str], reader: dict[str, Any]) -> None:  # pyright: ignore[reportAny, reportExplicitAny]
        self.process: Popen[bytes] = process
        self.master: int = master
        self.queue: Queue[str] = queue
        self.buffer: str | None = None
        self.before: str | None = None
        self.after: str | None = None
        self.match: re.Match[str] | None = None
        self.pattern: re.Pattern[str] | None = None
        self.reader: dict[str, Any] = reader  # pyright: ignore[reportAny, reportExplicitAny]
        self._timeout: float | None = None
        self._logger: IO.Logger | None = None
        self._eol: str = ""

    def __enter__(self) -> "IO":
        return self

    def __exit__(self, exc_type: type[BaseException] | None, exc_val: BaseException | None, exc_tb: Any) -> None:  # pyright: ignore[reportAny, reportExplicitAny]
        self.close()

    def logger(self, logger: Any = None, prefix: str = "") -> IO.Logger | None:  # pyright: ignore[reportAny, reportExplicitAny]
        if logger:
            self._logger = self.Logger(logger, prefix=prefix)
        return self._logger

    def timeout(self, timeout: float | None = None) -> float | None:
        if timeout:
            self._timeout = timeout
        return self._timeout

    def eol(self, eol: str | None = None) -> str:
        if eol:
            self._eol = eol
        return self._eol

    def close(self, force: bool = True) -> None:
        self.reader["kill_event"].set()
        os.system("pkill -TERM -P %d" % self.process.pid)
        if force:
            self.process.kill()
        else:
            self.process.terminate()
        os.close(self.master)
        if self._logger:
            self._logger.write("\n")
            self._logger.flush()

    def send(self, data: str, eol: str | None = None) -> int:
        if eol is None:
            eol = self._eol
        return self.write(data + eol)

    def write(self, data: str) -> int:
        return os.write(self.master, data.encode())

    def expect(self, pattern: str, timeout: float | None = None, escape: bool = False) -> re.Match[str]:
        self.match = None
        self.before = None
        self.after = None
        if escape:
            pattern = re.escape(pattern)
        pattern = re.compile(pattern)
        if timeout is None:
            timeout = self._timeout
        timeleft = timeout
        while True:
            start_time = time.time()
            if self.buffer is not None:
                self.match = pattern.search(self.buffer, 0)
                if self.match is not None:
                    self.after = self.buffer[self.match.start() : self.match.end()]
                    self.before = self.buffer[: self.match.start()]
                    self.buffer = self.buffer[self.match.end() :]
                    break
            if timeleft < 0:
                break
            try:
                data = self.read(timeout=timeleft, raise_exception=True)
            except TimeoutError:
                if self._logger:
                    self._logger.write((self.buffer or "") + "\n")
                    self._logger.flush()
                exception = ExpectTimeoutError(pattern, timeout, self.buffer)
                self.buffer = None
                raise exception
            timeleft -= time.time() - start_time
            if data:
                self.buffer = (self.buffer + data) if self.buffer else data
        if self._logger:
            self._logger.write((self.before or "") + (self.after or ""))
            self._logger.flush()
        if self.match is None:
            exception = ExpectTimeoutError(pattern, timeout, self.buffer)
            self.buffer = None
            raise exception
        return self.match

    def read(self, timeout: float = 0, raise_exception: bool = False) -> str:
        data = ""
        timeleft = timeout
        try:
            while timeleft >= 0:
                start_time = time.time()
                data += self.queue.get(timeout=timeleft)
                if data:
                    break
                timeleft -= time.time() - start_time
        except Empty:
            if data:
                return data
            if raise_exception:
                raise TimeoutError(timeout)
            pass
        if not data and raise_exception:
            raise TimeoutError(timeout)

        return data


def spawn(command: list[str]) -> IO:
    master, slave = pty.openpty()
    process = Popen(
        command,
        preexec_fn=os.setsid,
        stdout=slave,
        stdin=slave,
        stderr=slave,
        bufsize=1,
    )
    os.close(slave)

    queue = Queue()
    reader_kill_event = Event()
    thread = Thread(target=reader, args=(process, master, queue, reader_kill_event))
    thread.daemon = True
    thread.start()

    return IO(
        process,
        master,
        queue,
        reader={"thread": thread, "kill_event": reader_kill_event},
    )


def reader(process: Popen[bytes], out: int, queue: Queue[str], kill_event: Event) -> None:
    while True:
        try:
            # TODO: there are some issues with 1<<16 buffer size
            data = os.read(out, 1 << 17).decode(errors="replace")
            queue.put(data)
        except:
            if kill_event.is_set():
                break
            raise
