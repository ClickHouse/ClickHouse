import logging
import os
import signal
import subprocess as sp
import tempfile
from threading import Timer
from typing import Any, Callable, cast

import numpy as np
import pandas as pd

DEFAULT_QUERY_TIMEOUT = 600


class Client:
    def __init__(
        self,
        host: str,
        port: int = 9000,
        command: str = "/usr/bin/clickhouse-client",
        secure: bool = False,
        config: str | None = None,
    ) -> None:
        self.host: str = host
        self.port: int = port
        self.command: list[str] = [command]

        if os.path.basename(command) == "clickhouse":
            self.command.append("client")

        if secure:
            self.command.append("--secure")
        if config is not None:
            self.command += ["--config-file", config]

        self.command += ["--host", self.host, "--port", str(self.port), "--stacktrace"]

    def stacktraces_on_timeout_decorator(func: Callable[..., Any]) -> Callable[..., Any]:
        def wrap(self: Any, *args: Any, **kwargs: Any) -> Any:
            try:
                return func(self, *args, **kwargs)
            except sp.TimeoutExpired:
                # I failed to make pytest print stacktraces using print(...) or logging.debug(...), so...
                self.get_query_request(
                    "INSERT INTO TABLE FUNCTION file('stacktraces_on_timeout.txt', 'TSVRaw', 'tn String, tid UInt64, qid String, st String') "
                    "SELECT thread_name, thread_id, query_id, arrayStringConcat(arrayMap(x -> demangle(addressToSymbol(x)), trace), '\n') AS res FROM system.stack_trace "
                    "SETTINGS allow_introspection_functions=1",
                    timeout=60,
                ).get_answer_and_error()
                raise

        return wrap

    @stacktraces_on_timeout_decorator
    def query(
        self,
        sql: str,
        stdin: str | None = None,
        timeout: float | None = None,
        settings: dict[str, Any] | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        host: str | None = None,
        ignore_error: bool = False,
        query_id: str | None = None,
        parse: bool = False,
    ) -> pd.DataFrame | str:
        return self.get_query_request(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
            host=host,
            ignore_error=ignore_error,
            query_id=query_id,
            parse=parse,
        ).get_answer()

    def get_query_request(
        self,
        sql: str,
        stdin: str | None = None,
        timeout: float | None = None,
        settings: dict[str, Any] | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        host: str | None = None,
        ignore_error: bool = False,
        query_id: str | None = None,
        parse: bool = False,
    ) -> "CommandRequest":
        command: list[str] = self.command[:]

        if stdin is None:
            stdin: str = sql
        else:
            command += ["--query", sql]

        if settings is not None:
            for setting, value in settings.items():
                command += ["--" + setting, str(value)]

        if user is not None:
            command += ["--user", user]
        if password is not None:
            command += ["--password", password]
        if database is not None:
            command += ["--database", database]

        if host is not None:
            replaced = False
            for i, token in enumerate(command):
                if token == "--host" and i + 1 < len(command):
                    command[i + 1] = host
                    replaced = True
                    break
            if not replaced:
                # Should not happen normally, but keep fallback
                command += ["--host", host]

        if query_id is not None:
            command += ["--query_id", query_id]
        if parse:
            command += ["--format=TabSeparatedWithNames"]

        return CommandRequest(command, stdin, timeout, ignore_error, parse)

    @stacktraces_on_timeout_decorator
    def query_and_get_error(
        self,
        sql: str,
        stdin: str | None = None,
        timeout: float | None = None,
        settings: dict[str, Any] | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        query_id: str | None = None,
    ) -> str:
        return self.get_query_request(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
            query_id=query_id,
        ).get_error()

    @stacktraces_on_timeout_decorator
    def query_and_get_answer_with_error(
        self,
        sql: str,
        stdin: str | None = None,
        timeout: float | None = None,
        settings: dict[str, Any] | None = None,
        user: str | None = None,
        password: str | None = None,
        database: str | None = None,
        query_id: str | None = None,
    ) -> tuple[str, str]:
        return self.get_query_request(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
            query_id=query_id,
        ).get_answer_and_error()


class QueryTimeoutExceedException(Exception):
    pass


class QueryRuntimeException(Exception):
    def __init__(self, message: str, returncode: int, stderr: str) -> None:
        super(QueryRuntimeException, self).__init__(message)
        self.returncode: int = returncode
        self.stderr: str = stderr


class CommandRequest:
    def __init__(
        self,
        command: list[str],
        stdin: str | None = None,
        timeout: float | None = None,
        ignore_error: bool = False,
        parse: bool = False,
        stdout_file_path: str | None = None,
        stderr_file_path: str | None = None,
        env: dict[str, str] = {},
    ) -> None:
        # Write data to tmp file to avoid PIPEs and execution blocking
        stdin_file: tempfile._TemporaryFileWrapper[str] = tempfile.TemporaryFile(
            mode="w+")
        assert stdin is not None
        _: int = stdin_file.write(stdin)
        stdin_file.seek(0)
        self.stdout_file: tempfile._TemporaryFileWrapper[bytes] | str = tempfile.TemporaryFile(
        ) if stdout_file_path is None else stdout_file_path
        self.stderr_file: tempfile._TemporaryFileWrapper[bytes] | str = tempfile.TemporaryFile(
        ) if stderr_file_path is None else stderr_file_path
        self.ignore_error: bool = ignore_error
        self.parse: bool = parse
        # print " ".join(command)

        # we suppress stderror on client becase sometimes thread sanitizer
        # can print some debug information there
        env["ASAN_OPTIONS"] = "use_sigaltstack=0"
        env["TSAN_OPTIONS"] = "use_sigaltstack=0 verbosity=0"
        self.process: sp.Popen[str] = sp.Popen(
            command,
            stdin=stdin_file,
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            env=env,
            universal_newlines=True,
        )

        self.timer: Timer | None = None
        self.process_finished_before_timeout: bool = True
        if timeout is not None:

            def kill_process() -> None:
                if self.process.poll() is None:
                    self.process_finished_before_timeout = False
                    self.process.kill()

            self.timer = Timer(timeout, kill_process)
            self.timer.start()

    def remove_trash_from_stderr(self, stderr: str) -> str:
        # FIXME https://github.com/ClickHouse/ClickHouse/issues/48181
        if not stderr:
            return stderr
        lines: list[str] = stderr.split("\n")
        lines = [
            x for x in lines if ("completion_queue" not in x and "Kick failed" not in x)
        ]
        return "\n".join(lines)

    def get_answer(self) -> pd.DataFrame | str:
        self.process.wait(timeout=DEFAULT_QUERY_TIMEOUT)
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout: str = cast(bytes, self.stdout_file.read()
                           ).decode("utf-8", errors="replace")
        stderr: str = cast(bytes, self.stderr_file.read()
                           ).decode("utf-8", errors="replace")

        if (
            self.timer is not None
            and not self.process_finished_before_timeout
            and not self.ignore_error
        ):
            logging.debug(f"Timed out. Last stdout:{stdout}, stderr:{stderr}")
            raise QueryTimeoutExceedException("Client timed out!")

        if (
            self.process.returncode != 0 or self.remove_trash_from_stderr(stderr)
        ) and not self.ignore_error:
            raise QueryRuntimeException(
                "Client failed! Return code: {}, stderr: {}".format(
                    self.process.returncode, stderr
                ),
                self.process.returncode,
                stderr,
            )

        if self.parse:
            from io import StringIO

            df: pd.DataFrame = pd.read_csv(StringIO(stdout), sep="\t")
            assert isinstance(df, pd.DataFrame)
            return (
                df.replace(r"\N", None)
                .replace(np.nan, None)
            )

        return stdout

    def get_error(self) -> str:
        self.process.wait(timeout=DEFAULT_QUERY_TIMEOUT)
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout: str = cast(bytes, self.stdout_file.read()).decode(
            # pyright: ignore[reportUnknownMemberType]
            "utf-8", errors="replace")
        stderr: str = cast(bytes, self.stderr_file.read()).decode(
            # pyright: ignore[reportUnknownMemberType]
            "utf-8", errors="replace")

        if (
            self.timer is not None
            and not self.process_finished_before_timeout
            and not self.ignore_error
        ):
            raise QueryTimeoutExceedException("Client timed out!")

        if self.process.returncode == 0:
            raise QueryRuntimeException(
                "Client expected to be failed but succeeded! stdout: {}".format(stdout),
                self.process.returncode,
                stderr,
            )

        return stderr

    def get_answer_and_error(self) -> tuple[str, str]:
        self.process.wait(timeout=DEFAULT_QUERY_TIMEOUT)
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout: str = cast(bytes, self.stdout_file.read()
                           ).decode("utf-8", errors="replace")
        stderr: str = cast(bytes, self.stderr_file.read()
                           ).decode("utf-8", errors="replace")

        if (
            self.timer is not None
            and not self.process_finished_before_timeout
            and not self.ignore_error
        ):
            raise QueryTimeoutExceedException("Client timed out!")

        return (stdout, stderr)

    def pause_process(self) -> None:
        self.process.send_signal(signal.SIGSTOP)

    def resume_process(self) -> None:
        self.process.send_signal(signal.SIGCONT)
