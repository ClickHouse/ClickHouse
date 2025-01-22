import logging
import os
import subprocess as sp
import tempfile
from threading import Timer

import numpy as np
import pandas as pd

DEFAULT_QUERY_TIMEOUT = 600


class Client:
    def __init__(
        self,
        host,
        port=9000,
        command="/usr/bin/clickhouse-client",
        secure=False,
        config=None,
    ):
        self.host = host
        self.port = port
        self.command = [command]

        if os.path.basename(command) == "clickhouse":
            self.command.append("client")

        if secure:
            self.command.append("--secure")
        if config is not None:
            self.command += ["--config-file", config]

        self.command += ["--host", self.host, "--port", str(self.port), "--stacktrace"]

    def stacktraces_on_timeout_decorator(func):
        def wrap(self, *args, **kwargs):
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
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        host=None,
        ignore_error=False,
        query_id=None,
        parse=False,
    ):
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
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        host=None,
        ignore_error=False,
        query_id=None,
        parse=False,
    ):
        command = self.command[:]

        if stdin is None:
            stdin = sql
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
            command += ["--host", host]
        if query_id is not None:
            command += ["--query_id", query_id]
        if parse:
            command += ["--format=TabSeparatedWithNames"]

        return CommandRequest(command, stdin, timeout, ignore_error, parse)

    @stacktraces_on_timeout_decorator
    def query_and_get_error(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        query_id=None,
    ):
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
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        query_id=None,
    ):
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
    def __init__(self, message, returncode, stderr):
        super(QueryRuntimeException, self).__init__(message)
        self.returncode = returncode
        self.stderr = stderr


class CommandRequest:
    def __init__(
        self, command, stdin=None, timeout=None, ignore_error=False, parse=False
    ):
        # Write data to tmp file to avoid PIPEs and execution blocking
        stdin_file = tempfile.TemporaryFile(mode="w+")
        stdin_file.write(stdin)
        stdin_file.seek(0)
        self.stdout_file = tempfile.TemporaryFile()
        self.stderr_file = tempfile.TemporaryFile()
        self.ignore_error = ignore_error
        self.parse = parse
        # print " ".join(command)

        # we suppress stderror on client becase sometimes thread sanitizer
        # can print some debug information there
        env = {}
        env["ASAN_OPTIONS"] = "use_sigaltstack=0"
        env["TSAN_OPTIONS"] = "use_sigaltstack=0 verbosity=0"
        self.process = sp.Popen(
            command,
            stdin=stdin_file,
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            env=env,
            universal_newlines=True,
        )

        self.timer = None
        self.process_finished_before_timeout = True
        if timeout is not None:

            def kill_process():
                if self.process.poll() is None:
                    self.process_finished_before_timeout = False
                    self.process.kill()

            self.timer = Timer(timeout, kill_process)
            self.timer.start()

    def remove_trash_from_stderr(self, stderr):
        # FIXME https://github.com/ClickHouse/ClickHouse/issues/48181
        if not stderr:
            return stderr
        lines = stderr.split("\n")
        lines = [
            x for x in lines if ("completion_queue" not in x and "Kick failed" not in x)
        ]
        return "\n".join(lines)

    def get_answer(self):
        self.process.wait(timeout=DEFAULT_QUERY_TIMEOUT)
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout = self.stdout_file.read().decode("utf-8", errors="replace")
        stderr = self.stderr_file.read().decode("utf-8", errors="replace")

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

            return (
                pd.read_csv(StringIO(stdout), sep="\t")
                .replace(r"\N", None)
                .replace(np.nan, None)
            )

        return stdout

    def get_error(self):
        self.process.wait(timeout=DEFAULT_QUERY_TIMEOUT)
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout = self.stdout_file.read().decode("utf-8", errors="replace")
        stderr = self.stderr_file.read().decode("utf-8", errors="replace")

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

    def get_answer_and_error(self):
        self.process.wait(timeout=DEFAULT_QUERY_TIMEOUT)
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout = self.stdout_file.read().decode("utf-8", errors="replace")
        stderr = self.stderr_file.read().decode("utf-8", errors="replace")

        if (
            self.timer is not None
            and not self.process_finished_before_timeout
            and not self.ignore_error
        ):
            raise QueryTimeoutExceedException("Client timed out!")

        return (stdout, stderr)
