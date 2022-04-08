import os
import subprocess as sp
import tempfile
import logging
from threading import Timer


class Client:
    def __init__(self, host, port=9000, command="/usr/bin/clickhouse-client"):
        self.host = host
        self.port = port
        self.command = [command]

        if os.path.basename(command) == "clickhouse":
            self.command.append("client")

        self.command += ["--host", self.host, "--port", str(self.port), "--stacktrace"]

    def query(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
        ignore_error=False,
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
            ignore_error=ignore_error,
            query_id=query_id,
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
        ignore_error=False,
        query_id=None,
    ):
        command = self.command[:]

        if stdin is None:
            command += ["--multiquery"]
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

        if query_id is not None:
            command += ["--query_id", query_id]

        return CommandRequest(command, stdin, timeout, ignore_error)

    def query_and_get_error(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
    ):
        return self.get_query_request(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
        ).get_error()

    def query_and_get_answer_with_error(
        self,
        sql,
        stdin=None,
        timeout=None,
        settings=None,
        user=None,
        password=None,
        database=None,
    ):
        return self.get_query_request(
            sql,
            stdin=stdin,
            timeout=timeout,
            settings=settings,
            user=user,
            password=password,
            database=database,
        ).get_answer_and_error()


class QueryTimeoutExceedException(Exception):
    pass


class QueryRuntimeException(Exception):
    def __init__(self, message, returncode, stderr):
        super(QueryRuntimeException, self).__init__(message)
        self.returncode = returncode
        self.stderr = stderr


class CommandRequest:
    def __init__(self, command, stdin=None, timeout=None, ignore_error=False):
        # Write data to tmp file to avoid PIPEs and execution blocking
        stdin_file = tempfile.TemporaryFile(mode="w+")
        stdin_file.write(stdin)
        stdin_file.seek(0)
        self.stdout_file = tempfile.TemporaryFile()
        self.stderr_file = tempfile.TemporaryFile()
        self.ignore_error = ignore_error

        # print " ".join(command)

        # we suppress stderror on client becase sometimes thread sanitizer
        # can print some debug information there
        env = {}
        env["TSAN_OPTIONS"] = "verbosity=0"
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

    def get_answer(self):
        self.process.wait()
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

        if (self.process.returncode != 0 or stderr) and not self.ignore_error:
            raise QueryRuntimeException(
                "Client failed! Return code: {}, stderr: {}".format(
                    self.process.returncode, stderr
                ),
                self.process.returncode,
                stderr,
            )

        return stdout

    def get_error(self):
        self.process.wait()
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
        self.process.wait()
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
