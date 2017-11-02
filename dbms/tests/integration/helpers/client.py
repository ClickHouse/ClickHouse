import errno
import subprocess as sp
from threading import Timer
import tempfile


class Client:
    def __init__(self, host, port=9000, command='/usr/bin/clickhouse-client'):
        self.host = host
        self.port = port
        self.command = [command, '--host', self.host, '--port', str(self.port), '--stacktrace']


    def query(self, sql, stdin=None, timeout=None, settings=None):
        return self.get_query_request(sql, stdin=stdin, timeout=timeout, settings=settings).get_answer()


    def get_query_request(self, sql, stdin=None, timeout=None, settings=None):
        command = self.command[:]

        if stdin is None:
            command += ['--multiquery']
            stdin = sql
        else:
            command += ['--query', sql]

        if settings is not None:
            for setting, value in settings.iteritems():
                command += ['--' + setting, str(value)]

        return CommandRequest(command, stdin, timeout)


class QueryTimeoutExceedException(Exception):
    pass


class QueryRuntimeException(Exception):
    pass


class CommandRequest:
    def __init__(self, command, stdin=None, timeout=None):
        # Write data to tmp file to avoid PIPEs and execution blocking
        stdin_file = tempfile.TemporaryFile()
        stdin_file.write(stdin)
        stdin_file.seek(0)
        self.stdout_file = tempfile.TemporaryFile()
        self.stderr_file = tempfile.TemporaryFile()

        #print " ".join(command)

        self.process = sp.Popen(command, stdin=stdin_file, stdout=self.stdout_file, stderr=self.stderr_file)

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

        stdout = self.stdout_file.read()
        stderr = self.stderr_file.read()

        if self.timer is not None and not self.process_finished_before_timeout:
            raise QueryTimeoutExceedException('Client timed out!')

        if self.process.returncode != 0 or stderr:
            raise QueryRuntimeException('Client failed! Return code: {}, stderr: {}'.format(self.process.returncode, stderr))

        return stdout
