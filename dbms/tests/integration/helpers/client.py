import errno
import subprocess as sp
from threading import Timer
import tempfile


class Client:
    def __init__(self, host, port=9000, command='/usr/bin/clickhouse-client'):
        self.host = host
        self.port = port
        self.command = [command, '--host', self.host, '--port', str(self.port)]


    def query(self, sql, stdin=None, timeout=None):
        return QueryRequest(self, sql, stdin, timeout).get_answer()


    def get_query_request(self, sql, stdin=None, timeout=None):
        return QueryRequest(self, sql, stdin, timeout)


class QueryRequest:
    def __init__(self, client, sql, stdin=None, timeout=None):
        self.client = client

        command = self.client.command[:]
        if stdin is None:
            command += ['--multiquery']
            stdin = sql
        else:
            command += ['--query', sql]

        # Write data to tmp file to avoid PIPEs and execution blocking
        stdin_file = tempfile.TemporaryFile()
        stdin_file.write(stdin)
        stdin_file.seek(0)
        self.stdout_file = tempfile.TemporaryFile()
        self.stderr_file = tempfile.TemporaryFile()

        #print " ".join(command), "\nQuery:", sql

        self.process = sp.Popen(command, stdin=stdin_file, stdout=self.stdout_file, stderr=self.stderr_file)

        self.timer = None
        self.process_finished_before_timeout = True
        if timeout is not None:
            def kill_process():
                if self.process.poll() is None:
                    self.process.kill()
                    self.process_finished_before_timeout = False

            self.timer = Timer(timeout, kill_process)
            self.timer.start()


    def get_answer(self):
        self.process.wait()
        self.stdout_file.seek(0)
        self.stderr_file.seek(0)

        stdout = self.stdout_file.read()
        stderr = self.stderr_file.read()

        if self.process.returncode != 0 or stderr:
            raise Exception('Client failed! Return code: {}, stderr: {}'.format(self.process.returncode, stderr))

        if self.timer is not None and not self.process_finished_before_timeout:
            raise Exception('Client timed out!')

        return stdout

