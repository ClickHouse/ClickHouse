import errno
import subprocess as sp
from threading import Timer


class Client:
    def __init__(self, host, port=9000, command='/usr/bin/clickhouse-client'):
        self.host = host
        self.port = port
        self.command = [command, '--host', self.host, '--port', str(self.port)]

    def query(self, sql, stdin=None, timeout=10.0):
        if stdin is None:
            command = self.command + ['--multiquery']
            stdin = sql
        else:
            command = self.command + ['--query', sql]

        process = sp.Popen(command, stdin=sp.PIPE, stdout=sp.PIPE, stderr=sp.PIPE)

        timer = None
        if timeout is not None:
            def kill_process():
                try:
                    process.kill()
                except OSError as e:
                    if e.errno != errno.ESRCH:
                        raise

            timer = Timer(timeout, kill_process)
            timer.start()

        stdout, stderr = process.communicate(stdin)

        if timer is not None:
            if timer.finished.is_set():
                raise Exception('Client timed out!')
            else:
                timer.cancel()

        if process.returncode != 0:
            raise Exception('Client failed! return code: {}, stderr: {}'.format(process.returncode, stderr))

        return stdout
