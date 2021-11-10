import subprocess
import threading
import os


class ClickHouseClient:
    def __init__(self, binary_path, port):
        self.binary_path = binary_path
        self.port = port

    def query(self, query, timeout=10, pipe=None):

        result = []
        process = []

        def run(path, port, text, result, in_pipe, process):

            if in_pipe is None:
                in_pipe = subprocess.PIPE

            pipe = subprocess.Popen([path, 'client', '--port', str(port), '-q', text],
                                    stdin=in_pipe, stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True)
            process.append(pipe)
            stdout_data, stderr_data = pipe.communicate()

            if stderr_data:
                raise Exception('Error while executing query: {}\nstdout:\n{}\nstderr:\n{}'
                                .format(text, stdout_data, stderr_data))

            result.append(stdout_data)

        thread = threading.Thread(target=run, args=(self.binary_path, self.port, query, result, pipe, process))
        thread.start()
        thread.join(timeout)
        if thread.isAlive():
            if len(process):
                process[0].kill()
            thread.join()
            raise Exception('timeout exceed for query: ' + query)

        if len(result):
            return result[0]
