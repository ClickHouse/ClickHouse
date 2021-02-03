import subprocess
import threading
import socket
import time


class ClickHouseServer:
    def __init__(self, binary_path, config_path, stdout_file=None, stderr_file=None, shutdown_timeout=10):
        self.binary_path = binary_path
        self.config_path = config_path
        self.pipe = None
        self.stdout_file = stdout_file
        self.stderr_file = stderr_file
        self.shutdown_timeout = shutdown_timeout

    def start(self):
        cmd = [self.binary_path, 'server', '--config', self.config_path]
        out_pipe = None
        err_pipe = None
        if self.stdout_file is not None:
            out_pipe = open(self.stdout_file, 'w')
        if self.stderr_file is not None:
            err_pipe = open(self.stderr_file, 'w')
        self.pipe = subprocess.Popen(cmd, stdout=out_pipe, stderr=err_pipe)

    def wait_for_request(self, port, timeout=1):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            # is not working
            # s.settimeout(timeout)

            step = 0.01
            for iter in range(int(timeout / step)):
                if s.connect_ex(('localhost', port)) == 0:
                    return
                time.sleep(step)

            s.connect(('localhost', port))
        except socket.error as socketerror:
            print("Error: ", socketerror)
            raise

    def shutdown(self, timeout=10):

        def wait(pipe):
            pipe.wait()

        if self.pipe is not None:
            self.pipe.terminate()
            thread = threading.Thread(target=wait, args=(self.pipe,))
            thread.start()
            thread.join(timeout)
            if thread.isAlive():
                self.pipe.kill()
                thread.join()

            if self.pipe.stdout is not None:
                self.pipe.stdout.close()
            if self.pipe.stderr is not None:
                self.pipe.stderr.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, type, value, traceback):
        self.shutdown(self.shutdown_timeout)
