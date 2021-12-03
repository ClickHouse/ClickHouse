#!/usr/bin/env python3

from subprocess import Popen, PIPE, STDOUT
import sys
import os


# Very simple tee logic implementation. You can specify shell command, output
# logfile and env variables. After TeePopen is created you can only wait until
# it finishes. stderr and stdout will be redirected both to specified file and
# stdout.
class TeePopen:
    # pylint: disable=W0102
    def __init__(self, command, log_file, env=os.environ.copy()):
        self.command = command
        self.log_file = log_file
        self.env = env

    def __enter__(self):
        # pylint: disable=W0201
        self.process = Popen(self.command, shell=True, universal_newlines=True, env=self.env, stderr=STDOUT, stdout=PIPE, bufsize=1)
        self.log_file = open(self.log_file, 'w', encoding='utf-8')
        return self

    def __exit__(self, t, value, traceback):
        for line in self.process.stdout:
            sys.stdout.write(line)
            self.log_file.write(line)

        self.process.wait()
        self.log_file.close()

    def wait(self):
        for line in self.process.stdout:
            sys.stdout.write(line)
            self.log_file.write(line)

        return self.process.wait()
