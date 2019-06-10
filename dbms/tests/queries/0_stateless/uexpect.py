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
import time
import sys
import re

from threading import Thread
from subprocess import Popen
from Queue import Queue, Empty

class TimeoutError(Exception):
    def __init__(self, timeout):
        self.timeout = timeout

    def __str__(self):
        return 'Timeout %.3fs' % float(self.timeout)

class ExpectTimeoutError(Exception):
    def __init__(self, pattern, timeout, buffer):
        self.pattern = pattern
        self.timeout = timeout
        self.buffer = buffer

    def __str__(self):
        return ('Timeout %.3fs ' % float(self.timeout) +
            'for %s ' % repr(self.pattern.pattern) +
            'buffer %s ' % repr(self.buffer[:]) +
            'or \'%s\'' % ','.join(['%x' % ord(c) for c in self.buffer[:]]))

class IO(object):
    class EOF(object):
        pass

    class Timeout(object):
        pass

    EOF = EOF
    TIMEOUT = Timeout

    class Logger(object):
        def __init__(self, logger, prefix=''):
            self._logger = logger
            self._prefix = prefix

        def write(self, data):
            self._logger.write(('\n' + data).replace('\n','\n' + self._prefix))

        def flush(self):
            self._logger.flush()

    def __init__(self, process, master, queue):
        self.process = process
        self.master = master
        self.queue = queue
        self.buffer = None
        self.before = None
        self.after = None
        self.match = None
        self.pattern = None
        self._timeout = None
        self._logger = None
        self._eol = ''

    def logger(self, logger=None, prefix=''):
        if logger:
            self._logger = self.Logger(logger, prefix=prefix)
        return self._logger

    def timeout(self, timeout=None):
        if timeout:
            self._timeout = timeout
        return self._timeout

    def eol(self, eol=None):
        if eol:
            self._eol = eol
        return self._eol

    def close(self):
        if self._logger:
            self._logger.write('\n')
            self._logger.flush()

    def send(self, data, eol=None):
        if eol is None:
            eol = self._eol
        return self.write(data + eol)

    def write(self, data):
        return os.write(self.master, data)

    def expect(self, pattern, timeout=None, escape=False):
        self.match = None
        self.before = None
        self.after = None
        if escape:
            pattern = re.escape(pattern)
        pattern = re.compile(pattern)
        if timeout is None:
            timeout = self._timeout
        timeleft = timeout
        while timeleft >= 0:
            start_time = time.time()
            try:
                data = self.read(timeout=timeleft, raise_exception=True)
            except TimeoutError:
                if self._logger:
                    self._logger.write(self.buffer + '\n')
                    self._logger.flush()
                exception = ExpectTimeoutError(pattern, timeout, self.buffer)
                self.buffer = None
                raise exception
            timeleft -= (time.time() - start_time)
            if data:
                self.buffer = self.buffer + data if self.buffer else data
            self.match = pattern.search(self.buffer, 0)
            if self.match:
                self.after = self.buffer[self.match.start():self.match.end()]
                self.before = self.buffer[:self.match.start()]
                self.buffer = self.buffer[self.match.end():]
                break
        if self._logger:
            self._logger.write((self.before or '') + (self.after or ''))
            self._logger.flush()
        if self.match is None:
            exception = ExpectTimeoutError(pattern, timeout, self.buffer)
            self.buffer = None
            raise exception
        return self.match

    def read(self, timeout=0, raise_exception=False):
        data = ''
        timeleft = timeout
        try:
            while timeleft >= 0 :
                start_time = time.time()
                data += self.queue.get(timeout=timeleft)
                if data:
                    break
                timeleft -= (time.time() - start_time)
        except Empty:
            if data:
                return data
            if raise_exception:
                raise TimeoutError(timeout)
            pass
        if not data and raise_exception:
            raise TimeoutError(timeout)

        return data

def spawn(command):
    master, slave = pty.openpty()
    process = Popen(command, preexec_fn=os.setsid, stdout=slave, stdin=slave, stderr=slave, bufsize=1)
    os.close(slave)

    queue = Queue()
    thread = Thread(target=reader, args=(process, master, queue))
    thread.daemon = True
    thread.start()

    return IO(process, master, queue)

def reader(process, out, queue):
    while True:
        if process.poll() is not None:
            data = os.read(out)
            queue.put(data)
            break
        data = os.read(out, 65536)
        queue.put(data)

if __name__ == '__main__':
   io = spawn(['/bin/bash','--noediting'])
   prompt = '\$ '
   io.logger(sys.stdout)
   io.timeout(2)
   io.eol('\r')

   io.expect(prompt)
   io.send('clickhouse-client')
   prompt = ':\) '
   io.expect(prompt)
   io.send('SELECT 1')
   io.expect(prompt)
   io.send('SHOW TABLES')
   io.expect('.*\r\n.*')
   io.expect(prompt)
   io.close()
