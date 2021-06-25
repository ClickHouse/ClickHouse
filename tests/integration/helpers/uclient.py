import os
import sys
import time

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))

import uexpect

prompt = ':\) '
end_of_block = r'.*\r\n.*\r\n'

class client(object):
    def __init__(self, command=None, name='', log=None):
        self.client = uexpect.spawn(['/bin/bash','--noediting'])
        if command is None:
            command = '/usr/bin/clickhouse-client'
        self.client.command = command
        self.client.eol('\r')
        self.client.logger(log, prefix=name)
        self.client.timeout(20)
        self.client.expect('[#\$] ', timeout=2)
        self.client.send(command)

    def __enter__(self):
        return self.client.__enter__()

    def __exit__(self, type, value, traceback):
        self.client.reader['kill_event'].set()
        # send Ctrl-C
        self.client.send('\x03', eol='')
        time.sleep(0.3)
        self.client.send('quit', eol='\r')
        self.client.send('\x03', eol='')
        return self.client.__exit__(type, value, traceback)
