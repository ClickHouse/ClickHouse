import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))

import uexpect

prompt = ':\) '
end_of_block = r'.*\r\n.*\r\n'

def client(command=None, name='', log=None):
    client = uexpect.spawn(['/bin/bash','--noediting'])
    if command is None:
        command = os.environ.get('CLICKHOUSE_BINARY', 'clickhouse') + '-client'
    client.eol('\r')
    client.logger(log, prefix=name)
    client.timeout(20)
    client.expect('[#\$] ', timeout=2)
    client.send(command)
    return client
