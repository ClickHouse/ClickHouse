import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))

sys.path.insert(0, os.path.join(CURDIR))

import uexpect

prompt = ':\) '
end_of_block = r'.*\r\n.*\r\n'

def client(command=None, name='', log=None):
    if command is None:
        client = uexpect.spawn(os.environ.get('CLICKHOUSE_CLIENT'))
    else:
        client = uexpect.spawn(command)
    client.eol('\r')
    client.logger(log, prefix=name)
    client.timeout(20)
    return client
