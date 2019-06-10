#!/usr/bin/env python
import imp
import os
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))

uexpect = imp.load_source('uexpect', os.path.join(CURDIR, '..', '..', 'integration', 'helpers', 'uexpect.py'))

def client(name='', command=None):
    if command is None:
        client = uexpect.spawn(os.environ.get('CLICKHOUSE_CLIENT'))
    else:
        client = uexpect.spawn(command)
    client.eol('\r')
    # Note: uncomment this line for debugging
    #client.logger(sys.stdout, prefix=name)
    client.timeout(2)
    return client

prompt = ':\) '
end_of_block = r'.*\r\n.*\r\n'

with client('client1>') as client1, client('client2>', ['bash', '--noediting']) as client2:
    client1.expect(prompt)

    client1.send('DROP TABLE IF EXISTS test.lv')
    client1.expect(prompt)
    client1.send(' DROP TABLE IF EXISTS test.mt')
    client1.expect(prompt)
    client1.send('CREATE TABLE test.mt (a Int32) Engine=MergeTree order by tuple()')
    client1.expect(prompt)
    client1.send('CREATE LIVE VIEW test.lv AS SELECT sum(a) FROM test.mt')
    client1.expect(prompt)

    client2.expect('[\$#] ')
    client2.send('wget -O- -q "http://localhost:8123/?query=WATCH test.lv EVENTS"')
    client2.expect('.*1\tc9d39b11cce79112219a73aaa319b475\r\n')

    client1.send('INSERT INTO test.mt VALUES (1),(2),(3)')
    client1.expect(prompt)

    client2.expect('.*2\t.*\r\n')
    ## send Ctrl-C
    os.kill(client2.process.pid,signal.SIGINT)

    client1.send('DROP TABLE test.lv')
    client1.expect(prompt)
    client1.send('DROP TABLE test.mt')
    client1.expect(prompt)
