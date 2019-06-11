#!/usr/bin/env python
import imp
import os
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))

uexpect = imp.load_source('uexpect', os.path.join(CURDIR, 'helpers', 'uexpect.py'))

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
    client2.send('wget -O- -q "http://localhost:8123/?live_view_heartbeat_interval=1&query=WATCH test.lv EVENTS FORMAT JSONEachRowWithProgress"')
    client2.expect('{"progress":{"read_rows":"1","read_bytes":"49","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}}\r\n', escape=True)
    client2.expect('{"row":{"version":"1","hash":"c9d39b11cce79112219a73aaa319b475"}}', escape=True)
    client2.expect('{"progress":{"read_rows":"1","read_bytes":"49","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}}', escape=True)
    # heartbeat is provided by progress message
    client2.expect('{"progress":{"read_rows":"1","read_bytes":"49","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}}', escape=True)

    client1.send('INSERT INTO test.mt VALUES (1),(2),(3)')
    client1.expect(prompt)

    client2.expect('{"row":{"version":"2","hash":"4cd0592103888d4682de9a32a23602e3"}}\r\n', escape=True)

    ## send Ctrl-C
    os.kill(client2.process.pid,signal.SIGINT)

    client1.send('DROP TABLE test.lv')
    client1.expect(prompt)
    client1.send('DROP TABLE test.mt')
    client1.expect(prompt)
