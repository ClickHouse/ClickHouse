#!/usr/bin/env python3
import os
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, 'helpers'))

from client import client, prompt, end_of_block

log = None
# uncomment the line below for debugging
#log=sys.stdout

with client(name='client1>', log=log) as client1, client(name='client2>', log=log) as client2:
    client1.expect(prompt)
    client2.expect(prompt)

    client1.send('SET allow_experimental_live_view = 1')
    client1.expect(prompt)
    client2.send('SET allow_experimental_live_view = 1')
    client2.expect(prompt)

    client1.send('DROP TABLE IF EXISTS test.lv')
    client1.expect(prompt)
    client1.send(' DROP TABLE IF EXISTS test.mt')
    client1.expect(prompt)
    client1.send('SET live_view_heartbeat_interval=1')
    client1.expect(prompt)
    client1.send('CREATE TABLE test.mt (a Int32) Engine=MergeTree order by tuple()')
    client1.expect(prompt)
    client1.send('CREATE LIVE VIEW test.lv WITH TIMEOUT AS SELECT sum(a) FROM test.mt')
    client1.expect(prompt)
    client1.send('WATCH test.lv')
    client1.expect('_version')
    client1.expect(r'0.*1' + end_of_block)
    client2.send('INSERT INTO test.mt VALUES (1),(2),(3)')
    client1.expect(r'6.*2' + end_of_block)
    # wait for heartbeat
    client1.expect('Progress: 2.00 rows.*\)')
    # send Ctrl-C
    client1.send('\x03', eol='')
    match = client1.expect('(%s)|([#\$] )' % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send('DROP TABLE test.lv')
    client1.expect(prompt)
    client1.send('DROP TABLE test.mt')
    client1.expect(prompt)
