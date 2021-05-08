#!/usr/bin/env python3
import os
import sys
import time
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, 'helpers'))

from client import client, prompt, end_of_block

log = None
# uncomment the line below for debugging
#log=sys.stdout

with client(name='client1>', log=log) as client1, client(name='client2>', log=log) as client2, client(name='client3>', log=log) as client3:
    client1.expect(prompt)
    client2.expect(prompt)
    client3.expect(prompt)

    client1.send('SET allow_experimental_live_view = 1')
    client1.expect(prompt)
    client3.send('SET allow_experimental_live_view = 1')
    client3.expect(prompt)

    client1.send('DROP TABLE IF EXISTS test.lv')
    client1.expect(prompt)
    client1.send('DROP TABLE IF EXISTS test.lv_sums')
    client1.expect(prompt)
    client1.send('DROP TABLE IF EXISTS test.mt')
    client1.expect(prompt)
    client1.send('DROP TABLE IF EXISTS test.sums')
    client1.expect(prompt)
    client1.send('CREATE TABLE test.mt (a Int32) Engine=MergeTree order by tuple()')
    client1.expect(prompt)
    client1.send('CREATE LIVE VIEW test.lv AS SELECT sum(a) AS s FROM test.mt')
    client1.expect(prompt)
    client1.send('CREATE TABLE test.sums (s Int32, version Int32) Engine=MergeTree ORDER BY tuple()')
    client1.expect(prompt)
    client3.send('CREATE LIVE VIEW test.lv_sums AS SELECT * FROM test.sums ORDER BY version')
    client3.expect(prompt)

    client3.send("WATCH test.lv_sums FORMAT CSVWithNames")

    client1.send('INSERT INTO test.sums WATCH test.lv')
    client1.expect(r'INSERT INTO')

    client3.expect('0,1.*\r\n')

    client2.send('INSERT INTO test.mt VALUES (1),(2),(3)')
    client2.expect(prompt)
    client3.expect('6,2.*\r\n')

    client2.send('INSERT INTO test.mt VALUES (4),(5),(6)')
    client2.expect(prompt)
    client3.expect('21,3.*\r\n')

    # send Ctrl-C
    client3.send('\x03', eol='')
    match = client3.expect('(%s)|([#\$] )' % prompt)
    if match.groups()[1]:
        client3.send(client3.command)
        client3.expect(prompt)

    # send Ctrl-C
    client1.send('\x03', eol='')
    match = client1.expect('(%s)|([#\$] )' % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)

    client2.send('DROP TABLE test.lv')
    client2.expect(prompt)
    client2.send('DROP TABLE test.lv_sums')
    client2.expect(prompt)
    client2.send('DROP TABLE test.sums')
    client2.expect(prompt)
    client2.send('DROP TABLE test.mt')
    client2.expect(prompt)
