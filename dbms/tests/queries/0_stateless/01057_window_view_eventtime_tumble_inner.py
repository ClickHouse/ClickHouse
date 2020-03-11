#!/usr/bin/env python
import os
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, 'helpers'))

from client import client, prompt, end_of_block

log = None
# uncomment the line below for debugging
# log=sys.stdout

with client(name='client1>', log=log) as client1, client(name='client2>', log=log) as client2:
    client1.expect(prompt)
    client2.expect(prompt)

    client1.send('SET allow_experimental_window_view = 1')
    client1.expect(prompt)
    client2.send('SET allow_experimental_window_view = 1')
    client2.expect(prompt)

    client1.send('DROP TABLE IF EXISTS test.mt')
    client1.expect(prompt)
    client1.send(' DROP TABLE IF EXISTS test.dst')
    client1.expect(prompt)
    client1.send(' DROP TABLE IF EXISTS test.`.inner.wv`')
    client1.expect(prompt)
    client1.send(' DROP TABLE IF EXISTS test.wv')
    client1.expect(prompt)

    client1.send('CREATE TABLE test.dst(count UInt64) Engine=MergeTree ORDER BY tuple()')
    client1.expect(prompt)
    client1.send('CREATE TABLE test.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple()')
    client1.expect(prompt)
    client1.send("CREATE WINDOW VIEW test.wv TO test.dst ENGINE=MergeTree ORDER BY tuple() WATERMARK INTERVAL '1' SECOND AS SELECT count(a) AS count FROM test.mt GROUP BY TUMBLE(timestamp, INTERVAL '1' SECOND) AS wid;")
    client1.expect(prompt)
    
    client1.send('WATCH test.wv')
    client1.expect('Progress: 0.00 rows.*\)')
    client2.send('INSERT INTO test.mt VALUES (1, now())')
    client1.expect('1' + end_of_block)
    client1.expect('Progress: 1.00 rows.*\)')

    # send Ctrl-C
    client1.send('\x03', eol='')
    match = client1.expect('(%s)|([#\$] )' % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send('DROP TABLE test.wv')
    client1.expect(prompt)
    client1.send('DROP TABLE test.mt')
    client1.expect(prompt)
    client1.send('DROP TABLE test.dst')
    client1.expect(prompt)
