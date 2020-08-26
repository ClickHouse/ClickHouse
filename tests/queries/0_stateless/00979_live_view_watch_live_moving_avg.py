#!/usr/bin/env python
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
    client1.send('CREATE TABLE test.mt (a Int32, id Int32) Engine=Memory')
    client1.expect(prompt)
    client1.send('CREATE LIVE VIEW test.lv AS SELECT sum(a)/2 FROM (SELECT a, id FROM ( SELECT a, id FROM test.mt ORDER BY id DESC LIMIT 2 ) ORDER BY id DESC LIMIT 2)') 
    client1.expect(prompt)
    client1.send('WATCH test.lv')
    client1.expect('_version')
    client1.expect(r'0.*1' + end_of_block)
    client2.send('INSERT INTO test.mt VALUES (1, 1),(2, 2),(3, 3)')
    client1.expect(r'2\.5.*2' + end_of_block)
    client2.expect(prompt)
    client2.send('INSERT INTO test.mt VALUES (4, 4),(5, 5),(6, 6)')
    client1.expect(r'5\.5.*3' + end_of_block)
    client2.expect(prompt)
    for v, i in enumerate(range(7,129)):
       client2.send('INSERT INTO test.mt VALUES (%d, %d)' % (i, i))
       client1.expect(r'%.1f.*%d' % (i-0.5, 4+v) + end_of_block)
       client2.expect(prompt)
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
