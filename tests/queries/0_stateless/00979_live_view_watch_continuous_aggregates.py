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
    client1.send('DROP TABLE IF EXISTS test.mt')
    client1.expect(prompt)
    client1.send('CREATE TABLE test.mt (time DateTime, location String, temperature UInt32) Engine=MergeTree order by tuple()')
    client1.expect(prompt)
    client1.send('CREATE LIVE VIEW test.lv AS SELECT toStartOfDay(time) AS day, location, avg(temperature) FROM test.mt GROUP BY day, location ORDER BY day, location')
    client1.expect(prompt)
    client1.send('WATCH test.lv FORMAT CSVWithNames')
    client1.expect(r'_version')
    client2.send("INSERT INTO test.mt VALUES ('2019-01-01 00:00:00','New York',60),('2019-01-01 00:10:00','New York',70)")
    client2.expect(prompt)
    client1.expect(r'"2019-01-01 00:00:00","New York",65')
    client2.send("INSERT INTO test.mt VALUES ('2019-01-01 00:00:00','Moscow',30),('2019-01-01 00:10:00', 'Moscow', 40)")
    client2.expect(prompt)
    client1.expect(r'"2019-01-01 00:00:00","Moscow",35')
    client1.expect(r'"2019-01-01 00:00:00","New York",65')
    client2.send("INSERT INTO test.mt VALUES ('2019-01-02 00:00:00','New York',50),('2019-01-02 00:10:00','New York',60)")
    client2.expect(prompt)
    client1.expect(r'"2019-01-01 00:00:00","Moscow",35')
    client1.expect(r'"2019-01-01 00:00:00","New York",65')
    client1.expect(r'"2019-01-02 00:00:00","New York",55')
    client2.send("INSERT INTO test.mt VALUES ('2019-01-02 00:00:00','Moscow',20),('2019-01-02 00:10:00', 'Moscow', 30)")
    client2.expect(prompt)
    client1.expect(r'"2019-01-01 00:00:00","Moscow",35')
    client1.expect(r'"2019-01-01 00:00:00","New York",65')
    client1.expect(r'"2019-01-02 00:00:00","Moscow",25')
    client1.expect(r'"2019-01-02 00:00:00","New York",55')
    client2.send("INSERT INTO test.mt VALUES ('2019-01-02 00:03:00','New York',40),('2019-01-02 00:06:00','New York',30)")
    client2.expect(prompt)
    client1.expect(r'"2019-01-01 00:00:00","Moscow",35')
    client1.expect(r'"2019-01-01 00:00:00","New York",65')
    client1.expect(r'"2019-01-02 00:00:00","Moscow",25')
    client1.expect(r'"2019-01-02 00:00:00","New York",45')
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
