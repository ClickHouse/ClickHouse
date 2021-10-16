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
    client1.send('DROP DICTIONARY IF EXITS test.dict')
    client1.expect(prompt)
    
    client1.send("CREATE TABLE test.mt (a Int32, b Int32) Engine=MergeTree order by tuple()")
    client1.expect(prompt)
    client1.send("CREATE DICTIONARY test.dict(a Int32, b Int32) PRIMARY KEY a LAYOUT(FLAT()) " + \
                 "SOURCE(CLICKHOUSE(db 'test' table 'mt')) LIFETIME(1)")
    client1.expect(prompt)   
    client1.send("CREATE LIVE VIEW test.lv WITH REFRESH 1 AS SELECT * FROM test.dict")
    client1.expect(prompt)

    client2.send("INSERT INTO test.mt VALUES (1,2)")
    client2.expect(prompt) 

    client1.send('WATCH test.lv FORMAT JSONEachRow')
    client1.expect(r'"_version":"1"')
    
    client2.send("INSERT INTO test.mt VALUES (2,2)")
    client2.expect(prompt) 
    client1.expect(r'"_version":"2"')
    
    client2.send("INSERT INTO test.mt VALUES (3,2)")
    client2.expect(prompt)    
    client1.expect(r'"_version":"3"')
    
    # send Ctrl-C
    client1.send('\x03', eol='')
    match = client1.expect('(%s)|([#\$] )' % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)

    client1.send('DROP TABLE IF EXISTS test.lv')
    client1.expect(prompt)
    client1.send('DROP DICTIONARY IF EXISTS test.dict')
    client1.expect(prompt)
    client1.send('DROP TABLE IF EXISTS test.mt')
    client1.expect(prompt)
    
    

