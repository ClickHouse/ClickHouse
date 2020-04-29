#!/usr/bin/env python
import os
import sys
import time
import signal
import requests

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, 'helpers'))

from httpechoserver import start_server, HTTP_SERVER_URL_STR
from client import client, prompt, end_of_block

log = None
# uncomment the line below for debugging
#log=sys.stdout

server = start_server(9)
server.start()

try:
    for output_format in ['CSV', 'JSONEachRow', 'JSONEachRowWithProgress']:
        with client(name='client1>', log=log) as client1, client(name='client2>', log=log) as client2:
            client1.expect(prompt)
            client2.expect(prompt)

            client1.send('SET allow_experimental_live_view = 1')
            client1.expect(prompt)

            client1.send('DROP TABLE IF EXISTS test.lv')
            client1.expect(prompt)
            client1.send('DROP TABLE IF EXISTS test.mt')
            client1.expect(prompt)
            client1.send('CREATE TABLE test.mt (a Int32) Engine=MergeTree order by tuple()')
            client1.expect(prompt)
            client1.send("CREATE TABLE test.url (s Int64, _version UInt64) Engine=URL('%s', %s)" % (HTTP_SERVER_URL_STR, output_format))
            client1.expect(prompt)
            client1.send('CREATE LIVE VIEW test.lv TO test.url AS SELECT sum(a) AS s FROM test.mt')
            client1.expect(prompt)
            time.sleep(0.25)
            sys.stdout.write("-- first insert --\n")
            sys.stdout.write(server.out.read() + "\n")

            client2.send('INSERT INTO test.mt VALUES (1),(2),(3)')
            client2.expect(prompt)
            time.sleep(0.25)
            sys.stdout.write("-- second insert --\n")
            sys.stdout.write(server.out.read() + "\n")

            client2.send('INSERT INTO test.mt VALUES (4),(5),(6)')
            client2.expect(prompt)
            time.sleep(0.25)
            sys.stdout.write("-- third insert --\n")
            sys.stdout.write(server.out.read() + "\n")

            client1.send('DROP TABLE test.lv')
            client1.expect(prompt)
            client1.send('DROP TABLE test.url')
            client1.expect(prompt)
            client1.send('DROP TABLE test.mt')
            client1.expect(prompt)
finally:
    try:
        for i in range(9):
            requests.post(HTTP_SERVER_URL_STR, data=b"0\r\n", timeout=1)
    except Exception:
        pass
    finally:
        server.join()
