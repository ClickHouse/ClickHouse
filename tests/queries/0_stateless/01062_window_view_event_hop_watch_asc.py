#!/usr/bin/env python3

import os
import sys
import signal

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from client import client, prompt, end_of_block

log = None
# uncomment the line below for debugging
# log=sys.stdout

with client(name="client1>", log=log) as client1, client(
    name="client2>", log=log
) as client2:
    database_name = os.environ["CLICKHOUSE_DATABASE"]
    client1.expect(prompt)
    client2.expect(prompt)

    client1.send("SET enable_analyzer = 0")
    client1.expect(prompt)
    client1.send("SET allow_experimental_window_view = 1")
    client1.expect(prompt)
    client1.send("SET window_view_heartbeat_interval = 1")
    client1.expect(prompt)
    client2.send("SET allow_experimental_window_view = 1")
    client2.expect(prompt)
    client2.send("SET enable_analyzer = 0")
    client2.expect(prompt)

    client1.send(f"DROP TABLE IF EXISTS {database_name}.mt")
    client1.expect(prompt)
    client1.send(f"DROP TABLE IF EXISTS {database_name}.wv SYNC")
    client1.expect(prompt)

    client1.send(
        f"CREATE TABLE {database_name}.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple()"
    )
    client1.expect(prompt)
    client1.send(
        f"CREATE WINDOW VIEW {database_name}.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, hopEnd(wid) AS w_end FROM {database_name}.mt GROUP BY hop(timestamp, INTERVAL '2' SECOND, INTERVAL '3' SECOND, 'US/Samoa') AS wid"
    )
    client1.expect(prompt)

    client1.send(f"WATCH {database_name}.wv")
    client1.expect("Query id" + end_of_block)
    client1.expect("Progress: 0.00 rows.*\\)")
    client2.send(
        f"INSERT INTO {database_name}.mt VALUES (1, toDateTime('1990/01/01 12:00:00', 'US/Samoa'));"
    )
    client2.expect(prompt)
    client2.send(
        f"INSERT INTO {database_name}.mt VALUES (1, toDateTime('1990/01/01 12:00:05', 'US/Samoa'));"
    )
    client2.expect(prompt)
    client1.expect("1*" + end_of_block)
    client2.send(
        f"INSERT INTO {database_name}.mt VALUES (1, toDateTime('1990/01/01 12:00:06', 'US/Samoa'));"
    )
    client2.expect(prompt)
    client2.send(
        f"INSERT INTO {database_name}.mt VALUES (1, toDateTime('1990/01/01 12:00:10', 'US/Samoa'));"
    )
    client2.expect(prompt)
    client1.expect("1" + end_of_block)
    client1.expect("2" + end_of_block)
    client1.expect("Progress: 3.00 rows.*\\)")

    # send Ctrl-C
    client1.send("\x03", eol="")
    match = client1.expect("(%s)|([#\\$] )" % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send(f"DROP TABLE {database_name}.wv SYNC")
    client1.expect(prompt)
    client1.send(f"DROP TABLE {database_name}.mt")
    client1.expect(prompt)
