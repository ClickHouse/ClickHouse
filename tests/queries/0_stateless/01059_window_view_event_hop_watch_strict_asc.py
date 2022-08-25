#!/usr/bin/env python3
# Tags: no-parallel

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
    client1.expect(prompt)
    client2.expect(prompt)

    client1.send("SET allow_experimental_window_view = 1")
    client1.expect(prompt)
    client1.send("SET window_view_heartbeat_interval = 1")
    client1.expect(prompt)

    client1.send("CREATE DATABASE IF NOT EXISTS db_01059_event_hop_watch_strict_asc")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS db_01059_event_hop_watch_strict_asc.mt")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS db_01059_event_hop_watch_strict_asc.wv NO DELAY")
    client1.expect(prompt)

    client1.send(
        "CREATE TABLE db_01059_event_hop_watch_strict_asc.mt(a Int32, timestamp DateTime('US/Samoa')) ENGINE=MergeTree ORDER BY tuple()"
    )
    client1.expect(prompt)
    client1.send(
        "CREATE WINDOW VIEW db_01059_event_hop_watch_strict_asc.wv ENGINE Memory WATERMARK=STRICTLY_ASCENDING AS SELECT count(a) AS count, hopEnd(wid) as w_end FROM db_01059_event_hop_watch_strict_asc.mt GROUP BY hop(timestamp, INTERVAL '2' SECOND, INTERVAL '3' SECOND, 'US/Samoa') AS wid;"
    )
    client1.expect(prompt)

    client1.send("WATCH db_01059_event_hop_watch_strict_asc.wv")
    client1.expect("Query id" + end_of_block)
    client1.expect("Progress: 0.00 rows.*\)")
    client2.send(
        "INSERT INTO db_01059_event_hop_watch_strict_asc.mt VALUES (1, toDateTime('1990/01/01 12:00:00', 'US/Samoa'));"
    )
    client2.expect("Ok.")
    client2.send(
        "INSERT INTO db_01059_event_hop_watch_strict_asc.mt VALUES (1, toDateTime('1990/01/01 12:00:05', 'US/Samoa'));"
    )
    client2.expect("Ok.")
    client1.expect("1*1990-01-01 12:00:02" + end_of_block)
    client1.expect("Progress: 1.00 rows.*\)")

    client2.send(
        "INSERT INTO db_01059_event_hop_watch_strict_asc.mt VALUES (1, toDateTime('1990/01/01 12:00:10', 'US/Samoa'));"
    )
    client2.expect("Ok.")
    client1.expect("1*1990-01-01 12:00:06" + end_of_block)
    client1.expect("1*1990-01-01 12:00:08" + end_of_block)
    client1.expect("Progress: 3.00 rows.*\)")

    # send Ctrl-C
    client1.send("\x03", eol="")
    match = client1.expect("(%s)|([#\$] )" % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send("DROP TABLE db_01059_event_hop_watch_strict_asc.wv NO DELAY")
    client1.expect(prompt)
    client1.send("DROP TABLE db_01059_event_hop_watch_strict_asc.mt")
    client1.expect(prompt)
    client1.send("DROP DATABASE IF EXISTS db_01059_event_hop_watch_strict_asc")
    client1.expect(prompt)
