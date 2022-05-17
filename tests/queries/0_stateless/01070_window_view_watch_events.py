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
    client2.send("SET allow_experimental_window_view = 1")
    client2.expect(prompt)

    client1.send("CREATE DATABASE IF NOT EXISTS 01070_window_view_watch_events")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS 01070_window_view_watch_events.mt NO DELAY")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS 01070_window_view_watch_events.wv NO DELAY")
    client1.expect(prompt)

    client1.send(
        "CREATE TABLE 01070_window_view_watch_events.mt(a Int32, timestamp DateTime('US/Samoa')) ENGINE=MergeTree ORDER BY tuple()"
    )
    client1.expect(prompt)
    client1.send(
        "CREATE WINDOW VIEW 01070_window_view_watch_events.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, tumbleEnd(wid) AS w_end FROM 01070_window_view_watch_events.mt GROUP BY tumble(timestamp, INTERVAL '5' SECOND, 'US/Samoa') AS wid"
    )
    client1.expect(prompt)

    client1.send("WATCH 01070_window_view_watch_events.wv EVENTS")
    client1.expect("Query id" + end_of_block)
    client1.expect("Progress: 0.00 rows.*\)")
    client2.send(
        "INSERT INTO 01070_window_view_watch_events.mt VALUES (1, toDateTime('1990/01/01 12:00:00', 'US/Samoa'));"
    )
    client2.expect("Ok.")
    client2.send(
        "INSERT INTO 01070_window_view_watch_events.mt VALUES (1, toDateTime('1990/01/01 12:00:06', 'US/Samoa'));"
    )
    client2.expect("Ok.")
    client1.expect("1990-01-01 12:00:05" + end_of_block)
    client1.expect("Progress: 1.00 rows.*\)")

    # send Ctrl-C
    client1.send("\x03", eol="")
    match = client1.expect("(%s)|([#\$] )" % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send("DROP TABLE 01070_window_view_watch_events.wv NO DELAY;")
    client1.expect(prompt)
    client1.send("DROP TABLE 01070_window_view_watch_events.mt;")
    client1.expect(prompt)
    client1.send("DROP DATABASE IF EXISTS 01070_window_view_watch_events;")
    client1.expect(prompt)
