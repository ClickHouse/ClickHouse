#!/usr/bin/env python3
# Tags: no-parallel

import os
import sys

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

    client1.send("CREATE DATABASE IF NOT EXISTS 01082_window_view_watch_limit")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS 01082_window_view_watch_limit.mt")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS 01082_window_view_watch_limit.wv SYNC")
    client1.expect(prompt)

    client1.send(
        "CREATE TABLE 01082_window_view_watch_limit.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple()"
    )
    client1.expect(prompt)
    client1.send(
        "CREATE WINDOW VIEW 01082_window_view_watch_limit.wv ENGINE Memory WATERMARK=ASCENDING AS SELECT count(a) AS count, hopEnd(wid) AS w_end FROM 01082_window_view_watch_limit.mt GROUP BY hop(timestamp, INTERVAL '2' SECOND, INTERVAL '3' SECOND, 'US/Samoa') AS wid"
    )
    client1.expect("Ok.")
    client1.expect(prompt)

    client1.send("WATCH 01082_window_view_watch_limit.wv LIMIT 1")
    client1.expect("Query id" + end_of_block)
    client1.expect("Progress: 0.00 rows.*\\)")
    client2.send(
        "INSERT INTO 01082_window_view_watch_limit.mt VALUES (1, '1990/01/01 12:00:00');"
    )
    client2.expect("Ok.")
    client2.send(
        "INSERT INTO 01082_window_view_watch_limit.mt VALUES (1, '1990/01/01 12:00:05');"
    )
    client2.expect("Ok.")
    client1.expect("1" + end_of_block)
    client1.expect("Progress: 1.00 rows.*\\)")
    client1.expect("1 row" + end_of_block)
    client1.expect(prompt)

    client1.send("DROP TABLE 01082_window_view_watch_limit.wv SYNC")
    client1.expect(prompt)
    client1.send("DROP TABLE 01082_window_view_watch_limit.mt")
    client1.expect(prompt)
    client1.send("DROP DATABASE IF EXISTS 01082_window_view_watch_limit")
    client1.expect(prompt)
