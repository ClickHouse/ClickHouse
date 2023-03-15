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

    client1.send("CREATE DATABASE 01069_window_view_proc_tumble_watch")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS 01069_window_view_proc_tumble_watch.mt")
    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS 01069_window_view_proc_tumble_watch.wv NO DELAY")
    client1.expect(prompt)

    client1.send(
        "CREATE TABLE 01069_window_view_proc_tumble_watch.mt(a Int32, timestamp DateTime) ENGINE=MergeTree ORDER BY tuple()"
    )
    client1.expect(prompt)
    client1.send(
        "CREATE WINDOW VIEW 01069_window_view_proc_tumble_watch.wv ENGINE Memory AS SELECT count(a) AS count FROM 01069_window_view_proc_tumble_watch.mt GROUP BY tumble(timestamp, INTERVAL '1' SECOND, 'US/Samoa') AS wid;"
    )
    client1.expect(prompt)

    client1.send("WATCH 01069_window_view_proc_tumble_watch.wv")
    client1.expect("Query id" + end_of_block)
    client1.expect("Progress: 0.00 rows.*\)")
    client2.send(
        "INSERT INTO 01069_window_view_proc_tumble_watch.mt VALUES (1, now('US/Samoa') + 3)"
    )
    client2.expect("Ok.")
    client1.expect("1" + end_of_block)
    client1.expect("Progress: 1.00 rows.*\)")
    client2.send(
        "INSERT INTO 01069_window_view_proc_tumble_watch.mt VALUES (1, now('US/Samoa') + 3)"
    )
    client2.expect("Ok.")
    client1.expect("1" + end_of_block)
    client1.expect("Progress: 2.00 rows.*\)")

    # send Ctrl-C
    client1.send("\x03", eol="")
    match = client1.expect("(%s)|([#\$] )" % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send("DROP TABLE 01069_window_view_proc_tumble_watch.wv NO DELAY")
    client1.expect(prompt)
    client1.send("DROP TABLE 01069_window_view_proc_tumble_watch.mt")
    client1.expect(prompt)
    client1.send("DROP DATABASE IF EXISTS 01069_window_view_proc_tumble_watch")
    client1.expect(prompt)
