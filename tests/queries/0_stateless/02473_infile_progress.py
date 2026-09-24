#!/usr/bin/env python3
# Tags: no-replicated-database, no-parallel, no-fasttest

import os
import signal
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from client import client, end_of_block, prompt

log = None
# uncomment the line below for debugging
# log=sys.stdout

with client(
    name="client>",
    log=log,
    command=os.environ.get("CLICKHOUSE_BINARY", "clickhouse")
    + " client --storage_file_read_method=pread",
) as client1:
    filename = os.environ["CLICKHOUSE_TMP"] + "/infile_progress.tsv"

    client1.expect(prompt)
    client1.send("DROP TABLE IF EXISTS test.infile_progress")
    client1.expect(prompt)
    client1.send(f"SELECT number FROM numbers(5) INTO OUTFILE '{filename}'")
    client1.expect(prompt)
    client1.send(
        "CREATE TABLE test.infile_progress (a Int32) Engine=MergeTree order by tuple()"
    )
    client1.expect(prompt)
    client1.send(f"INSERT INTO test.infile_progress FROM INFILE '{filename}'")
    client1.expect("Progress: 5.00 rows, 10.00 B.*\\)")
    client1.expect(prompt)

    # send Ctrl-C
    client1.send("\x03", eol="")
    match = client1.expect("(%s)|([#\\$] )" % prompt)
    if match.groups()[1]:
        client1.send(client1.command)
        client1.expect(prompt)
    client1.send("DROP TABLE test.infile_progress")
    client1.expect(prompt)

    os.remove(filename)
