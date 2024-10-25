#!/usr/bin/env python3
# Tags: no-parallel, no-fasttest

import os
import signal
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from client import client, end_of_block, prompt

log = None
# uncomment the line below for debugging
# log=sys.stdout

TMP_FILE = os.path.join(
    os.environ.get("CLICKHOUSE_TMP", "/tmp"),
    os.path.basename(os.path.abspath(__file__)) + ".hist",
)

with client(
    name="client1>",
    log=log,
    extra_options={"history_file": TMP_FILE, "history_max_entries": 2},
) as client:
    client.expect(prompt)
    client.send("SELECT 1")
    client.expect(prompt)
    client.send("SELECT 2")
    client.expect(prompt)
    client.send("SELECT 3")
    client.expect(prompt)

with open(TMP_FILE, "r") as f:
    for line in f:
        if not line.startswith("###"):
            print(line, end="")
