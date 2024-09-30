#!/usr/bin/env python3
import os
import signal
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from client import client, end_of_block, prompt

log = None
# uncomment the line below for debugging
# log=sys.stdout

with client(name="client1>", log=log) as client1:
    client1.expect(prompt)
    client1.send("SELECT number FROM numbers(1000) FORMAT Null")
    client1.expect("Progress: 1\\.00 thousand rows, 8\\.00 KB .*" + end_of_block)
    client1.expect("0 rows in set. Elapsed: [\\w]{1}\\.[\\w]{3} sec.")
    client1.expect("Peak memory usage: .*B" + end_of_block)
