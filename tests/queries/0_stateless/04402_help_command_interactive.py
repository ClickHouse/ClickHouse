#!/usr/bin/env python3
# Exercises the `help` meta-command on the positive interactive `clickhouse-client` path (the
# `is_interactive` gate), which the local-only checks in 04401_help_command.sh do not cover.
# https://github.com/ClickHouse/ClickHouse/issues/89377

import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from client import client, prompt

log = None
# Uncomment for debugging:
# log = sys.stdout

with client(name="client1>", log=log) as client1:
    client1.expect(prompt)

    # `help <name>` renders the embedded documentation; the banner carries the entity name and type.
    client1.send("help max_threads")
    client1.expect(r"\(Setting\)")
    client1.expect(prompt)

    # An unknown word reports that nothing was found.
    client1.send("help nonexistent_entity_xyzzy")
    client1.expect("No documentation found for 'nonexistent_entity_xyzzy'")
    client1.expect(prompt)
