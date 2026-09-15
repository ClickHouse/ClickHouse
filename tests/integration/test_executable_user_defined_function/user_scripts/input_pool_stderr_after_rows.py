#!/usr/bin/python3

# A pooled command that answers its rows and then, immediately, writes a diagnostic to `stderr`.
#
# Configured with `stderr_reaction` `throw`, which promises that anything the command writes to
# `stderr` fails the query. A pooled worker that satisfied the row count is handed straight back to
# the pool without being waited for, so this is the one path where nothing looks at its `stderr`
# again - the probe that refuses to pool a dirty worker runs after the query has already succeeded.
# The setting would then cost the command a worker while telling the user nothing, which is not what
# it says it does.
#
# Written right after the rows, with no pause: what the server can act on is what has already
# arrived by the time it looks. Output that lands later is beyond reach without making every
# successful pooled call wait for output that usually never comes.

import sys

if __name__ == "__main__":
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        sys.stdout.write(f"Key {line}\n")
        sys.stdout.flush()

        sys.stderr.write("complaining right after the rows\n")
        sys.stderr.flush()
