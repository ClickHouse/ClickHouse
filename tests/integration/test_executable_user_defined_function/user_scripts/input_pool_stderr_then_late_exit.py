#!/usr/bin/python3
# A pooled worker that answers, stays quiet long enough to be handed back to the pool, then writes
# a diagnostic to stderr and exits. Nobody is reading its pipes by then: the query it served is
# over and the process sits idle. The next borrow finds it dead and replaces it - and must report
# what it wrote on the way out rather than drop it with the process.
import os
import sys
import time

QUIET_GAP_SECONDS = float(sys.argv[sys.argv.index("--gap") + 1]) if "--gap" in sys.argv else 0.3

if __name__ == "__main__":
    for line in sys.stdin:
        if not line.strip():
            continue
        sys.stdout.write(f"{os.getpid()}\n")
        sys.stdout.flush()
        time.sleep(QUIET_GAP_SECONDS)
        sys.stderr.write("last words of the worker\n")
        sys.stderr.flush()
        sys.exit(0)
