#!/usr/bin/env python3
"""executable_pool UDF that sleeps for the float passed in.

Used to validate that ElapsedMicroseconds and PoolWaitMicroseconds reflect
real wall time. With pool_size=1 and two concurrent queries, the second
query's PoolWaitMicroseconds should be > 0.
"""

import sys
import time

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        seconds = float(line)
    except ValueError:
        seconds = 0.0
    if seconds > 0:
        time.sleep(seconds)
    sys.stdout.write("1\n")
    sys.stdout.flush()
