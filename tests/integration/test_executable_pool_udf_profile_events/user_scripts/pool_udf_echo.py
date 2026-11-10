#!/usr/bin/env python3
"""executable_pool UDF that echoes its input.

Used to validate Invocations, ElapsedMicroseconds, InputBytes, OutputBytes
without doing meaningful CPU work.
"""

import sys

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    try:
        n = int(line)
    except ValueError:
        n = 0
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
