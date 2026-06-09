#!/usr/bin/env python3
"""executable UDF that allocates memory after closing stdout.

Verifies that `ExecutableUserDefinedFunctionPeakMemoryByteSeconds` does NOT
observe memory the function allocates after closing stdout (output-phase contract).
The child writes its output, closes stdout, then allocates ~256 MiB and touches
every page to force RSS. Because sampling stops at stdout EOF, the post-close
allocation is excluded from the peak even though elapsed time and CPU still cover
that interval.
"""
import os
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

sys.stdout.flush()
os.close(1)
buf = bytearray(256 * 1024 * 1024)
for i in range(0, len(buf), 4096):
    buf[i] = i & 0xFF
os._exit(0)
