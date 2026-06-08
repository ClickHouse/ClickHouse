#!/usr/bin/env python3
"""executable UDF that allocates and holds memory after closing stdout.

Verifies that `ExecutableUserDefinedFunctionPeakMemoryByteSeconds` observes memory
the function holds after it has written every expected row and closed stdout. The
child writes its output, closes stdout (signalling end of output), then allocates
~256 MiB and holds it for two seconds — far longer than the sampler's end-of-stream
cadence — so the peak is observed while the blocking reap still attributes wall time
and CPU to the same invocation. `check_exit_code` is left at its default (`true`):
the child exits 0 after the hold.
"""
import os
import sys
import time

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
time.sleep(2)
os._exit(0)
