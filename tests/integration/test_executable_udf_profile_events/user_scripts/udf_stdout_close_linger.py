#!/usr/bin/env python3
"""executable UDF that closes stdout after processing all rows, then lingers.

Used to validate the `check_exit_code=false` non-blocking-cleanup contract:
  - ClickHouse sees stdout EOF and considers the UDF done, so the pipeline
    completes without waiting for the child process to exit.
  - `tryReapWithoutStatusCheck` returns false (child still running), so
    `executableFinished` stays false and no wait4 rusage is captured.
  - Despite no reap, `ExecutableUserDefinedFunctionInputBytes` and
    `ExecutableUserDefinedFunctionOutputBytes` are still reported because those
    counters are recorded by the pipe buffers as data streams through, independent
    of child reap status.
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

# Close stdout explicitly so ClickHouse sees EOF on the output pipe and
# considers the UDF invocation complete, while the process itself lingers.
os.close(1)

time.sleep(30)
