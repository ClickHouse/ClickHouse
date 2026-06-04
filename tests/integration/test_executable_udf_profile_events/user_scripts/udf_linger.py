#!/usr/bin/env python3
"""executable UDF that echoes input, closes stdout, then lingers.

Regression fixture: check_exit_code=false means the source must NOT block
waiting for child exit. This child stops producing output (closes stdout)
but keeps running, so a blocking reap in cleanup would hang the query; the
bounded ~ShellCommand teardown must terminate it instead.
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
os.close(sys.stdout.fileno())  # signal EOF so the query's pipeline finishes
time.sleep(120)  # linger well past command_termination_timeout
