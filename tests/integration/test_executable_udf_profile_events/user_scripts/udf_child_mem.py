#!/usr/bin/env python3
"""executable UDF that forks a memory-hungry child and waits for it.

Used to validate that peak RSS of a reaped child rolls up into
`ExecutableUserDefinedFunctionPeakMemoryByteSeconds` via wait4 ru_maxrss.
The child allocates ~64 MiB and touches every page so the kernel maps it all;
the parent never touches that memory directly.
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

    pid = os.fork()
    if pid == 0:
        # Child: allocate ~64 MiB and touch every page so VmHWM rises.
        buf = bytearray(64 * 1024 * 1024)
        for i in range(0, len(buf), 4096):
            buf[i] = i & 0xFF
        os._exit(0)
    else:
        # Parent: reap the child so its ru_maxrss rolls up.
        os.waitpid(pid, 0)
        sys.stdout.write(f"{n}\n")
        sys.stdout.flush()
