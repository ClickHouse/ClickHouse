#!/usr/bin/env python3
"""Scenario: peak_rss aggregation across multiple LIVE descendants.

Spawns three helper subprocesses at script startup. Each helper
allocates a ~50 MiB `bytearray` and touches every page so the
allocation shows up in `VmHWM`. The helpers then sleep indefinitely,
so by the time `recordReleased` runs all three are still alive in the
sampler's subtree walk and all three carry the same ~50 MiB resident.

The point of this UDF is to lock the `max`-not-`sum` semantics of
`peak_rss` aggregation: max → derived peak ≈ 50 MiB, sum → derived
peak ≈ 150 MiB. Memory has no `cutime`/`cstime` analogue, so the
helpers MUST be alive when the post-walk runs — `daemon=True` keeps
them tied to the parent's lifetime within the pool slot.
"""

import multiprocessing
import sys
import time


_N_HELPERS = 3
_HELPER_BYTES = 50 * 1024 * 1024  # 50 MiB per helper
_PAGE_SIZE = 4096


def _helper_main(ready_event):
    blob = bytearray(_HELPER_BYTES)
    # Touch every page so the OS commits real pages and `VmHWM` matches
    # the allocation size, not just the virtual reservation.
    for offset in range(0, _HELPER_BYTES, _PAGE_SIZE):
        blob[offset] = 1
    ready_event.set()
    while True:
        time.sleep(60)
        # Use `blob` after the sleep so the optimiser can't elide it.
        _ = blob[0]


_ready = [multiprocessing.Event() for _ in range(_N_HELPERS)]
_procs = [
    multiprocessing.Process(
        target=_helper_main, args=(_ready[i],), daemon=True
    )
    for i in range(_N_HELPERS)
]
for _p in _procs:
    _p.start()
# Block until every helper has finished touching its bytearray.
for _ev in _ready:
    _ev.wait(timeout=30)


for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    n = int(line)
    # Make the per-row wall non-trivial so the borrow wall × peak_rss
    # product is large enough to read back as a meaningful integer.
    time.sleep(0.1)
    sys.stdout.write(f"{n}\n")
    sys.stdout.flush()
