#pragma once

#include <base/types.h>

namespace DB::PerCPUMemoryBudget
{

/// Per-thread last-seen view of the per-CPU `nallocs` / `nfrees` counters.
/// Lives in `ThreadStatus` so the hot path in `CurrentMemoryTracker`
/// reaches it as an offset off `current_thread`, without an extra TLS
/// resolution.
///
/// Counters are unsigned: signed overflow would be UB and we don't want the
/// optimiser to assume away the wrap. Unsigned wraps cleanly, and the
/// crossing check in `chargeAlloc`/`chargeFree` uses `!=` on the
/// SLICE-shifted bucket so it stays correct across the
/// (~29-year-at-10GB/s/CPU) wrap point.
///
/// Pulled into a tiny standalone header so `ThreadStatus.h` doesn't need to
/// drag in librseq's heavy machinery.
struct PerCPUMemoryBudgetState
{
    int    cpu = -1;     /// CPU we last successfully charged on; -1 = uninitialised
    UInt64 nallocs = 0;  /// last-seen value of `nallocs[cpu]` for this thread
    UInt64 nfrees  = 0;  /// last-seen value of `nfrees[cpu]`
};

}
