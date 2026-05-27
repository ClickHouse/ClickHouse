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
    /// Alloc and free track migration independently: each side remembers the
    /// CPU on which its baseline (`nallocs` / `nfrees`) was captured, so a
    /// migration observed by one side does not falsely mark the other side as
    /// non-migrated on its next op (where its baseline still refers to the
    /// previous CPU's counter and a bucket comparison would be meaningless).
    int    alloc_cpu = -1;    /// CPU on which `nallocs` was last observed; -1 = uninitialised
    int    free_cpu  = -1;    /// CPU on which `nfrees`  was last observed; -1 = uninitialised
    UInt64 nallocs = 0;       /// last-seen value of `nallocs[alloc_cpu]` for this thread
    UInt64 nfrees  = 0;       /// last-seen value of `nfrees[free_cpu]`
    UInt64 pending_alloc = 0; /// bytes accumulated locally since last per-CPU `nallocs` charge
    UInt64 pending_free  = 0; /// bytes accumulated locally since last per-CPU `nfrees`  charge
};

}
