#pragma once

#include <base/types.h>

/// Per-CPU untracked memory counters.
///
/// Replacement for the per-thread `untracked_memory` accumulator used by
/// `CurrentMemoryTracker`. One Int64 slot per CPU instead of per thread, so
/// the worst-case unflushed window scales with `ncpu` rather than `nthreads`.
///
/// Two backends, picked at build time:
///   1. librseq (preferred when USE_LIBRSEQ=1 is defined and rseq_init
///      succeeds at startup): non-atomic per-CPU RMW via restartable
///      sequences. Migration mid-CS is detected by the kernel and the CS
///      is restarted. No `lock` prefix.
///   2. Fallback: `__atomic_add_fetch` on the slot, with the CPU index
///      resolved via `sched_getcpu`. Single-CPU uncontended atomic — one
///      `lock xadd` (x86_64) or `ldadd` (aarch64 LSE), L1-resident slot.
///
/// All overflow/limit policy stays in the caller — the per-slot threshold
/// is the same `untracked_memory_limit` the legacy path uses.
namespace DB::PerCpuUntrackedMemory
{

/// True iff initialization succeeded (slot array allocated; if librseq is
/// linked in, also that `rseq_init` reported a working registration).
/// Callers must check this before using the API.
bool isEnabled();

/// Number of slots, equal to `sysconf(_SC_NPROCESSORS_CONF)` at startup.
/// Fixed for the lifetime of the process.
int cpuCount();

/// Result of `add`: the slot that was actually updated, plus an approximate
/// new value (may underestimate slightly if a concurrent `add` on the same
/// CPU completed between our commit and the read-back; the caller uses this
/// only to decide whether to drain, where small drift is harmless).
struct AddResult
{
    int cpu;
    Int64 new_local;
};

/// Resolve the current CPU, perform an RMW on its slot, and return both the
/// CPU used and the post-update value. Pair `drain(result.cpu)` with this
/// call so that the drain hits the slot that received the bytes — critical
/// because the CPU id can change between two un-paired calls.
AddResult add(Int64 delta);

/// Atomically exchanges slots[cpu] with 0; returns the prior value. The CPU
/// index must be supplied by the caller (typically from a paired `add`, or
/// from `currentCpu()` for un-paired flushes).
Int64 drain(int cpu);

/// Sum of slots[i] across all i, with relaxed loads and no reset. O(ncpu).
/// Slow path — intended for `MemoryTracker` to consult under memory pressure.
Int64 peekTotal();

/// Resolve the current CPU index without performing a slot operation.
/// Useful for un-paired `drain` sites (e.g. `flushUntrackedMemory` on
/// blocker-level change or thread exit) where there is no preceding `add`.
int currentCpu();

}
