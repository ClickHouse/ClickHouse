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
///      sequences. Both `add` and `tryFlush` use rseq, so they are
///      race-free against each other on the same CPU — the kernel
///      restarts whoever is preempted. Cross-CPU `tryFlush` calls abort
///      cleanly without modifying the slot.
///   2. Fallback: atomic ops on the slot. CPU index resolved via
///      `sched_getcpu`. Both `add` and `tryFlush` are single instructions
///      (`lock xadd` on x86_64, `ldadd`/`ldaddal` on aarch64 LSE) — the
///      slot is L1-resident and uncontended.
///
/// All overflow/limit policy stays in the caller.
namespace DB::PerCpuUntrackedMemory
{

bool isEnabled();
int  cpuCount();

/// Resolve the current CPU index without performing a slot operation.
/// Useful for un-paired drain sites (e.g. `flushUntrackedMemory`).
int currentCpu();

struct AddResult
{
    int   cpu;        /// the slot that was actually updated
    Int64 new_local;  /// approximate value of slots[cpu] after the add
};

/// Resolve the current CPU, perform a per-CPU RMW on its slot, and return
/// both the CPU used and the post-update value. Pair `tryFlush(result.cpu,
/// result.new_local)` with this call to flush on overflow.
AddResult add(Int64 delta);

/// Paired with `add`: subtract `amount` from slots[cpu] via the same rseq
/// primitive. Race-free against concurrent rseq adds on `cpu`. Returns
/// `false` if the rseq sequence aborted because the caller migrated off
/// `cpu`; the slot is unchanged in that case and the bytes stay accounted
/// in the slot until a future overflow on that cpu drains them. Bounded
/// by `cpuCount() * max_untracked_memory`.
bool tryFlush(int cpu, Int64 amount);

/// Un-paired drain: atomically swap slots[cpu] with 0 and return the prior
/// value. Always succeeds. The caller is expected to be running on `cpu`
/// itself (typically resolved via `currentCpu()` immediately before this
/// call), so concurrent rseq adds from other threads on the same CPU are
/// serialized by the kernel and cannot race.
Int64 drain(int cpu);

/// Sum of slots[i] across all i, with relaxed loads and no reset. O(ncpu).
/// Slow path — intended for `MemoryTracker` to consult under memory pressure.
Int64 peekTotal();

}
