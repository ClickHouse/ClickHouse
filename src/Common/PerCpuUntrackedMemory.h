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

/// Subtract `amount` from slots[cpu], race-free against concurrent rseq
/// adds on the same cpu (uses rseq itself on the librseq path).
///
/// Returns `true` if the subtraction was applied. The caller is then
/// responsible for transferring `amount` to the underlying `MemoryTracker`
/// chain (positive: alloc, negative: free).
///
/// Returns `false` if the rseq sequence aborted because the caller is no
/// longer on `cpu`. The slot is unchanged in this case; the bytes stay
/// accounted in the slot and will be drained by a future overflow on
/// that CPU. Bounded by `cpuCount() * max_untracked_memory`.
bool tryFlush(int cpu, Int64 amount);

/// Relaxed-load read of slots[cpu]. Useful before an un-paired `tryFlush`
/// so the caller knows the amount to subtract.
Int64 peek(int cpu);

/// Sum of slots[i] across all i, with relaxed loads and no reset. O(ncpu).
/// Slow path — intended for `MemoryTracker` to consult under memory pressure.
Int64 peekTotal();

}
