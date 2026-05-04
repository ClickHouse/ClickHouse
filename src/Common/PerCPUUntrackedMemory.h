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
///      sequences. The `track` op fuses "check overflow → drain → add" into
///      a single rseq CS by passing `delta - current` as the rseq increment.
///   2. Fallback: atomic ops on the slot via a CAS loop with the same
///      check-or-drain semantics. Single-CPU uncontended.
///
/// All limit policy stays in the caller — `track(delta, limit)` is told the
/// per-CPU threshold; the caller picks it (e.g. from `untracked_memory_limit`).
namespace DB::PerCPUUntrackedMemory
{

bool isEnabled();
int  cpuCount();

/// Resolve the current CPU index without performing a slot operation.
/// Useful for un-paired drain sites (e.g. `flushUntrackedMemory`).
int currentCPU();

struct TrackResult
{
    int   cpu;       /// the slot that was actually updated
    Int64 to_flush;  /// signed bytes the caller must transfer to the parent
                     /// tracker before returning to user code:
                     ///   > 0 → memory_tracker->allocImpl(to_flush)
                     ///   < 0 → memory_tracker->free(-to_flush)
                     ///   = 0 → nothing further; the delta is just buffered
};

/// Add `delta` to the current CPU's slot. If the slot's prior value was over
/// `limit` (in magnitude), drain it as part of the same rseq CS — the
/// drained value comes back as `to_flush`. Otherwise the delta is just
/// buffered and `to_flush == 0`.
///
/// Total accounting is invariant to peek staleness: the rseq increment is
/// `delta - prior` (drain path) or `delta` (buffer path), so the slot ends
/// holding exactly the bytes from peers that arrived between the peek and
/// the CS. No bytes are lost or double-counted.
TrackResult track(Int64 delta, Int64 limit);

/// Un-paired drain: atomically swap slots[cpu] with 0 and return the prior
/// value. Always succeeds. The caller is expected to be running on `cpu`
/// itself (typically resolved via `currentCPU()` immediately before this
/// call), so concurrent rseq adds from other threads on the same CPU are
/// serialized by the kernel and cannot race.
Int64 drain(int cpu);

/// Sum of slots[i] across all i, with relaxed loads and no reset. O(ncpu).
/// Slow path — intended for `MemoryTracker` to consult under memory pressure.
Int64 peekTotal();

}
