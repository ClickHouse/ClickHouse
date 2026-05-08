#pragma once

#include <base/types.h>

/// Per-CPU pacer for `ThreadStatus::untracked_memory` flushes.
///
/// Two monotone byte-counters per CPU — `nallocs` and `nfrees` — record bytes
/// allocated and freed on that CPU. On every op a thread does an
/// `__atomic_fetch_add` (or rseq equivalent) on the appropriate counter and
/// compares the new value against the value it last saw on that CPU
/// (`PerCPUMemoryBudgetState`). When the counter has crossed a SLICE
/// boundary since the thread's last observation, the thread is told to flush
/// its `untracked_memory` to the tracker chain.
///
/// Properties:
///   * counters never decrement -> no drift, no counter-charge to undo;
///   * cross-CPU migration is handled at the caller (state.cpu mismatch ->
///     flush + refresh state). After migration, `T_i` is tied to the new
///     CPU's counter alone, so the bound below is per-CPU not per-thread;
///   * `Σ T_i ≤ ncpus * SLICE` in steady state: between two crossings on
///     CPU c, the counter advanced by exactly SLICE bytes; every thread on
///     c that contributed will see the bucket change on its next op and
///     flush, so the residual `T_i` aligned to c is bounded by SLICE.
///
/// The fast path is one rseq load+store (~2ns) when rseq is available, or a
/// single `lock xadd` (~10ns) otherwise. Frees are accounted symmetrically.
namespace DB::PerCPUMemoryBudget
{

constexpr Int64 SLICE = 4 * 1024 * 1024;
constexpr int   SLICE_LOG2 = 22;
static_assert(SLICE == (Int64{1} << SLICE_LOG2));

/// Per-thread state. Caller stores this in TLS (or, eventually, in
/// `ThreadStatus`) and passes it to every charge call.
struct PerCPUMemoryBudgetState
{
    int   cpu = -1;     /// CPU this thread last charged on; -1 = uninitialised
    Int64 nallocs = 0;  /// last-seen value of `nallocs[cpu]` for this thread
    Int64 nfrees  = 0;  /// last-seen value of `nfrees[cpu]`
};

bool isRSeqReady();

/// Returns the current CPU, or a negative value when the per-CPU machinery
/// is unavailable. Caller short-circuits the budget layer in that case.
int currentCPU();

/// Hot-path entry points. Atomically advance the per-CPU counter, update
/// `state`'s last-seen value, and return true when the SLICE bucket changed
/// since the caller last looked — the signal to flush `untracked_memory`.
bool chargeAlloc(Int64 size, PerCPUMemoryBudgetState & state);
bool chargeFree (Int64 size, PerCPUMemoryBudgetState & state);

}
