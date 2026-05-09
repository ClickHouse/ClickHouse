#pragma once

#include <Common/CacheLine.h>

#include <memory>
#if !defined(OS_LINUX)
#include <thread>
#endif

#include "config.h"

#if defined(OS_LINUX)
#    include <sched.h>
#    include <unistd.h>
#endif

#if USE_LIBRSEQ
#    include <rseq/rseq.h>
#endif

#include <base/defines.h>
#include <base/types.h>

/// Per-CPU pacer for `ThreadStatus::untracked_memory` flushes.
///
/// Two monotone byte-counters per CPU — `nallocs` and `nfrees` — record bytes
/// allocated and freed on that CPU. On every op a thread does an
/// `__atomic_fetch_add` (or rseq equivalent) on the appropriate counter and
/// compares the new value against the value it last saw on that CPU
/// (`PerCPUMemoryBudgetState`). The caller is told to flush its
/// `untracked_memory` whenever:
///   * the SLICE bucket changed since this thread last looked, OR
///   * the CPU itself changed since the previous charge (migration).
///
/// Properties:
///   * counters never decrement -> no drift, no counter-charge to undo;
///   * migration detection is folded into `chargeAlloc`/`chargeFree`: there
///     is exactly one CPU read per op (`rseq_cpu_start` or `sched_getcpu`),
///     and the migration vs crossing signal is collapsed into the bool
///     return — caller has a single flush condition;
///   * `Σ T_i ≤ ncpus * SLICE` in steady state: between two crossings on
///     CPU c, the counter advanced by exactly SLICE bytes; every thread on
///     c that contributed will see the bucket change on its next op and
///     flush, so the residual `T_i` aligned to c is bounded by SLICE.
///
/// The fast path is one rseq load+store (~2ns) when rseq is available, or a
/// single `lock xadd` (~10ns) otherwise. Frees are accounted symmetrically.
namespace DB::PerCPUMemoryBudget
{

constexpr UInt64 SLICE = 4 * 1024 * 1024;
constexpr int    SLICE_LOG2 = 22;
static_assert(SLICE == (UInt64{1} << SLICE_LOG2));

/// Per-thread state. Caller stores this in TLS (or, eventually, in
/// `ThreadStatus`) and passes it to every charge call.
///
/// Counters are unsigned: signed overflow would be UB and we don't want the
/// optimiser to assume away the wrap. Unsigned wraps cleanly, and the
/// crossing check below uses `!=` on the SLICE-shifted bucket so it stays
/// correct across the (~29-year-at-10GB/s/CPU) wrap point.
struct PerCPUMemoryBudgetState
{
    int    cpu = -1;     /// CPU we last successfully charged on; -1 = uninitialised
    UInt64 nallocs = 0;  /// last-seen value of `nallocs[cpu]` for this thread
    UInt64 nfrees  = 0;  /// last-seen value of `nfrees[cpu]`
};

struct alignas(CH_CACHE_LINE_SIZE) Slot
{
    UInt64 nallocs;
    UInt64 nfrees;
};

/// Static-init pipeline: declared in source order, initialised in source
/// order within this TU. Callers reach this code only after
/// `total_memory_tracker` is constructed (the gate in `getMemoryTracker`),
/// so by the time anyone touches `slots[]` it is fully populated.
inline const int slot_count = []
{
#if defined(OS_LINUX)
    Int64 n = ::sysconf(_SC_NPROCESSORS_CONF);
    return n > 0 ? static_cast<int>(n) : 0;
#else
    return static_cast<int>(std::thread::hardware_concurrency());
#endif
}();

const std::unique_ptr<Slot[]> slots = std::make_unique<Slot[]>(slot_count);

#if USE_LIBRSEQ
inline const bool rseq_ready = []
{
    return rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
}();
#endif

inline bool isRSeqReady()
{
#if USE_LIBRSEQ
    return rseq_ready;
#else
    return false;
#endif
}

/// Returns the current CPU, or a negative value when the per-CPU machinery
/// is unavailable. Exposed mostly for tests/diagnostics; the hot path
/// (`chargeAlloc`/`chargeFree`) reads the CPU itself.
ALWAYS_INLINE inline int currentCPU()
{
#if USE_LIBRSEQ
    if (likely(rseq_ready))
    {
        int cpu = static_cast<int>(rseq_cpu_start());
        return static_cast<unsigned>(cpu) < static_cast<unsigned>(slot_count) ? cpu : -1;
    }
#endif
#if defined(OS_LINUX)
    int cpu = ::sched_getcpu();
    return static_cast<unsigned>(cpu) < static_cast<unsigned>(slot_count) ? cpu : -1;
#else
    return -1;
#endif
}

namespace detail
{

template <bool alloc>
ALWAYS_INLINE UInt64 * counterFor(int cpu)
{
    if constexpr (alloc) return &slots[cpu].nallocs;
    else                 return &slots[cpu].nfrees;
}

/// Single hot-path helper, parameterised on whether we're charging the
/// alloc-side or the free-side counter.
///
/// Important: the entire rseq vs. atomic dispatch is structured as a single
/// outer branch on `rseq_ready` so that:
///   * the compiler loads `rseq_ready` exactly once;
///   * the rseq fast path keeps `rseq_offset` in a single register across
///     the cpu read and the rseq op (the previous structure leaked to two
///     loads of `rseq_offset` because the load was split across separate
///     `if` regions).
template <bool alloc>
ALWAYS_INLINE bool charge(Int64 size, PerCPUMemoryBudgetState & state)
{
    int cpu;
    UInt64 next;

#if USE_LIBRSEQ
    if (likely(rseq_ready))
    {
        cpu = static_cast<int>(rseq_cpu_start());
        if (unlikely(static_cast<unsigned>(cpu) >= static_cast<unsigned>(slot_count)))
            return false;

        UInt64 * counter = counterFor<alloc>(cpu);
        while (true)
        {
            UInt64 current = __atomic_load_n(counter, __ATOMIC_RELAXED);
            next = current + static_cast<UInt64>(size);
            int r = rseq_load_cbne_store__ptr(
                RSEQ_MO_RELAXED,
                RSEQ_PERCPU_CPU_ID,
                reinterpret_cast<intptr_t *>(counter),
                static_cast<intptr_t>(current),
                static_cast<intptr_t>(next),
                cpu);
            if (likely(r == 0))
                break;
            /// rseq aborted (preemption / migration / signal). Re-read cpu and retry.
            cpu = static_cast<int>(rseq_cpu_start());
            if (unlikely(static_cast<unsigned>(cpu) >= static_cast<unsigned>(slot_count)))
                return false;
            counter = counterFor<alloc>(cpu);
        }
    }
    else
#endif
    {
#if defined(OS_LINUX)
        cpu = ::sched_getcpu();
#else
        cpu = -1;
#endif
        if (unlikely(static_cast<unsigned>(cpu) >= static_cast<unsigned>(slot_count)))
            return false;
        next = __atomic_add_fetch(counterFor<alloc>(cpu), static_cast<UInt64>(size), __ATOMIC_RELAXED);
    }

    /// Migration detection happens *after* a successful charge — `cpu` is the
    /// CPU we actually ended up on, regardless of any rseq retries.
    bool migrated = (state.cpu != cpu);
    state.cpu = cpu;

    /// `!=` (not `>`) on the SLICE-shifted bucket: monotone counters only
    /// ever advance or wrap, so any change of bucket is a crossing — and
    /// using `!=` makes the check correct across the unsigned wrap point.
    if constexpr (alloc)
    {
        bool cross = (next >> SLICE_LOG2) != (state.nallocs >> SLICE_LOG2);
        state.nallocs = next;
        return migrated || cross;
    }
    else
    {
        bool cross = (next >> SLICE_LOG2) != (state.nfrees >> SLICE_LOG2);
        state.nfrees = next;
        return migrated || cross;
    }
}

}

/// Hot-path entry points. Atomically advance the per-CPU counter, update
/// `state`, and return true when the caller should flush `untracked_memory`
/// (either a SLICE crossing happened or this thread changed CPU).
ALWAYS_INLINE inline bool chargeAlloc(Int64 size, PerCPUMemoryBudgetState & state)
{
    return detail::charge<true>(size, state);
}
ALWAYS_INLINE inline bool chargeFree(Int64 size, PerCPUMemoryBudgetState & state)
{
    return detail::charge<false>(size, state);
}

}
