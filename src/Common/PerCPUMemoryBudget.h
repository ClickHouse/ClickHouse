#pragma once

#include <Common/CacheLine.h>
#include <Common/PerCPUMemoryBudgetState.h>

#include <memory>
#if !defined(OS_LINUX)
#include <thread>
#endif

#include "config.h"

#if defined(OS_LINUX)
#    include <sched.h>
#    include <unistd.h>
#endif

#if defined(OS_LINUX)
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

constexpr UInt64 SLICE = 8 * 1024 * 1024;
constexpr int    SLICE_LOG2 = 23;
static_assert(SLICE == (UInt64{1} << SLICE_LOG2));

/// Per-thread accumulator before touching the per-CPU counter. Larger means
/// fewer rseq/atomic ops on the hot path (1 in `BUFFER_SIZE / avg_alloc_size`
/// calls reach the slot) at the cost of a wider `Σ T_i` ceiling — each
/// thread can carry up to ~2 * BUFFER_SIZE of unflushed pending bytes.
constexpr UInt64 BUFFER_SIZE = 32 * 1024;

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

#if defined(OS_LINUX)
inline const bool rseq_ready = []
{
    return rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
}();
#endif

inline bool isRSeqReady()
{
#if defined(OS_LINUX)
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
#if defined(OS_LINUX)
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

#if defined(OS_LINUX)
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

/// Hot-path entry points. Bytes are accumulated in a per-thread buffer
/// (`state.pending_alloc` / `state.pending_free`) and only flushed to the
/// per-CPU counter once `BUFFER_SIZE` is reached. The fast path is one TLS
/// add + one branch — no rseq, no atomic.
///
/// Returns true when the caller should flush `untracked_memory` (a SLICE
/// crossing on the per-CPU counter, or a CPU migration). Whenever the
/// fast path returns false, the call has not touched the per-CPU layer at
/// all — `state.cpu` is unchanged, no migration is observed.
ALWAYS_INLINE inline bool chargeAlloc(Int64 size, PerCPUMemoryBudgetState & state)
{
    state.pending_alloc += static_cast<UInt64>(size);
    if (likely(state.pending_alloc < BUFFER_SIZE))
        return false;
    UInt64 to_charge = state.pending_alloc;
    state.pending_alloc = 0;
    return detail::charge<true>(static_cast<Int64>(to_charge), state);
}
ALWAYS_INLINE inline bool chargeFree(Int64 size, PerCPUMemoryBudgetState & state)
{
    state.pending_free += static_cast<UInt64>(size);
    if (likely(state.pending_free < BUFFER_SIZE))
        return false;
    UInt64 to_charge = state.pending_free;
    state.pending_free = 0;
    return detail::charge<false>(static_cast<Int64>(to_charge), state);
}

}
