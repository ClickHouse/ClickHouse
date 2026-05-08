#include <Common/PerCPUMemoryBudget.h>

#include <Common/CacheLine.h>

#include <memory>
#include <thread>

#include "config.h"

#if defined(OS_LINUX)
#    include <sched.h>
#    include <unistd.h>
#endif

#if USE_LIBRSEQ
#    include <rseq/rseq.h>
#endif

namespace DB::PerCPUMemoryBudget
{

namespace
{

struct alignas(CH_CACHE_LINE_SIZE) Slot
{
    Int64 nallocs;
    Int64 nfrees;
};

/// Static-init pipeline: declared in source order, initialised in source
/// order within this TU. Callers reach this code only after
/// `total_memory_tracker` is constructed (the gate in `getMemoryTracker`),
/// so by the time anyone touches `slots[]` it is fully populated.
const int slot_count = []
{
#if defined(OS_LINUX)
    long n = ::sysconf(_SC_NPROCESSORS_CONF);
    return n > 0 ? static_cast<int>(n) : 0;
#else
    return static_cast<int>(std::thread::hardware_concurrency());
#endif
}();

const std::unique_ptr<Slot[]> slots = std::make_unique<Slot[]>(slot_count);

#if USE_LIBRSEQ
const bool rseq_ready = []
{
    return rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
}();
#endif

#if USE_LIBRSEQ
bool chargeRseq(Int64 * counter, Int64 size, Int64 & state_last_seen)
{
    while (true)
    {
        int cpu = static_cast<int>(rseq_cpu_start());
        if (cpu >= slot_count)
            return false;
        Int64 current = __atomic_load_n(counter, __ATOMIC_RELAXED);
        Int64 next = current + size;
        intptr_t * counter_ptr = reinterpret_cast<intptr_t *>(counter);
        int r = rseq_load_cbne_store__ptr(
            RSEQ_MO_RELAXED,
            RSEQ_PERCPU_CPU_ID,
            counter_ptr,
            static_cast<intptr_t>(current),
            static_cast<intptr_t>(next),
            cpu);
        if (r == 0)
        {
            bool cross = (next >> SLICE_LOG2) > (state_last_seen >> SLICE_LOG2);
            state_last_seen = next;
            return cross;
        }
        /// rseq aborted (preemption, migration, signal). Retry.
    }
}
#endif

bool chargeAtomic(Int64 * counter, Int64 size, Int64 & state_last_seen)
{
    Int64 next = __atomic_add_fetch(counter, size, __ATOMIC_RELAXED);
    bool cross = (next >> SLICE_LOG2) > (state_last_seen >> SLICE_LOG2);
    state_last_seen = next;
    return cross;
}

inline bool chargeImpl(Int64 * counter, Int64 size, Int64 & state_last_seen)
{
#if USE_LIBRSEQ
    if (rseq_ready)
        return chargeRseq(counter, size, state_last_seen);
#endif
    return chargeAtomic(counter, size, state_last_seen);
}

}

bool isRSeqReady()
{
#if USE_LIBRSEQ
    return rseq_ready;
#else
    return false;
#endif
}

int currentCPU()
{
#if USE_LIBRSEQ
    if (rseq_ready)
    {
        int cpu = static_cast<int>(rseq_cpu_start());
        return cpu < slot_count ? cpu : -1;
    }
#endif
#if defined(OS_LINUX)
    int cpu = ::sched_getcpu();
    return (cpu >= 0 && cpu < slot_count) ? cpu : -1;
#else
    return -1;
#endif
}

bool chargeAlloc(Int64 size, PerCPUMemoryBudgetState & state)
{
    if (state.cpu < 0 || state.cpu >= slot_count)
        return false;
    return chargeImpl(&slots[state.cpu].nallocs, size, state.nallocs);
}

bool chargeFree(Int64 size, PerCPUMemoryBudgetState & state)
{
    if (state.cpu < 0 || state.cpu >= slot_count)
        return false;
    return chargeImpl(&slots[state.cpu].nfrees, size, state.nfrees);
}

}
