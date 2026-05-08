#include <Common/PerCPUMemoryBudget.h>

#include <Common/CacheLine.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/MemoryTracker.h>
#include <Common/getNumberOfCPUCoresToUse.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <tuple>

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
    Int64 value;
};

enum class State : uint8_t
{
    NOT_INITIALIZED = 0,
    /// Protection from recursion
    INITIALIZING    = 1,
    INITIALIZED     = 2,
};
std::atomic<State> state{State::NOT_INITIALIZED};

std::unique_ptr<Slot[]> slots;
Int64 slot_count = 0;

#if USE_LIBRSEQ
bool rseq_ready = false;
#endif

inline int currentCPU()
{
#if USE_LIBRSEQ
    if (rseq_ready)
        return static_cast<int>(rseq_cpu_start());
#endif
#if defined(OS_LINUX)
    int cpu = ::sched_getcpu();
    return cpu < 0 ? 0 : cpu;
#else
    return 0;
#endif
}

Int64 getTotalCPUs()
{
#if defined(OS_LINUX)
    return ::sysconf(_SC_NPROCESSORS_CONF);
#else
    return std::thread::hardware_concurrency();
#endif
}

__attribute__((noinline))
bool tryInitSlow()
try
{
    if (!isTotalMemoryTrackerInitialized())
        return false;

    static std::mutex init_mutex;
    std::lock_guard lock(init_mutex);

    State s = state.load(std::memory_order_relaxed);
    if (s == State::INITIALIZED)
        return true;
    /// Recursion protection:
    ///   tryInitSlow() -> getNumberOfCPUCoresToUse() -> tryInitSlow()
    if (s == State::INITIALIZING)
        return false;

    state.store(State::INITIALIZING, std::memory_order_release);

    Int64 total_cpus = getTotalCPUs();
    if (total_cpus <= 0)
    {
        state.store(State::NOT_INITIALIZED, std::memory_order_release);
        return false;
    }
    slot_count = total_cpus;

#if USE_LIBRSEQ
    rseq_ready = (rseq_init() == RSEQ_INIT_OK && rseq_size > 0);
#endif

    slots = std::make_unique<Slot[]>(slot_count);
    /// Note, we allocate all slots not only effective, since we do not know which one will be in use
    for (Int64 i = 0; i < slot_count; ++i)
        slots[i].value = SLICE;

    /// Cgroup-aware count caps the reservation so containers don't over-charge for CPUs the kernel will never schedule us on.
    /// (NOTE: CPU hot plug is not supported)
    Int64 effective_cpus = getNumberOfCPUCoresToUse();
    if (effective_cpus <= 0 || effective_cpus > slot_count)
        effective_cpus = slot_count;
    std::ignore = CurrentMemoryTracker::alloc(SLICE * effective_cpus);

    state.store(State::INITIALIZED, std::memory_order_release);
    return true;
}
catch (...)
{
    state.store(State::NOT_INITIALIZED, std::memory_order_release);
    throw;
}

inline bool isReadyHot()
{
    thread_local bool ready_tls = false;
    if (likely(ready_tls))
        return true;
    State s = state.load(std::memory_order_acquire);
    if (s == State::INITIALIZED)
    {
        ready_tls = true;
        return true;
    }
    if (s == State::NOT_INITIALIZED && tryInitSlow())
    {
        ready_tls = true;
        return true;
    }
    return false;
}

bool outOfBounds(Int64 slice_value)
{
    return slice_value < 0 || slice_value > 2 * SLICE;
}

#if USE_LIBRSEQ
bool chargeRseq(Int64 delta)
{
    while (true)
    {
        int cpu = static_cast<int>(rseq_cpu_start());
        if (cpu >= slot_count)
            return false;
        Int64 current = __atomic_load_n(&slots[cpu].value, __ATOMIC_RELAXED);
        Int64 next = current - delta;
        /// Refuse rather than overcommit. Caller will flush via the per-thread
        /// path with a partial counter-charge (see header).
        if (outOfBounds(next))
            return true;
        intptr_t * slot_ptr = reinterpret_cast<intptr_t *>(&slots[cpu].value);
        int r = rseq_load_cbne_store__ptr(
            RSEQ_MO_RELAXED,
            RSEQ_PERCPU_CPU_ID,
            slot_ptr,
            static_cast<intptr_t>(current),
            static_cast<intptr_t>(next),
            cpu);
        if (r == 0)
            return false;
    }
}
#endif

bool chargeAtomic(Int64 delta)
{
    int cpu = currentCPU();
    if (cpu >= slot_count)
        return false;
    Int64 current = __atomic_load_n(&slots[cpu].value, __ATOMIC_RELAXED);
    while (true)
    {
        Int64 next = current - delta;
        if (outOfBounds(next))
            return true;
        if (__atomic_compare_exchange_n(&slots[cpu].value, &current, next,
                                        /*weak=*/true, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
            return false;
        /// `current` was reloaded by compare_exchange on failure; loop.
    }
}

}

bool isReady()
{
    return state.load(std::memory_order_acquire) == State::INITIALIZED;
}

bool isRSeqReady()
{
#if USE_LIBRSEQ
    return rseq_ready;
#else
    return 0;
#endif
}

bool charge(Int64 delta)
{
    if (unlikely(!isReadyHot()))
        return false;
#if USE_LIBRSEQ
    if (rseq_ready)
        return chargeRseq(delta);
#endif
    return chargeAtomic(delta);
}

Int64 reservedBytes()
{
    if (!isReady())
        return 0;
    Int64 total = 0;
    for (int i = 0; i < slot_count; ++i)
        total += __atomic_load_n(&slots[i].value, __ATOMIC_RELAXED);
    return total;
}

}
