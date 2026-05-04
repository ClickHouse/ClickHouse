#include <Common/PerCPUUntrackedMemory.h>

#include <Common/CacheLine.h>

#include <cstddef>
#include <cstdint>

#include "config.h"

#if defined(OS_LINUX)
#    include <sched.h>
#    include <unistd.h>
#endif

#if USE_LIBRSEQ
#    include <rseq/rseq.h>
#endif

namespace DB::PerCPUUntrackedMemory
{

namespace
{

/// Upper bound on the number of CPUs we are willing to track per slot.
/// Sizing the BSS array at 1024 costs MAX_CPUS * CH_CACHE_LINE_SIZE bytes —
/// at most 256 KiB on s390x, 128 KiB on POWER, 64 KiB everywhere else. If a
/// system exceeds the cap we disable the per-CPU path and fall back to the
/// legacy per-thread accumulator in `CurrentMemoryTracker`.
constexpr int MAX_CPUS = 1024;

/// Cache-line aligned so cross-CPU writes do not bounce the same line.
/// `alignas` also forces tail padding (sizeof must be a multiple of
/// alignof), so the array stride is exactly CH_CACHE_LINE_SIZE.
struct alignas(CH_CACHE_LINE_SIZE) Slot
{
    Int64 value;
};

alignas(CH_CACHE_LINE_SIZE) Slot slots[MAX_CPUS]{};

#if defined(OS_LINUX)

const int n_cpu = []
{
    Int64 n = ::sysconf(_SC_NPROCESSORS_CONF);
    if (n <= 0 || n > MAX_CPUS)
        return 0;
    return static_cast<int>(n);
}();

#else

constexpr int n_cpu = 0;

#endif

#if USE_LIBRSEQ

/// Non-zero `rseq_size` means glibc auto-registered an rseq area for this
/// thread and `rseq_init` discovered it. After that, all the per-CPU helpers
/// in this file run on the rseq fast path.
const bool rseq_ready = []
{
    if (n_cpu <= 0)
        return false;
    return rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
}();

#else

constexpr bool rseq_ready = false;

#endif

inline int currentCPU()
{
#if USE_LIBRSEQ
    if (rseq_ready)
    {
        /// `cpu_id_start` is the snapshot field: always a valid CPU index
        /// (no -1/-2 sentinels), use it for indexing without a branch.
        return static_cast<int>(rseq_cpu_start());
    }
#endif

#if defined(OS_LINUX)
    int cpu = ::sched_getcpu();
    return cpu < 0 ? 0 : cpu;
#else
    return 0;
#endif
}

}

bool isEnabled()
{
    return n_cpu > 0;
}

int cpuCount()
{
    return n_cpu;
}

Int64 track(Int64 delta, Int64 limit)
{
#if USE_LIBRSEQ
    if (rseq_ready)
    {
        /// One rseq CS per call. We peek `current` outside the CS (racy);
        /// inside, the CS does `slot[cpu] += adjustment`, where:
        ///   - if peek showed |current| > limit: adjustment = delta - current,
        ///     leaving slot ≈ delta + (peer adds since peek);
        ///   - otherwise:                         adjustment = delta.
        /// Whatever the peek's staleness, total bytes accounted == real bytes:
        /// caller flushes `current` to its parent tracker, slot retains
        /// (real_at_CS - current) + delta. Migration aborts the CS; we retry
        /// from the top with a fresh peek and CPU.
        while (true)
        {
            int cpu = static_cast<int>(rseq_cpu_start());
            Int64 current = __atomic_load_n(&slots[cpu].value, __ATOMIC_RELAXED);
            bool over = current > limit || current < -limit;
            Int64 adjustment = over ? (delta - current) : delta;
            intptr_t * slot_ptr = reinterpret_cast<intptr_t *>(&slots[cpu].value);
            if (rseq_load_add_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU_CPU_ID,
                                         slot_ptr, static_cast<intptr_t>(adjustment), cpu) == 0)
                return over ? current : Int64{0};
        }
    }
#endif

    /// Atomic fallback: CAS loop with the same check-or-drain semantics.
    /// One CAS per attempt; under contention may retry until peers settle.
    int cpu = currentCPU();
    while (true)
    {
        Int64 current = __atomic_load_n(&slots[cpu].value, __ATOMIC_RELAXED);
        bool over = current > limit || current < -limit;
        Int64 next = over ? delta : (current + delta);
        if (__atomic_compare_exchange_n(&slots[cpu].value, &current, next,
                                        /*weak=*/false, __ATOMIC_RELAXED, __ATOMIC_RELAXED))
            return over ? current : Int64{0};
    }
}

Int64 drain()
{
    /// Un-paired drain on this thread's current CPU. The kernel serializes
    /// our atomic exchange against any concurrent rseq RMW from other
    /// threads on the same CPU (whoever was in a CS got preempted and
    /// restarted), so the exchange sees a stable slot value.
    return __atomic_exchange_n(&slots[currentCPU()].value, Int64{0}, __ATOMIC_RELAXED);
}

Int64 peekTotal()
{
    Int64 total = 0;
    for (int i = 0; i < n_cpu; ++i)
        total += __atomic_load_n(&slots[i].value, __ATOMIC_RELAXED);
    return total;
}

}
