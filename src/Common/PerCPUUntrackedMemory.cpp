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

/// Cache-line padded so cross-CPU writes do not bounce the same line.
struct alignas(CH_CACHE_LINE_SIZE) Slot
{
    Int64 value;
    char pad[CH_CACHE_LINE_SIZE - sizeof(Int64)];
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

}

bool isEnabled()
{
    return n_cpu > 0;
}

int cpuCount()
{
    return n_cpu;
}

int currentCPU()
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

AddResult add(Int64 delta)
{
#if USE_LIBRSEQ
    if (rseq_ready)
    {
        /// rseq RMW: `slots[cpu].value += delta` with kernel-restartable
        /// non-atomicity. The librseq protocol expects the snapshot CPU id
        /// to come from `rseq_cpu_start()`; the CS body internally reads
        /// `cpu_id` (live) and aborts if it differs from `cpu`. Loop until
        /// we commit on some CPU.
        int cpu;
        while (true)
        {
            cpu = static_cast<int>(rseq_cpu_start());
            intptr_t * slot_ptr = reinterpret_cast<intptr_t *>(&slots[cpu].value);
            intptr_t count = static_cast<intptr_t>(delta);
            if (rseq_load_add_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU_CPU_ID, slot_ptr, count, cpu) == 0)
                break;
        }
        /// Read-back is racy with concurrent rseq adds on the same CPU,
        /// but bounded by one peer's count. Used only as an overflow
        /// heuristic, where small drift is harmless. Lowers to a single
        /// `mov` (x86_64) / `ldr` (aarch64); the C++ atomic load is for
        /// the memory model, not for hardware atomicity.
        Int64 new_local = __atomic_load_n(&slots[cpu].value, __ATOMIC_RELAXED);
        return {cpu, new_local};
    }
#endif

    int cpu = currentCPU();
    Int64 new_local = __atomic_add_fetch(&slots[cpu].value, delta, __ATOMIC_RELAXED);
    return {cpu, new_local};
}

bool tryFlush(int cpu, Int64 amount)
{
    if (amount == 0)
        return true;

#if USE_LIBRSEQ
    if (rseq_ready)
    {
        /// Same rseq primitive as `add`, with delta = -amount. The kernel
        /// restarts whoever was preempted, so there is no race between
        /// concurrent rseq adds on `cpu` and this subtract. If we are no
        /// longer on `cpu`, the rseq sequence aborts cleanly and the slot
        /// is unchanged — caller treats this as "flush deferred".
        intptr_t * slot_ptr = reinterpret_cast<intptr_t *>(&slots[cpu].value);
        intptr_t count = -static_cast<intptr_t>(amount);
        return rseq_load_add_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU_CPU_ID, slot_ptr, count, cpu) == 0;
    }
#endif

    /// Atomic fallback path: no rseq, so all writers use atomic ops and
    /// fetch_sub is race-free against them. Always succeeds.
    __atomic_fetch_sub(&slots[cpu].value, amount, __ATOMIC_RELAXED);
    return true;
}

Int64 drain(int cpu)
{
    /// Un-paired drain. Caller is expected to be running on `cpu` itself,
    /// so the kernel guarantees no concurrent rseq RMW from other threads
    /// on this CPU (whoever was in a CS got preempted and restarted by the
    /// kernel before we ran). The atomic exchange therefore sees a stable
    /// slot value and cannot race with rseq's load/add/store sequence.
    return __atomic_exchange_n(&slots[cpu].value, Int64{0}, __ATOMIC_RELAXED);
}

Int64 peekTotal()
{
    Int64 total = 0;
    for (int i = 0; i < n_cpu; ++i)
        total += __atomic_load_n(&slots[i].value, __ATOMIC_RELAXED);
    return total;
}

}
