#include <Common/PerCpuUntrackedMemory.h>

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "config.h"

#if defined(__linux__)
#    include <sys/mman.h>
#    include <unistd.h>
#endif

#if USE_LIBRSEQ
#    include <rseq/rseq.h>
#elif defined(__linux__)
#    include <sched.h>
#endif

namespace DB::PerCpuUntrackedMemory
{

namespace
{

/// Cache-line padded to keep cross-CPU writes from bouncing the same line.
struct alignas(64) Slot
{
    Int64 value;
    char pad[64 - sizeof(Int64)];
};

#if defined(__linux__)

const int n_cpu = []
{
    long n = ::sysconf(_SC_NPROCESSORS_CONF);
    return n > 0 ? static_cast<int>(n) : 0;
}();

Slot * const slots = []() -> Slot *
{
    if (n_cpu <= 0)
        return nullptr;
    size_t bytes = sizeof(Slot) * static_cast<size_t>(n_cpu);
    void * p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    return p == MAP_FAILED ? nullptr : static_cast<Slot *>(p);
}();

#else

constexpr int n_cpu = 0;
Slot * const slots = nullptr;

#endif

#if USE_LIBRSEQ

/// Non-zero `rseq_size` means glibc auto-registered an rseq area for this
/// thread and `rseq_init` discovered it. After that, all the per-CPU helpers
/// in this file run on the rseq fast path.
const bool rseq_ready = []
{
    if (!slots)
        return false;
    return rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
}();

#else

constexpr bool rseq_ready = false;

#endif

inline int normalizeCpu(int cpu)
{
    if (cpu < 0)
        return 0;
    if (cpu >= n_cpu)
        return cpu % n_cpu;
    return cpu;
}

}

bool isEnabled()
{
    return slots != nullptr;
}

int cpuCount()
{
    return n_cpu;
}

int currentCpu()
{
#if USE_LIBRSEQ
    if (rseq_ready)
        return normalizeCpu(rseq_current_cpu_raw());
#endif

#if defined(__linux__)
    return normalizeCpu(::sched_getcpu());
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
        /// non-atomicity. Loops until we commit on some CPU.
        int cpu;
        while (true)
        {
            cpu = normalizeCpu(rseq_current_cpu_raw());
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

    int cpu = currentCpu();
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
