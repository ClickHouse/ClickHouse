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

/// Resolved at startup. `rseq_size > 0` after a successful registration —
/// glibc auto-registers and `rseq_init` only discovers that, so this should
/// succeed on any modern Linux x86_64 / aarch64 build.
const bool rseq_ready = []
{
    if (!slots)
        return false;
    return rseq_init() == RSEQ_INIT_OK && rseq_size > 0;
}();

#else

constexpr bool rseq_ready = false;

#endif

inline int resolveCpu()
{
#if USE_LIBRSEQ
    if (rseq_ready)
    {
        int32_t cpu = rseq_current_cpu_raw();
        if (cpu < 0)
            cpu = 0;
        else if (cpu >= n_cpu)
            cpu %= n_cpu;
        return cpu;
    }
#endif

#if defined(__linux__)
    int cpu = ::sched_getcpu();
    if (cpu < 0)
        cpu = 0;
    else if (cpu >= n_cpu)
        cpu %= n_cpu;
    return cpu;
#else
    return 0;
#endif
}

#if USE_LIBRSEQ

/// rseq RMW: `slots[cpu].value += delta` with kernel-restartable
/// non-atomicity. Loops on preempt/migrate (return value != 0), refreshing
/// the CPU index each iteration. After commit, read back the slot value
/// for the caller — this read is racy with concurrent rseq adds on the
/// same CPU but only by a bounded amount (one peer's commit), and the
/// caller uses the value only as an overflow heuristic.
inline AddResult rseqAdd(Int64 delta)
{
    int cpu;
    while (true)
    {
        cpu = rseq_current_cpu_raw();
        if (cpu < 0)
            cpu = 0;
        else if (cpu >= n_cpu)
            cpu %= n_cpu;

        intptr_t * slot_ptr = reinterpret_cast<intptr_t *>(&slots[cpu].value);
        intptr_t count = static_cast<intptr_t>(delta);
        if (rseq_load_add_store__ptr(RSEQ_MO_RELAXED, RSEQ_PERCPU_CPU_ID, slot_ptr, count, cpu) == 0)
            break;
    }
    Int64 new_local = __atomic_load_n(&slots[cpu].value, __ATOMIC_RELAXED);
    return {cpu, new_local};
}

#endif

}

bool isEnabled()
{
    if (!slots)
        return false;
#if USE_LIBRSEQ
    /// Even if rseq registration failed we can still service ops via the
    /// atomic fallback below, but we want isEnabled() to mean "the per-CPU
    /// path is active". With librseq linked in, atomic-only is the fallback
    /// inside the same TU; expose isEnabled iff slots exist.
#endif
    return true;
}

int cpuCount()
{
    return n_cpu;
}

int currentCpu()
{
    return resolveCpu();
}

AddResult add(Int64 delta)
{
#if USE_LIBRSEQ
    if (rseq_ready)
        return rseqAdd(delta);
#endif

    int cpu = resolveCpu();
    Int64 new_local = __atomic_add_fetch(&slots[cpu].value, delta, __ATOMIC_RELAXED);
    return {cpu, new_local};
}

Int64 drain(int cpu)
{
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
