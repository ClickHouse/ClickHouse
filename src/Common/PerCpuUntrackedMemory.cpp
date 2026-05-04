#include <Common/PerCpuUntrackedMemory.h>

#include <atomic>
#include <cstddef>

#if defined(__linux__)
#    include <sys/mman.h>
#    include <unistd.h>
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

}

bool isEnabled()
{
    return slots != nullptr;
}

int cpuCount()
{
    return n_cpu;
}

Int64 add(int cpu, Int64 delta)
{
    /// Single-CPU uncontended atomic. Lowers to one instruction on both
    /// shipped archs — `lock xaddq` (x86_64) and `ldadd` (aarch64 LSE,
    /// mandatory in our build, see cmake/cpu_features.cmake). The slot is
    /// L1-resident on the owning CPU, so cost is ~5–10 cycles. Migration
    /// mid-instruction is harmless: the operation is atomic on whichever
    /// CPU executes it; only attribution may shift, but the total recovered
    /// by `peekTotal` and the next `drain` is exact.
    return __atomic_add_fetch(&slots[cpu].value, delta, __ATOMIC_RELAXED);
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
