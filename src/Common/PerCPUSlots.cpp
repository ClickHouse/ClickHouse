#include <Common/PerCPUSlots.h>

#include <Common/PerCPU.h>

#if defined(OS_LINUX)
#include <cerrno>
#include <sched.h>
#endif

namespace DB
{

namespace
{

/// CPUs this process may run on, in ascending order. Honors the affinity mask on Linux, so a
/// cpuset-limited process only routes to the CPUs it actually uses; falls back to
/// [0, getNumCPUs()) elsewhere.
std::vector<UInt32> getAllowedCPUs()
{
    std::vector<UInt32> cpus;
#if defined(OS_LINUX)
    /// The fixed `cpu_set_t` holds only `__CPU_SETSIZE` (1024) CPUs, so `sched_getaffinity` fails
    /// with EINVAL once the mask covers a CPU id >= 1024. Grow a dynamically-allocated mask until it
    /// fits, so pools on machines with many CPUs (or cpusets pinned to high ids) still map every
    /// allowed CPU to a reachable arena. Runs once, at startup.
    ///
    /// The mask is a plain 64-bit-word array (`cpu_set_t` is an array of `unsigned long`, which is
    /// 64-bit on the supported Linux platforms) rather than `CPU_ALLOC`, because `CPU_ALLOC` /
    /// `CPU_FREE` resolve to `__sched_cpualloc` / `__sched_cpufree` which are only available since
    /// GLIBC_2.7 and would break the release-binary compatibility check (max allowed GLIBC 2.4).
    /// `CPU_ALLOC_SIZE` and `CPU_ISSET_S` are macros with no such dependency. `UInt64` gives the
    /// alignment `cpu_set_t` needs.
    for (size_t num_cpus = 1024; num_cpus <= (size_t{1} << 20); num_cpus *= 2)
    {
        const size_t set_size = CPU_ALLOC_SIZE(num_cpus);
        std::vector<UInt64> mask((set_size + sizeof(UInt64) - 1) / sizeof(UInt64));
        auto * set = reinterpret_cast<cpu_set_t *>(mask.data());
        if (sched_getaffinity(0, set_size, set) == 0)
        {
            for (UInt32 cpu = 0; cpu < num_cpus; ++cpu)
            {
                if (CPU_ISSET_S(cpu, set_size, set))
                    cpus.push_back(cpu);
            }
            break;
        }
        if (errno != EINVAL) /// EINVAL means the set was too small; anything else is a real error.
            break;
    }
    /// On a real affinity-read failure, return empty rather than fabricating [0, getNumCPUs()):
    /// a guessed mask could map allowed CPUs onto only a few slots while unreachable arenas still
    /// count in the pool. The caller fails closed to a single arena, reachable from any CPU.
#else
    for (UInt32 cpu = 0; cpu < PerCPU::getNumCPUs(); ++cpu)
        cpus.push_back(cpu);
#endif
    return cpus;
}

}

PerCPUSlots::PerCPUSlots()
{
    std::vector<UInt32> allowed_cpus = getAllowedCPUs();
    num_slots = allowed_cpus.size();
    if (allowed_cpus.empty())
        return;

    slot_by_cpu.assign(static_cast<size_t>(allowed_cpus.back()) + 1, 0);
    for (size_t dense = 0; dense < allowed_cpus.size(); ++dense)
        slot_by_cpu[allowed_cpus[dense]] = static_cast<UInt32>(dense);
}

size_t PerCPUSlots::getCurrentSlot() const
{
    const Int32 cpu = PerCPU::getCurrentCPU();
    if (cpu < 0 || static_cast<size_t>(cpu) >= slot_by_cpu.size())
        return 0;
    return slot_by_cpu[cpu];
}

}
