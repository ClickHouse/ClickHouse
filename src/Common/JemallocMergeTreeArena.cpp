#include <Common/JemallocMergeTreeArena.h>

#include "config.h"

#if USE_JEMALLOC

#include <Common/Jemalloc.h>
#include <Common/PerCPU.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <optional>
#include <vector>

#include <fmt/format.h>
#include <jemalloc/jemalloc.h>

#if defined(OS_LINUX)
#include <cerrno>
#include <sched.h>
#endif

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
}

namespace DB::JemallocMergeTreeArena
{

namespace
{

/// Written once by `initialize` before `initialized` is published; read-only afterwards.
std::vector<unsigned> arena_indices;
/// Maps an absolute CPU id (from `getCurrentCPU`) to a slot in `arena_indices`. Sized to the
/// highest allowed CPU id. Built so every created arena is reachable regardless of the mask.
std::vector<UInt32> slot_by_cpu;
/// Number of arenas `initialize` tried to create (configured count capped at the allowed CPUs);
/// `arena_indices.size()` may be smaller if `arenas.create` failed.
size_t intended_arena_count = 0;
std::atomic<bool> initialized = false;

std::optional<unsigned> createArena()
{
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int err = je_mallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (err)
    {
        LOG_ERROR(
            &Poco::Logger::get("JemallocMergeTreeArena"),
            "Failed to create dedicated jemalloc MergeTree arena (mallctl error: {}). "
            "The pool continues with the arenas created so far (the default arenas if none).",
            err);
        return {};
    }
    return arena_index;
}

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

void initialize(size_t num_arenas)
{
    /// Startup-only; ignore repeated calls (e.g. a config reload).
    if (initialized.load(std::memory_order_acquire))
        return;

    if (num_arenas > 0)
    {
        /// Size the pool to the CPUs we can actually route to and build a dense CPU->slot map, so
        /// every created arena receives allocations even under a restrictive or sparse affinity
        /// mask (`cpu_id % N` alone would leave arenas unreachable and overstate the count).
        std::vector<UInt32> allowed_cpus = getAllowedCPUs();
        if (allowed_cpus.empty())
        {
            /// Affinity discovery failed; fail closed to one shared arena (reachable from any CPU,
            /// no CPU map needed) instead of guessing a map that could leave arenas unreachable.
            LOG_WARNING(
                &Poco::Logger::get("JemallocMergeTreeArena"),
                "Cannot determine the CPUs this process may run on; "
                "using a single dedicated MergeTree arena instead of {}.",
                num_arenas);
            allowed_cpus.push_back(0);
        }
        num_arenas = std::min(num_arenas, allowed_cpus.size());
        intended_arena_count = num_arenas;

        std::vector<unsigned> indices;
        indices.reserve(num_arenas);
        for (size_t i = 0; i < num_arenas; ++i)
        {
            auto index = createArena();
            if (!index)
                break; /// Keep whatever we managed to create; the rest falls back to the default arena.
            indices.push_back(*index);
        }

        if (!indices.empty())
        {
            /// Size the map to the highest allowed CPU id (ids are ascending), not a fixed cap, so
            /// hosts with many CPUs / high-id cpusets still map every allowed CPU to a real slot.
            slot_by_cpu.assign(static_cast<size_t>(allowed_cpus.back()) + 1, 0);
            for (size_t dense = 0; dense < allowed_cpus.size(); ++dense)
                slot_by_cpu[allowed_cpus[dense]] = static_cast<UInt32>(dense % indices.size());
        }

        arena_indices = std::move(indices);
    }

    initialized.store(true, std::memory_order_release);
}

unsigned getArenaIndex()
{
    if (!initialized.load(std::memory_order_acquire))
        return 0;

    const size_t n = arena_indices.size();
    if (n == 0)
        return 0;
    if (n == 1)
        return arena_indices[0];

    const Int32 cpu = PerCPU::getCurrentCPU();
    if (cpu < 0 || static_cast<size_t>(cpu) >= slot_by_cpu.size())
        return arena_indices[0];
    return arena_indices[slot_by_cpu[cpu]];
}

const std::vector<unsigned> & getArenaIndices()
{
    return arena_indices;
}

size_t getIntendedArenaCount()
{
    return intended_arena_count;
}

bool isEnabled()
{
    return initialized.load(std::memory_order_acquire) && !arena_indices.empty();
}

void purge()
{
    if (!isEnabled())
        return;

    Stopwatch watch;
    for (unsigned index : arena_indices)
    {
        Jemalloc::MibCache<unsigned> purge_mib(fmt::format("arena.{}.purge", index).c_str());
        purge_mib.run();
    }
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
}

}

#else

namespace DB::JemallocMergeTreeArena
{

void initialize(size_t) {}
unsigned getArenaIndex() { return 0; }
const std::vector<unsigned> & getArenaIndices() { static const std::vector<unsigned> empty; return empty; }
size_t getIntendedArenaCount() { return 0; }
bool isEnabled() { return false; }
void purge() {}

}

#endif
