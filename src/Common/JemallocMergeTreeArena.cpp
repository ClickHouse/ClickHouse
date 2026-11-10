#include <Common/JemallocMergeTreeArena.h>

#include "config.h"

#if USE_JEMALLOC

#include <Common/Jemalloc.h>
#include <Common/PerCPUSlots.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <atomic>
#include <optional>
#include <vector>

#include <fmt/format.h>
#include <jemalloc/jemalloc.h>

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
std::optional<PerCPUSlots> cpu_slots;
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
        PerCPUSlots slots;
        size_t num_allowed_cpus = slots.size();
        if (num_allowed_cpus == 0)
        {
            /// Affinity discovery failed; fail closed to one shared arena (reachable from any CPU,
            /// no CPU map needed) instead of guessing a map that could leave arenas unreachable.
            LOG_WARNING(
                &Poco::Logger::get("JemallocMergeTreeArena"),
                "Cannot determine the CPUs this process may run on; "
                "using a single dedicated MergeTree arena instead of {}.",
                num_arenas);
            num_allowed_cpus = 1;
        }
        num_arenas = std::min(num_arenas, num_allowed_cpus);
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

        cpu_slots = std::move(slots);
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

    return arena_indices[cpu_slots->getCurrentSlot() % n];
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
