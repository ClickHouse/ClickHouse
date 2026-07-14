#include <Common/JemallocMergeTreeArena.h>

#if USE_JEMALLOC

#include <Common/Jemalloc.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <Common/logger_useful.h>

#include <fmt/format.h>
#include <jemalloc/jemalloc.h>
#include <string>

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
}

namespace DB::JemallocMergeTreeArena
{

namespace
{

struct ArenaState
{
    unsigned index;
    bool enabled;
};

ArenaState createArena()
{
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int err = je_mallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (err)
    {
        /// Don't throw: this arena is a fragmentation optimization, not a correctness prerequisite.
        /// `getArenaIndex` is on every part-loading and table-creating hot path; throwing here
        /// would brick startup and every CREATE / INSERT / merge. Fall back to arena 0 — passing 0
        /// to `ScopedJemallocThreadArena` is already a documented no-op that allocates from the
        /// default arena. The cost is degraded fragmentation, not correctness.
        LOG_ERROR(
            &Poco::Logger::get("JemallocMergeTreeArena"),
            "Failed to create dedicated jemalloc MergeTree arena (mallctl error: {}). "
            "MergeTree allocations will use the default arena and the fragmentation reduction "
            "documented for `jemalloc.mergetree_arena.*` is disabled.",
            err);
        return {0, false};
    }
    return {arena_index, true};
}

const ArenaState & state()
{
    static const ArenaState s = createArena();
    return s;
}

}

unsigned getArenaIndex()
{
    return state().index;
}

bool isEnabled()
{
    return state().enabled;
}

void purge()
{
    if (!isEnabled())
        return;

    static Jemalloc::MibCache<unsigned> purge_mib(fmt::format("arena.{}.purge", getArenaIndex()).c_str());

    Stopwatch watch;
    purge_mib.run();
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
}

}

#else

namespace DB::JemallocMergeTreeArena
{

unsigned getArenaIndex() { return 0; }
bool isEnabled() { return false; }
void purge() {}

}

#endif
