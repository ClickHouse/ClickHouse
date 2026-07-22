#include <Common/JemallocJITArena.h>

#include "config.h"

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

namespace DB::JemallocJITArena
{

namespace
{

struct ArenaState
{
    unsigned index;
    bool created;
};

ArenaState createArena()
{
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int err = je_mallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (err)
    {
        /// Don't throw: this arena is a fragmentation optimization, not a correctness prerequisite.
        /// `getArenaIndex` is invoked unconditionally on every JIT compile path; throwing here
        /// would turn a transient allocator failure or any environment that disallows extra
        /// arenas into hard exceptions. Fall back to arena 0 — passing 0 to
        /// `ScopedJemallocThreadArena` is already a documented no-op that allocates from the
        /// default arena. The cost is degraded fragmentation, not correctness.
        LOG_ERROR(
            &Poco::Logger::get("JemallocJITArena"),
            "Failed to create dedicated jemalloc JIT arena (mallctl error: {}). "
            "JIT allocations will use the default arena and the fragmentation reduction "
            "documented for `jemalloc.jit_arena.*` is disabled.",
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
    /// The arena only has callers when JIT is built in. Without USE_EMBEDDED_COMPILER, nothing
    /// allocates here; reporting `enabled` would create a useless empty arena and expose a
    /// permanently-zero metric. Pair the runtime jemalloc check with the compile-time JIT check.
#if USE_EMBEDDED_COMPILER
    return state().created;
#else
    return false;
#endif
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

namespace DB::JemallocJITArena
{

unsigned getArenaIndex() { return 0; }
bool isEnabled() { return false; }
void purge() {}

}

#endif
