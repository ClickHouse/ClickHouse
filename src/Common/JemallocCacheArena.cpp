#include <Common/JemallocCacheArena.h>

#if USE_JEMALLOC

#include <jemalloc/jemalloc.h>

#include <Common/Exception.h>
#include <Common/Jemalloc.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <fmt/format.h>
#include <string>

namespace ProfileEvents
{
    extern const Event MemoryAllocatorPurge;
    extern const Event MemoryAllocatorPurgeTimeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_ALLOCATE_MEMORY;
}

}

namespace DB::JemallocCacheArena
{

namespace
{

unsigned createArena()
{
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int err = mallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (err)
        throw DB::Exception(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocCacheArena: Failed to create jemalloc arena, error: {}", err);
    return arena_index;
}

}

unsigned getArenaIndex()
{
    static unsigned index = createArena();
    return index;
}

void purge()
{
    static Jemalloc::MibCache<unsigned> purge_mib(fmt::format("arena.{}.purge", getArenaIndex()).c_str());

    Stopwatch watch;
    purge_mib.run();
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurge);
    ProfileEvents::increment(ProfileEvents::MemoryAllocatorPurgeTimeMicroseconds, watch.elapsedMicroseconds());
}

}

#else

namespace DB::JemallocCacheArena
{

unsigned getArenaIndex() { return 0; }
void purge() {}

}

#endif
