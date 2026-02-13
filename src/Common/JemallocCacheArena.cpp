#include <Common/JemallocCacheArena.h>

#if USE_JEMALLOC

#include <jemalloc/jemalloc.h>

#include <Common/Exception.h>
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
        throw DB::ErrnoException(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocCacheArena: Failed to create jemalloc arena, error: {}", err);
    return arena_index;
}

/// MIB-cached purge command for the cache arena.
/// We can't use Jemalloc::MibCache here because the arena index is dynamic
/// (determined at runtime), so we format the mallctl name once and cache the MIB.
struct PurgeMib
{
    size_t mib[3];
    size_t mib_length = 3;

    explicit PurgeMib(unsigned arena_index)
    {
        std::string name = fmt::format("arena.{}.purge", arena_index);
        mallctlnametomib(name.c_str(), mib, &mib_length);
    }

    void run() const
    {
        mallctlbymib(mib, mib_length, nullptr, nullptr, nullptr, 0);
    }
};

}

unsigned getArenaIndex()
{
    static unsigned index = createArena();
    return index;
}

void purge()
{
    static PurgeMib purge_mib(getArenaIndex());

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
