#include <Common/JemallocCacheArena.h>

#if USE_JEMALLOC

#include <jemalloc/jemalloc.h>

#include <Common/Exception.h>
#include <Common/Jemalloc.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>

#include <fmt/format.h>
#include <string>
#include <atomic>

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

std::atomic<bool> enabled{true};

namespace
{

bool arena_created = false;

unsigned createArena()
{
    unsigned arena_index = 0;
    size_t arena_index_size = sizeof(arena_index);
    int err = je_mallctl("arenas.create", &arena_index, &arena_index_size, nullptr, 0);
    if (err)
        throw DB::Exception(DB::ErrorCodes::CANNOT_ALLOCATE_MEMORY, "JemallocCacheArena: Failed to create jemalloc arena, error: {}", err);
    arena_created = true;
    return arena_index;
}

}

void setEnabled(bool value)
{
    chassert(!arena_created || value);
    enabled.store(value, std::memory_order_relaxed);
}

bool isEnabled()
{
    return enabled.load(std::memory_order_relaxed);
}

unsigned getArenaIndex()
{
    if (!enabled.load(std::memory_order_relaxed))
        return 0;

    static unsigned index = createArena();
    return index;
}

void purge()
{
    if (!enabled.load(std::memory_order_relaxed))
        return;

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

void setEnabled(bool) {}
bool isEnabled() { return false; }
unsigned getArenaIndex() { return 0; }
void purge() {}

}

#endif
