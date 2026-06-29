#include <Common/Arena.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event ArenaAllocChunks;
    extern const Event ArenaAllocBytes;
}

namespace DB
{

Arena::MemoryChunk::MemoryChunk(size_t size_)
{
    ProfileEvents::increment(ProfileEvents::ArenaAllocChunks);
    ProfileEvents::increment(ProfileEvents::ArenaAllocBytes, size_);

    begin = reinterpret_cast<char *>(Allocator<false>::alloc(size_));
    pos = begin;
    end = begin + size_ - pad_right;

    ASAN_POISON_MEMORY_REGION(begin, size_);
}

}
