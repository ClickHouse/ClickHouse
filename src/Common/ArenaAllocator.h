#pragma once
#include <Common/Arena.h>
#include <Common/Allocator.h>


namespace DB
{

/// Allocator which proxies all allocations to Arena. Used in aggregate functions.
class ArenaAllocator
{
public:
    static void * alloc(size_t size, Arena * arena)
    {
        return arena->alloc(size);
    }

    static void * realloc(void * buf, const size_t old_size, const size_t new_size, Arena * arena)
    {
        char const * data = reinterpret_cast<char *>(buf);

        // Invariant should be maintained: new_size > old_size
        if (data + old_size == arena->head.pos)
        {
            // Consecutive optimization
            arena->allocContinue(new_size - old_size, data);
            return reinterpret_cast<void *>(const_cast<char *>(data));
        }

        return arena->realloc(data, old_size, new_size);
    }

    static void free(void * /*buf*/, size_t /*size*/)
    {
        // Do nothing, trash in arena remains.
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }
};


/// Allocates in Arena with proper alignment.
template <size_t alignment>
class AlignedArenaAllocator
{
public:
    static void * alloc(size_t size, Arena * arena)
    {
        return arena->alignedAlloc(size, alignment);
    }

    static void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        char const * data = reinterpret_cast<char *>(buf);

        if (data + old_size == arena->head.pos)
        {
            arena->allocContinue(new_size - old_size, data, alignment);
            return reinterpret_cast<void *>(const_cast<char *>(data));
        }

        return arena->alignedRealloc(data, old_size, new_size, alignment);
    }

    static void free(void * /*buf*/, size_t /*size*/)
    {
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }
};


/// Switches to ordinary Allocator after REAL_ALLOCATION_THRESHOLD bytes to avoid fragmentation and trash in Arena.
template <size_t REAL_ALLOCATION_THRESHOLD = 4096, typename TRealAllocator = Allocator<false>, typename TArenaAllocator = ArenaAllocator, size_t alignment = 0>
class MixedArenaAllocator : private TRealAllocator
{
public:

    void * alloc(size_t size, Arena * arena)
    {
        return (size < REAL_ALLOCATION_THRESHOLD) ? TArenaAllocator::alloc(size, arena) : TRealAllocator::alloc(size, alignment);
    }

    void * realloc(void * buf, size_t old_size, size_t new_size, Arena * arena)
    {
        // Invariant should be maintained: new_size > old_size

        if (new_size < REAL_ALLOCATION_THRESHOLD)
            return TArenaAllocator::realloc(buf, old_size, new_size, arena);

        if (old_size >= REAL_ALLOCATION_THRESHOLD)
            return TRealAllocator::realloc(buf, old_size, new_size, alignment);

        void * new_buf = TRealAllocator::alloc(new_size, alignment);
        memcpy(new_buf, buf, old_size);
        return new_buf;
    }

    void free(void * buf, size_t size)
    {
        if (size >= REAL_ALLOCATION_THRESHOLD)
            TRealAllocator::free(buf, size);
    }

protected:
    static constexpr size_t getStackThreshold()
    {
        return 0;
    }
};


template <size_t alignment, size_t REAL_ALLOCATION_THRESHOLD = 4096>
using MixedAlignedArenaAllocator = MixedArenaAllocator<REAL_ALLOCATION_THRESHOLD, Allocator<false>, AlignedArenaAllocator<alignment>, alignment>;

}
