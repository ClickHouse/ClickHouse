#include <Common/memory.h>
#include <cstdlib>
#include <Common/MemoryAllocationTracker_fwd.h>


/** These functions can be substituted instead of regular ones when memory tracking is needed.
  */

extern "C" void * clickhouse_malloc(size_t size)
{
    void * res = malloc(size);
    if (res)
    {
        size_t actual_size = Memory::trackMemory(size);
        MemoryAllocationTracker::track_alloc(res, actual_size);
    }
    return res;
}

extern "C" void * clickhouse_calloc(size_t number_of_members, size_t size)
{
    void * res = calloc(number_of_members, size);
    if (res)
    {
        size_t actual_size = Memory::trackMemory(number_of_members * size);
        MemoryAllocationTracker::track_alloc(res, actual_size);
    }
    return res;
}

extern "C" void * clickhouse_realloc(void * ptr, size_t size)
{
    if (ptr)
    {
        size_t actual_size = Memory::untrackMemory(ptr);
        MemoryAllocationTracker::track_free(ptr, actual_size);
    }
    void * res = realloc(ptr, size);
    if (res)
    {
        size_t actual_size = Memory::trackMemory(size);
        MemoryAllocationTracker::track_alloc(res, actual_size);
    }
    return res;
}

extern "C" void * clickhouse_reallocarray(void * ptr, size_t number_of_members, size_t size)
{
    size_t real_size = 0;
    if (__builtin_mul_overflow(number_of_members, size, &real_size))
        return nullptr;

    return clickhouse_realloc(ptr, real_size);
}

extern "C" void clickhouse_free(void * ptr)
{
    size_t actual_size = Memory::untrackMemory(ptr);
    MemoryAllocationTracker::track_free(ptr, actual_size);
    free(ptr);
}

extern "C" int clickhouse_posix_memalign(void ** memptr, size_t alignment, size_t size)
{
    int res = posix_memalign(memptr, alignment, size);
    if (res == 0)
    {
        size_t actual_size = Memory::trackMemory(size);
        MemoryAllocationTracker::track_alloc(*memptr, actual_size);
    }
    return res;
}
