#include <cstdlib>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/memory.h>


/** These functions can be substituted instead of regular ones when memory tracking is needed.
  */

extern "C" void * clickhouse_malloc(size_t size)
{
    void * res = malloc(size);
    if (likely(res))
    {
        AllocationTrace trace;
        size_t actual_size = Memory::trackMemoryNoExcept(size, trace);
        if (unlikely(trace.isNull()))
        {
            free(res);
            return nullptr;
        }
        trace.onAlloc(res, actual_size);
    }
    return res;
}

extern "C" void * clickhouse_calloc(size_t number_of_members, size_t size)
{
    void * res = calloc(number_of_members, size);
    if (likely(res))
    {
        AllocationTrace trace;
        size_t actual_size = Memory::trackMemoryNoExcept(number_of_members * size, trace);
        if (unlikely(trace.isNull()))
        {
            free(res);
            return nullptr;
        }
        trace.onAlloc(res, actual_size);
    }
    return res;
}

extern "C" void * clickhouse_realloc(void * ptr, size_t size)
{
    AllocationTrace trace_free;
    size_t original_size = 0;
    if (likely(ptr))
        original_size = Memory::untrackMemory(ptr, trace_free);

    AllocationTrace trace_alloc;
    size_t new_size = Memory::trackMemoryNoExcept(size, trace_alloc);
    if (unlikely(trace_alloc.isNull()))
    {
        /// The new request failed. We need to track again the previous one (and ignore any errors)
        MemoryTrackerBlockerInThread memory_blocker;
        trace_alloc = CurrentMemoryTracker::allocNoThrow(original_size);
        return nullptr;
    }

    void * res = realloc(ptr, size);
    if (unlikely(!res))
    {
        /// Both changes worked but the final allocation failed, so we need to revert them
        MemoryTrackerBlockerInThread memory_blocker;
        if (original_size < new_size)
            trace_alloc = CurrentMemoryTracker::free(new_size - original_size);
        else if (original_size > new_size)
            trace_alloc = CurrentMemoryTracker::allocNoThrow(original_size - new_size);
        return nullptr;
    }

    trace_free.onFree(ptr, original_size);
    trace_alloc.onAlloc(res, new_size);
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
    AllocationTrace trace;
    size_t actual_size = Memory::untrackMemory(ptr, trace);
    trace.onFree(ptr, actual_size);
    free(ptr);
}

extern "C" int clickhouse_posix_memalign(void ** memptr, size_t alignment, size_t size)
{
    int res = posix_memalign(memptr, alignment, size);
    if (likely(res == 0))
    {
        AllocationTrace trace;
        size_t actual_size = Memory::trackMemoryNoExcept(size, trace);
        if (unlikely(trace.isNull()))
        {
            free(*memptr);
            return ENOMEM;
        }
        trace.onAlloc(*memptr, actual_size);
    }
    return res;
}
