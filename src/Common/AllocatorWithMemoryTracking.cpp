#include <Common/AllocatorWithMemoryTracking.h>

#include <cstdlib>

#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>


void * allocateWithMemoryTracking(size_t bytes, size_t alignment)
{
    auto trace = CurrentMemoryTracker::alloc(static_cast<Int64>(bytes));

    void * p = nullptr;
    if (alignment != 0)
    {
        const int res = __real_posix_memalign(&p, alignment, bytes);
        if (res != 0)
            // We don't have to, but to be sure
            p = nullptr;
    }
    else
    {
        p = __real_malloc(bytes);
    }

    if (!p) [[unlikely]]
    {
        [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
        throwBadAllocFromAllocatorWithMemoryTracking();
    }

    trace.onAlloc(p, bytes);
    return p;
}

void deallocateWithMemoryTracking(void * p, size_t bytes) noexcept
{
    __real_free(p);
    auto trace = CurrentMemoryTracker::free(static_cast<Int64>(bytes));
    trace.onFree(p, bytes);
}
