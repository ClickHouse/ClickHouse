#pragma once

#include <cstddef>
#include <cstdlib>

#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>


/// Implementation of std::allocator interface that tracks memory with MemoryTracker.
/// NOTE We already plug MemoryTracker into new/delete operators. So, everything works even with default allocator.
/// But it is enabled only if jemalloc is used (to obtain the size of the allocation on call to delete).
/// And jemalloc is disabled for builds with sanitizers. In these cases memory was not always tracked.
///
/// Functions __real_malloc and __real_free are used to call the MemoryTracker explicitly, so
/// it works even with sanitizers which has its own mechanism for intercepting malloc and free.
template <typename T>
struct AllocatorWithMemoryTracking
{
    using value_type = T;

    AllocatorWithMemoryTracking() = default;

    template <typename U>
    constexpr explicit AllocatorWithMemoryTracking(const AllocatorWithMemoryTracking<U> &) noexcept
    {
    }

    [[nodiscard]] T * allocate(size_t n)
    {
        if (n > std::numeric_limits<size_t>::max() / sizeof(T))
            throw std::bad_alloc();

        size_t bytes = n * sizeof(T);
        auto trace = CurrentMemoryTracker::alloc(bytes);

        T * p = static_cast<T *>(__real_malloc(bytes));
        if (!p)
            throw std::bad_alloc();

        trace.onAlloc(p, bytes);

        return p;
    }

    void deallocate(T * p, size_t n) noexcept
    {
        size_t bytes = n * sizeof(T);

        __real_free(p);
        auto trace = CurrentMemoryTracker::free(bytes);
        trace.onFree(p, bytes);
    }
};

template <typename T, typename U>
bool operator==(const AllocatorWithMemoryTracking <T> &, const AllocatorWithMemoryTracking <U> &)
{
    return true;
}

template <typename T, typename U>
bool operator!=(const AllocatorWithMemoryTracking <T> &, const AllocatorWithMemoryTracking <U> &)
{
    return false;
}
