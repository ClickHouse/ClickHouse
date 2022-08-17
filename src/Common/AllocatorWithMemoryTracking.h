#pragma once

#include <stdexcept>
#include <cstddef>
#include <cstdlib>

#include <Common/CurrentMemoryTracker.h>


/// Implementation of std::allocator interface that tracks memory with MemoryTracker.
/// NOTE We already plug MemoryTracker into new/delete operators. So, everything works even with default allocator.
/// But it is enabled only if jemalloc is used (to obtain the size of the allocation on call to delete).
/// And jemalloc is disabled for builds with sanitizers. In these cases memory was not always tracked.

template <typename T>
struct AllocatorWithMemoryTracking
{
    typedef T value_type;

    AllocatorWithMemoryTracking() = default;

    template <typename U>
    constexpr AllocatorWithMemoryTracking(const AllocatorWithMemoryTracking<U> &) noexcept
    {
    }

    [[nodiscard]] T * allocate(size_t n)
    {
        if (n > std::numeric_limits<size_t>::max() / sizeof(T))
            throw std::bad_alloc();

        size_t bytes = n * sizeof(T);
        CurrentMemoryTracker::alloc(bytes);

        T * p = static_cast<T *>(malloc(bytes));
        if (!p)
            throw std::bad_alloc();

        return p;
    }

    void deallocate(T * p, size_t n) noexcept
    {
        free(p);

        size_t bytes = n * sizeof(T);
        CurrentMemoryTracker::free(bytes);
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

