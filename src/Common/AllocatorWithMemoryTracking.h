#pragma once

#include <cstddef>
#include <cstdlib>
#include <type_traits>

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
    /// Allocator is stateless and thus always equal to another allocator.
    using is_always_equal = std::true_type;
    /// When propagate_on_container_move_assignment::value is:
    /// true: The container will move the allocator from the source to the destination during move assignment
    /// false (default): The container keeps its original allocator
    /// For a stateless allocator like this one, this option doesn't make a lot of sense and needed only
    /// to workaround a compilation error in our version of boost::container::devector.
    using propagate_on_container_move_assignment = std::true_type;

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

        T * p = nullptr;
        if constexpr (alignof(T) > alignof(std::max_align_t))
        {
            const int res = __real_posix_memalign(reinterpret_cast<void **>(&p), alignof(T), bytes);
            if (res != 0)
                // We don't have to, but to be sure
                p = nullptr;
        }
        else
        {
            p = static_cast<T *>(__real_malloc(bytes));
        }
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
constexpr bool operator==(const AllocatorWithMemoryTracking <T> &, const AllocatorWithMemoryTracking <U> &)
{
    return true;
}

template <typename T, typename U>
constexpr bool operator!=(const AllocatorWithMemoryTracking <T> &, const AllocatorWithMemoryTracking <U> &)
{
    return false;
}
