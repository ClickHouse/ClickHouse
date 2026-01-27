#pragma once

#include <cstddef>
#include <cstdlib>
#include <type_traits>
#include <memory>

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
constexpr bool operator==(const AllocatorWithMemoryTracking <T> &, const AllocatorWithMemoryTracking <U> &)
{
    return true;
}

template <typename T, typename U>
constexpr bool operator!=(const AllocatorWithMemoryTracking <T> &, const AllocatorWithMemoryTracking <U> &)
{
    return false;
}


/// This allocator allows to track memory usage of containers of arbitrary types.
/// The usage is simple: container.get_allocator().getBytesAllocated()
template <typename T>
struct BytesAwareAllocatorWithMemoryTracking
{
    using value_type = T;
    using propagate_on_container_swap = std::true_type;
    using propagate_on_container_copy_assignment = std::false_type;
    using propagate_on_container_move_assignment = std::true_type;
    using is_always_equal = std::false_type;

    template <typename U>
    struct rebind
    {
        using other = BytesAwareAllocatorWithMemoryTracking<U>;
    };

    BytesAwareAllocatorWithMemoryTracking() = default;
    BytesAwareAllocatorWithMemoryTracking(const BytesAwareAllocatorWithMemoryTracking&) = default;
    BytesAwareAllocatorWithMemoryTracking& operator=(const BytesAwareAllocatorWithMemoryTracking&) = default;

    BytesAwareAllocatorWithMemoryTracking(BytesAwareAllocatorWithMemoryTracking && other) noexcept
        : bytes_allocated(std::move(other.bytes_allocated))
    {
        other.bytes_allocated = std::make_shared<size_t>(0);
    }

    BytesAwareAllocatorWithMemoryTracking& operator=(BytesAwareAllocatorWithMemoryTracking && other) noexcept
    {
        bytes_allocated = other.bytes_allocated;
        other.bytes_allocated = std::make_shared<size_t>(0);
        return *this;
    }

    template <typename U>
    BytesAwareAllocatorWithMemoryTracking(const BytesAwareAllocatorWithMemoryTracking<U>& other)
        : bytes_allocated(other.bytes_allocated) {}

    T* allocate(size_t n)
    {
        *bytes_allocated += n * sizeof(T);
        return AllocatorWithMemoryTracking<T>().allocate(n);
    }

    void deallocate(T * p, size_t n)
    {
        *bytes_allocated -= n * sizeof(T);
        AllocatorWithMemoryTracking<T>().deallocate(p, n);
    }

    /// NOLINTNEXTLINE
    BytesAwareAllocatorWithMemoryTracking<T> select_on_container_copy_construction() const
    {
        /// During the container copy, the copied allocator should receive
        /// the previously defined amount of bytes allocated and then live its own life.
        BytesAwareAllocatorWithMemoryTracking<T> allocator;
        allocator.bytes_allocated = bytes_allocated;
        return allocator;
    }

    size_t getBytesAllocated() const
    {
        return *bytes_allocated;
    }

    template <typename U>
    bool operator==(const BytesAwareAllocatorWithMemoryTracking<U>& other) const
    {
        return bytes_allocated == other.bytes_allocated;
    }

    std::shared_ptr<size_t> bytes_allocated = std::make_shared<size_t>(0);
};
