#pragma once

#include <cstddef>
#include <limits>
#include <new>
#include <type_traits>
#include <memory>
#include <utility>

#include <base/defines.h>

/// Not used directly anymore (the allocation machinery moved to the .cpp), but kept so
/// the set of symbols this widely-included header transitively provides (e.g. config.h
/// macros) does not change for downstream translation units.
#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>


/// The throw is kept in a cold, never-inlined helper so that allocate() (which is
/// inlined into hot allocation sites) does not carry exception-handling machinery.
[[noreturn]] [[gnu::cold]] NO_INLINE inline void throwBadAllocFromAllocatorWithMemoryTracking()
{
    throw std::bad_alloc();
}

/// Out-of-line allocation primitives. Keeping the tracker + malloc + onAlloc/onFree
/// machinery in the .cpp lets allocate()/deallocate() inline to a single call, the same
/// shape std::allocator has (operator new/delete are also out-of-line calls). With the
/// machinery inlined, the container's fill-constructor became too large to inline into
/// callers and turned into an out-of-line call, which perturbed register allocation in
/// hot functions. `alignment == 0` selects the default (max_align_t) path.
[[nodiscard]] void * allocateWithMemoryTracking(size_t bytes, size_t alignment);
void deallocateWithMemoryTracking(void * p, size_t bytes) noexcept;


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
        if (n > std::numeric_limits<size_t>::max() / sizeof(T)) [[unlikely]]
            throwBadAllocFromAllocatorWithMemoryTracking();

        constexpr size_t alignment = alignof(T) > alignof(std::max_align_t) ? alignof(T) : 0;
        return static_cast<T *>(allocateWithMemoryTracking(n * sizeof(T), alignment));
    }

    void deallocate(T * p, size_t n) noexcept
    {
        deallocateWithMemoryTracking(p, n * sizeof(T));
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

    /// A moved-from allocator must stay usable (the container may keep using it), so leave it
    /// with a fresh, empty counter instead of a null pointer.
    BytesAwareAllocatorWithMemoryTracking(BytesAwareAllocatorWithMemoryTracking && other) noexcept
        : bytes_allocated(std::exchange(other.bytes_allocated, std::make_shared<size_t>(0)))
    {
    }

    BytesAwareAllocatorWithMemoryTracking& operator=(BytesAwareAllocatorWithMemoryTracking && other) noexcept
    {
        bytes_allocated = std::exchange(other.bytes_allocated, std::make_shared<size_t>(0));
        return *this;
    }

    /// Rebinding converting constructor required by the allocator interface; it must stay
    /// non-explicit and share the counter so rebound node allocators track the same container.
    /// NOLINTNEXTLINE(google-explicit-constructor)
    template <typename U>
    BytesAwareAllocatorWithMemoryTracking(const BytesAwareAllocatorWithMemoryTracking<U>& other) /// NOLINT
        : bytes_allocated(other.bytes_allocated) {}

    T* allocate(size_t n)
    {
        /// Increment only after a successful allocation so the counter does not drift upward
        /// when the underlying allocation throws.
        T * result = AllocatorWithMemoryTracking<T>().allocate(n);
        *bytes_allocated += n * sizeof(T);
        return result;
    }

    void deallocate(T * p, size_t n)
    {
        *bytes_allocated -= n * sizeof(T);
        AllocatorWithMemoryTracking<T>().deallocate(p, n);
    }

    /// NOLINTNEXTLINE
    BytesAwareAllocatorWithMemoryTracking<T> select_on_container_copy_construction() const
    {
        /// A copied container allocates its own nodes through the returned allocator, so the
        /// copy must start with a fresh (zero) counter and accumulate exactly its own bytes.
        /// Sharing `bytes_allocated` would make the source and the copy double-count into the
        /// same counter; copying the current value would over-count once the copy re-allocates.
        return BytesAwareAllocatorWithMemoryTracking<T>();
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

    /// The counter is held by `shared_ptr` so that all allocator instances the container creates
    /// internally (copies, and rebound node allocators produced via `rebind`) share one counter
    /// and report the container's total byte usage through any of them.
    std::shared_ptr<size_t> bytes_allocated = std::make_shared<size_t>(0);
};
