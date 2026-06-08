#pragma once

#include <cstddef>
#include <cstdlib>
#include <optional>
#include <type_traits>

#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/VariableContext.h>


/// Implementation of std::allocator interface that tracks memory with MemoryTracker.
/// NOTE We already plug MemoryTracker into new/delete operators. So, everything works even with default allocator.
/// But it is enabled only if jemalloc is used (to obtain the size of the allocation on call to delete).
/// And jemalloc is disabled for builds with sanitizers. In these cases memory was not always tracked.
///
/// Functions __real_malloc and __real_free are used to call the MemoryTracker explicitly, so
/// it works even with sanitizers which has its own mechanism for intercepting malloc and free.
///
/// `tracking_level` selects which memory tracker is charged and enforced. The default
/// (`VariableContext::Max`) installs no blocker, so allocations are charged at the natural
/// per-thread level — i.e. attributed to the query/thread doing the allocation. Passing a real
/// level (e.g. `VariableContext::User`) wraps every alloc/free in a `MemoryTrackerBlockerInThread`
/// of that level, so the per-thread/per-user trackers are skipped and only the parent (global/total)
/// tracker is charged and enforced. That is the right choice for process-wide containers that are
/// mutated from arbitrary threads (e.g. cross-thread scheduler queues): without it, a query thread
/// can spuriously hit `MEMORY_LIMIT_EXCEEDED` just by enqueueing shared work, and the alloc/free
/// would be charged to different threads' trackers, making accounting diverge.
template <typename T, VariableContext tracking_level = VariableContext::Max>
struct AllocatorWithMemoryTracking
{
    using value_type = T;
    /// Allocator is stateless and thus always equal to another allocator (of the same tracking level).
    using is_always_equal = std::true_type;
    /// When propagate_on_container_move_assignment::value is:
    /// true: The container will move the allocator from the source to the destination during move assignment
    /// false (default): The container keeps its original allocator
    /// For a stateless allocator like this one, this option doesn't make a lot of sense and needed only
    /// to workaround a compilation error in our version of boost::container::devector.
    using propagate_on_container_move_assignment = std::true_type;

    /// A non-type template parameter defeats `std::allocator_traits`' automatic rebind (it forwards
    /// only type parameters), so the rebind must be spelled out to carry `tracking_level` across.
    template <typename U>
    struct rebind { using other = AllocatorWithMemoryTracking<U, tracking_level>; };

    AllocatorWithMemoryTracking() = default;

    template <typename U>
    constexpr explicit AllocatorWithMemoryTracking(const AllocatorWithMemoryTracking<U, tracking_level> &) noexcept
    {
    }

    [[nodiscard]] T * allocate(size_t n)
    {
        if (n > std::numeric_limits<size_t>::max() / sizeof(T))
            throw std::bad_alloc();

        size_t bytes = n * sizeof(T);

        std::optional<MemoryTrackerBlockerInThread> blocker;
        if constexpr (tracking_level != VariableContext::Max)
            blocker.emplace(tracking_level);

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
        {
            [[maybe_unused]] auto rollback_trace = CurrentMemoryTracker::free(bytes);
            throw std::bad_alloc();
        }

        trace.onAlloc(p, bytes);

        return p;
    }

    void deallocate(T * p, size_t n) noexcept
    {
        size_t bytes = n * sizeof(T);

        __real_free(p);

        std::optional<MemoryTrackerBlockerInThread> blocker;
        if constexpr (tracking_level != VariableContext::Max)
            blocker.emplace(tracking_level);

        auto trace = CurrentMemoryTracker::free(bytes);
        trace.onFree(p, bytes);
    }
};

template <typename T, VariableContext LT, typename U, VariableContext LU>
constexpr bool operator==(const AllocatorWithMemoryTracking<T, LT> &, const AllocatorWithMemoryTracking<U, LU> &)
{
    return LT == LU;
}

template <typename T, VariableContext LT, typename U, VariableContext LU>
constexpr bool operator!=(const AllocatorWithMemoryTracking<T, LT> &, const AllocatorWithMemoryTracking<U, LU> &)
{
    return LT != LU;
}
