#pragma once

#include <Common/MemoryTrackerSwitcher.h>

#include <memory>

namespace DB
{

/// Keeps storage that can outlive a query in the global tracker. Object construction and
/// destruction still use the caller's tracker, including allocations owned by the object.
template <typename T>
struct GlobalMemoryAllocator
{
    using value_type = T;

    GlobalMemoryAllocator() = default;
    template <typename U>
    explicit GlobalMemoryAllocator(const GlobalMemoryAllocator<U> &)
    {
    }

    T * allocate(size_t count)
    {
        MemoryTrackerSwitcher scope(&total_memory_tracker, 0);
        return std::allocator<T>{}.allocate(count);
    }

    void deallocate(T * ptr, size_t count) noexcept
    {
        MemoryTrackerSwitcher scope(&total_memory_tracker, 0);
        std::allocator<T>{}.deallocate(ptr, count);
    }

    template <typename U>
    bool operator==(const GlobalMemoryAllocator<U> &) const noexcept
    {
        return true;
    }
};

}
