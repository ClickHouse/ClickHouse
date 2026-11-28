#pragma once

#include <cstddef>
#include <limits>
#include <type_traits>

#include <Common/AllocatorWithMemoryTracking.h>
#include <Common/JemallocNoDumpArenas.h>


/// Implementation of std::allocator interface that places memory in `JemallocNoDumpArenas`:
/// tracked by the memory tracker and excluded from core dumps.
template <typename T>
struct NoDumpAllocator
{
    using value_type = T;
    using is_always_equal = std::true_type;
    using propagate_on_container_move_assignment = std::true_type;

    NoDumpAllocator() = default;

    template <typename U>
    constexpr explicit NoDumpAllocator(const NoDumpAllocator<U> &) noexcept
    {
    }

    [[nodiscard]] T * allocate(size_t n)
    {
        if (n > std::numeric_limits<size_t>::max() / sizeof(T)) [[unlikely]]
            throwBadAllocFromAllocatorWithMemoryTracking();

        return static_cast<T *>(DB::JemallocNoDumpArenas::allocate(n * sizeof(T), alignof(T)));
    }

    void deallocate(T * p, size_t) noexcept
    {
        DB::JemallocNoDumpArenas::deallocate(p);
    }
};

template <typename T, typename U>
constexpr bool operator==(const NoDumpAllocator<T> &, const NoDumpAllocator<U> &)
{
    return true;
}

template <typename T, typename U>
constexpr bool operator!=(const NoDumpAllocator<T> &, const NoDumpAllocator<U> &)
{
    return false;
}
