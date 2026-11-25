#pragma once

#include <Common/MemoryTrackerBlockerInThread.h>

#include <cstddef>
#include <new>


namespace DB
{

/// For memory that belongs to the server, not to the query or user of the thread that allocates or frees it.
template <typename T>
struct ServerOwnedAllocator
{
    using value_type = T;

    ServerOwnedAllocator() = default;
    template <typename U>
    explicit ServerOwnedAllocator(const ServerOwnedAllocator<U> &) {}

    T * allocate(size_t n)
    {
        MemoryTrackerBlockerInThread not_charged_to_query_or_user;
        return static_cast<T *>(::operator new(n * sizeof(T)));
    }

    void deallocate(T * p, size_t n) noexcept
    {
        MemoryTrackerBlockerInThread not_charged_to_query_or_user;
        ::operator delete(p, n * sizeof(T));
    }

    template <typename U>
    bool operator==(const ServerOwnedAllocator<U> &) const { return true; }
};

}
