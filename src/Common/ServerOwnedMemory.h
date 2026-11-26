#pragma once

#include <Common/MemoryTrackerBlockerInThread.h>

#include <cstddef>
#include <functional>
#include <memory>
#include <new>
#include <utility>


/// Memory that belongs to the server, not to the query or user of the thread that allocates or frees it: only the global
/// tracker counts it, and its limit still applies. Rule: what is allocated under it has to be freed under it too, on any
/// thread, so prefer `makeServerOwned`, which carries the rule with the object.
///
/// Not for avoiding `MEMORY_LIMIT_EXCEEDED` (use `LockMemoryExceptionInThread`), and not for the internals of the tracker
/// and of logging, which need `MemoryTrackerBlockerInThread(VariableContext::Global)`.
struct ServerOwnedMemoryScope : MemoryTrackerBlockerInThread
{
    ServerOwnedMemoryScope() : MemoryTrackerBlockerInThread(VariableContext::User) {}
};

namespace DB
{

/// Server-owned storage only: the object is constructed and destroyed under the scope of the caller. For an object
/// whose members are the query's while it lives, like a task that runs for a query.
template <typename T>
struct ServerOwnedAllocator
{
    using value_type = T;

    ServerOwnedAllocator() = default;
    template <typename U>
    explicit ServerOwnedAllocator(const ServerOwnedAllocator<U> &) {}

    T * allocate(size_t n)
    {
        ServerOwnedMemoryScope scope;
        return static_cast<T *>(::operator new(n * sizeof(T)));
    }

    void deallocate(T * p, size_t n) noexcept
    {
        ServerOwnedMemoryScope scope;
        ::operator delete(p, n * sizeof(T));
    }

    template <typename U>
    bool operator==(const ServerOwnedAllocator<U> &) const { return true; }
};

/// Also constructs and destroys the object in the scope, so what its constructor allocates and its destructor frees is
/// the server's as well, wherever the last reference goes. Members grown later still need a scope.
template <typename T>
struct ServerOwnedObjectAllocator : ServerOwnedAllocator<T>
{
    ServerOwnedObjectAllocator() = default;
    template <typename U>
    explicit ServerOwnedObjectAllocator(const ServerOwnedObjectAllocator<U> &) {}

    template <typename U, typename... Args>
    void construct(U * p, Args &&... args)
    {
        ServerOwnedMemoryScope scope;
        ::new (static_cast<void *>(p)) U(std::forward<Args>(args)...);
    }

    template <typename U>
    void destroy(U * p) noexcept
    {
        ServerOwnedMemoryScope scope;
        p->~U();
    }
};

template <typename T, typename... Args>
std::shared_ptr<T> makeServerOwned(Args &&... args)
{
    return std::allocate_shared<T>(ServerOwnedObjectAllocator<T>{}, std::forward<Args>(args)...);
}

/// `std::function` keeps a callable that is not trivially copyable on the heap, and a pool frees its job on a worker that is
/// not attached to the query. For a callable whose move allocates nothing, like one that holds only a `shared_ptr`.
template <typename F>
std::function<void()> makeServerOwnedJob(F && f)
{
    ServerOwnedMemoryScope scope;
    return std::function<void()>(std::forward<F>(f));
}

}
