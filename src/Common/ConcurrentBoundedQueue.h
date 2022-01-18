#pragma once

#include <queue>
#include <type_traits>
#include <atomic>

#include <Poco/Mutex.h>
#include <Poco/Semaphore.h>

#include <common/MoveOrCopyIfThrow.h>
#include <Common/Exception.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}
}

/** A very simple thread-safe queue of limited size.
  * If you try to pop an item from an empty queue, the thread is blocked until the queue becomes nonempty.
  * If you try to push an element into an overflowed queue, the thread is blocked until space appears in the queue.
  */
template <typename T>
class ConcurrentBoundedQueue
{
private:
    std::queue<T> queue;
    mutable Poco::FastMutex mutex;
    Poco::Semaphore fill_count;
    Poco::Semaphore empty_count;
    std::atomic_bool closed = false;

    template <typename... Args>
    bool tryEmplaceImpl(Args &&... args)
    {
        bool emplaced = true;

        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            if (closed)
                emplaced = false;
            else
                queue.emplace(std::forward<Args>(args)...);
        }

        if (emplaced)
            fill_count.set();
        else
            empty_count.set();

        return emplaced;
    }

    void popImpl(T & x)
    {
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop();
        }
        empty_count.set();
    }

public:
    explicit ConcurrentBoundedQueue(size_t max_fill)
        : fill_count(0, max_fill)
        , empty_count(max_fill, max_fill)
    {}

    void push(const T & x)
    {
        empty_count.wait();
        if (!tryEmplaceImpl(x))
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "tryPush/tryEmplace must be used with close()");
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        empty_count.wait();
        if (!tryEmplaceImpl(std::forward<Args>(args)...))
            throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "tryPush/tryEmplace must be used with close()");
    }

    void pop(T & x)
    {
        fill_count.wait();
        popImpl(x);
    }

    bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        if (!empty_count.tryWait(milliseconds))
            return false;

        return tryEmplaceImpl(x);
    }

    template <typename... Args>
    bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        if (!empty_count.tryWait(milliseconds))
            return false;

        return tryEmplaceImpl(std::forward<Args>(args)...);
    }

    bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        if (!fill_count.tryWait(milliseconds))
            return false;

        popImpl(x);
        return true;
    }

    size_t size() const
    {
        Poco::ScopedLock<Poco::FastMutex> lock(mutex);
        return queue.size();
    }

    size_t empty() const
    {
        Poco::ScopedLock<Poco::FastMutex> lock(mutex);
        return queue.empty();
    }

    /// Forbids to push new elements to queue.
    /// Returns false if queue was not closed before call, returns true if queue was already closed.
    bool close()
    {
        Poco::ScopedLock<Poco::FastMutex> lock(mutex);
        return closed.exchange(true);
    }

    bool isClosed() const
    {
        return closed.load();
    }

    void clear()
    {
        while (fill_count.tryWait(0))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                queue.pop();
            }
            empty_count.set();
        }
    }
};
