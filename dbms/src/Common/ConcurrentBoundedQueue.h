#pragma once

#include <queue>
#include <type_traits>

#include <Poco/Mutex.h>
#include <Poco/Semaphore.h>

#include <Core/Types.h>

namespace detail
{
    template <class T, bool is_nothrow_move_assignable = std::is_nothrow_move_assignable<T>::value>
    struct MoveOrCopyIfThrow;

    template <class T>
    struct MoveOrCopyIfThrow<T, true>
    {
        void operator()(T && src, T & dst) const
        {
            dst = std::forward<T>(src);
        }
    };

    template <class T>
    struct MoveOrCopyIfThrow<T, false>
    {
        void operator()(T && src, T & dst) const
        {
            dst = src;
        }
    };

    template <class T>
    void moveOrCopyIfThrow(T && src, T & dst)
    {
        MoveOrCopyIfThrow<T>()(std::forward<T>(src), dst);
    }
};

/** A very simple thread-safe queue of limited length.
  * If you try to pop an item from an empty queue, the thread is blocked until the queue becomes nonempty.
  * If you try to push an element into an overflowed queue, the thread is blocked until space appears in the queue.
  */
template <typename T>
class ConcurrentBoundedQueue
{
private:
    size_t max_fill;
    std::queue<T> queue;
    Poco::FastMutex mutex;
    Poco::Semaphore fill_count;
    Poco::Semaphore empty_count;

public:
    ConcurrentBoundedQueue(size_t max_fill)
        : fill_count(0, max_fill), empty_count(max_fill, max_fill) {}

    void push(const T & x)
    {
        empty_count.wait();
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            queue.push(x);
        }
        fill_count.set();
    }

    template <class ... Args>
    void emplace(Args && ... args)
    {
        empty_count.wait();
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            queue.emplace(std::forward<Args>(args)...);
        }
        fill_count.set();
    }

    void pop(T & x)
    {
        fill_count.wait();
        {
            Poco::ScopedLock<Poco::FastMutex> lock(mutex);
            detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop();
        }
        empty_count.set();
    }

    bool tryPush(const T & x, DB::UInt64 milliseconds = 0)
    {
        if (empty_count.tryWait(milliseconds))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                queue.push(x);
            }
            fill_count.set();
            return true;
        }
        return false;
    }

    template <class ... Args>
    bool tryEmplace(DB::UInt64 milliseconds, Args && ... args)
    {
        if (empty_count.tryWait(milliseconds))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                queue.emplace(std::forward<Args>(args)...);
            }
            fill_count.set();
            return true;
        }
        return false;
    }

    bool tryPop(T & x, DB::UInt64 milliseconds = 0)
    {
        if (fill_count.tryWait(milliseconds))
        {
            {
                Poco::ScopedLock<Poco::FastMutex> lock(mutex);
                detail::moveOrCopyIfThrow(std::move(queue.front()), x);
                queue.pop();
            }
            empty_count.set();
            return true;
        }
        return false;
    }

    size_t size()
    {
        Poco::ScopedLock<Poco::FastMutex> lock(mutex);
        return queue.size();
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
