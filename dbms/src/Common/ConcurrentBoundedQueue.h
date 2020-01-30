#pragma once

#include <queue>
#include <type_traits>

#include <Poco/Mutex.h>
#include <Poco/Semaphore.h>

#include <common/Types.h>


namespace detail
{
    template <typename T, bool is_nothrow_move_assignable = std::is_nothrow_move_assignable_v<T>>
    struct MoveOrCopyIfThrow;

    template <typename T>
    struct MoveOrCopyIfThrow<T, true>
    {
        void operator()(T && src, T & dst) const
        {
            dst = std::forward<T>(src);
        }
    };

    template <typename T>
    struct MoveOrCopyIfThrow<T, false>
    {
        void operator()(T && src, T & dst) const
        {
            dst = src;
        }
    };

    template <typename T>
    void moveOrCopyIfThrow(T && src, T & dst)
    {
        MoveOrCopyIfThrow<T>()(std::forward<T>(src), dst);
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
    const size_t max_fill;
    std::mutex mutex;
    std::condition_variable changed;

public:
    ConcurrentBoundedQueue(size_t max_fill_)
        : max_fill(max_fill_)
    {
    }

    void push(const T & x)
    {
        std::unique_lock<std::mutex> lock(mutex);
        changed.wait(lock, [&] () { return queue.size() < max_fill; } );

        queue.push(x);
        changed.notify_all();
    }

    template <typename... Args>
    void emplace(Args &&... args)
    {
        std::unique_lock<std::mutex> lock(mutex);
        changed.wait(lock, [&] () { return queue.size() < max_fill; } );

        queue.emplace(std::forward<Args>(args)...);
        changed.notify_all();
    }

    void pop(T & x)
    {
        std::unique_lock<std::mutex> lock(mutex);
        changed.wait(lock, [&] () { return queue.size() > 0; } );

        detail::moveOrCopyIfThrow(std::move(queue.front()), x);
        queue.pop();

        changed.notify_all();
    }

    bool tryPush(const T & x, UInt64 milliseconds = 0)
    {
        std::unique_lock<std::mutex> lock(mutex);
        bool result = changed.wait_for(lock,
            std::chrono::milliseconds(milliseconds), [&] () { return queue.size() < max_fill; } );

        if (result)
        {
            queue.push(x);
            changed.notify_all();
        }

        return result;
    }

    template <typename... Args>
    bool tryEmplace(UInt64 milliseconds, Args &&... args)
    {
        std::unique_lock<std::mutex> lock(mutex);
        bool result = changed.wait_for(lock,
            std::chrono::milliseconds(milliseconds), [&] () { return queue.size() < max_fill; } );

        if (result)
        {
            queue.emplace(std::forward<Args>(args)...);
            changed.notify_all();
        }

        return result;
    }

    bool tryPop(T & x, UInt64 milliseconds = 0)
    {
        std::unique_lock<std::mutex> lock(mutex);
        bool result = changed.wait_for(lock,
            std::chrono::milliseconds(milliseconds), [&] () { return queue.size() > 0; } );

        if (result)
        {
            detail::moveOrCopyIfThrow(std::move(queue.front()), x);
            queue.pop();
            changed.notify_all();
        }

        return result;
    }

    size_t size()
    {
        std::unique_lock<std::mutex> lock(mutex);
        return queue.size();
    }

    void clear()
    {
        std::unique_lock<std::mutex> lock(mutex);
        std::queue<T> to_destroy;
        queue.swap(to_destroy);
        changed.notify_all();
    }
};
