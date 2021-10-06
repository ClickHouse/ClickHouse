#pragma once

#include <queue>
#include <mutex>

#include <base/MoveOrCopyIfThrow.h>


namespace DB
{

/// Queue with mutex and condvar. As simple as possible.
template <typename T>
class ThreadSafeQueue
{
private:
    mutable std::mutex queue_mutex;
    std::condition_variable cv;
    std::queue<T> queue;
    bool is_finished;
public:

    bool push(const T & response)
    {
        {
            std::lock_guard lock(queue_mutex);

            if (is_finished)
                return false;

            queue.push(response);
        }

        cv.notify_one();
        return true;
    }

    [[nodiscard]] bool tryPop(T & response, int64_t timeout_ms = 0)
    {
        std::unique_lock lock(queue_mutex);
        if (!cv.wait_for(lock,
                std::chrono::milliseconds(timeout_ms), [this] { return is_finished || !queue.empty(); }))
            return false;

        if (is_finished)
            return false;

        ::detail::moveOrCopyIfThrow(std::move(queue.front()), response);
        queue.pop();

        return true;
    }

    size_t size() const
    {
        std::lock_guard lock(queue_mutex);
        return queue.size();
    }

    bool isFinished() const
    {
        std::lock_guard lock(queue_mutex);
        return is_finished;
    }

    bool finish()
    {
        std::lock_guard<std::mutex> lock(queue_mutex);
        bool was_finished_before = is_finished;
        is_finished = true;

        cv.notify_all();

        return was_finished_before;
    }
};

}
