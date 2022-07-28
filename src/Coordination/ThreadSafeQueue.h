#pragma once

#include <queue>
#include <mutex>

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
public:

    void push(const T & response)
    {
        std::lock_guard lock(queue_mutex);
        queue.push(response);
        cv.notify_one();
    }

    bool tryPop(T & response, int64_t timeout_ms = 0)
    {
        std::unique_lock lock(queue_mutex);
        if (!cv.wait_for(lock,
                std::chrono::milliseconds(timeout_ms), [this] { return !queue.empty(); }))
            return false;

        response = queue.front();
        queue.pop();
        return true;
    }

    size_t size() const
    {
        std::lock_guard lock(queue_mutex);
        return queue.size();
    }
};

}
