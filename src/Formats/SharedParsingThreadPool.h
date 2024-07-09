#pragma once

#include <mutex>
#include <Common/ThreadPool.h>

namespace DB
{

/// Multiple pipelines will share this thread pool,
/// and the total number of threads shall not exceed `max_threads`.
/// Initially, `max_threads` should be divided among the multiple pipelines, setting `max_parsing_threads` for each pipeline.
/// During execution, if a pipeline does not have `max_parsing_threads` tasks, it can invoke `release` to give up threads.
/// Other pipelines can then attempt to fetch more free threads from the pool if some free threads are available.
class SharedParsingThreadPool
{
public:
    explicit SharedParsingThreadPool(size_t max_threads_, size_t num_streams)
        : max_threads(max_threads_)
    {
        chassert(num_streams > 0);
        max_parsing_threads_per_stream = std::max<size_t>(max_threads / num_streams, 1UL);
        free_threads = 0;
        if (max_threads > max_parsing_threads_per_stream * num_streams)
            free_threads = max_threads - max_parsing_threads_per_stream * num_streams;
    }

    size_t getMaxParsingThreadsPerStream() const
    {
        return max_parsing_threads_per_stream;
    }

    size_t tryAcquire(size_t max_threads_to_acquire)
    {
        std::lock_guard lock(mutex);
        size_t available_threads = std::min(max_threads_to_acquire, free_threads);
        free_threads -= available_threads;
        return available_threads;
    }

    void release(size_t num_threads)
    {
        std::lock_guard lock(mutex);
        free_threads += num_threads;
        chassert(free_threads <= max_threads);
    }

    std::shared_ptr<ThreadPool> getOrSetPool(std::function<std::shared_ptr<ThreadPool>(size_t)> creator)
    {
        std::lock_guard lock(mutex);
        if (!pool)
            pool = creator(max_threads);
        return pool;
    }

private:
    mutable std::mutex mutex;
    std::shared_ptr<ThreadPool> pool;
    size_t max_threads = 0;
    size_t max_parsing_threads_per_stream = 1;
    size_t free_threads = 0;
};

using SharedParsingThreadPoolPtr = std::shared_ptr<SharedParsingThreadPool>;

}
