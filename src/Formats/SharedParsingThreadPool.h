#pragma once

#include <mutex>
#include "Common/CurrentMetrics.h"
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
    SharedParsingThreadPool(size_t max_threads_, size_t num_streams_)
        : max_threads(max_threads_), num_streams(num_streams_)
    {
        threads_per_stream = std::max(1ul, max_threads / std::max(num_streams, 1ul));
        max_threads = std::max(max_threads, threads_per_stream * num_streams);
        free_threads = max_threads - threads_per_stream * num_streams;
    }

    size_t getThreadsPerStream() const
    {
        return threads_per_stream;
    }

    void finishStream()
    {
        std::lock_guard lock(mutex);
        chassert(num_streams > 0);
        chassert(free_threads + threads_per_stream <= max_threads);
        num_streams--;
        free_threads += threads_per_stream;
    }

    void releaseThreads(size_t threads_to_release)
    {
        std::lock_guard guard(mutex);
        chassert(free_threads + threads_to_release <= max_threads);
        free_threads += threads_to_release;
    }

    size_t tryAcquireThreads(size_t max_threads_to_acquire)
    {
        std::lock_guard lock(mutex);
        size_t acquired_threads = std::min(free_threads, max_threads_to_acquire);
        free_threads -= acquired_threads;
        return acquired_threads;
    }

    std::shared_ptr<ThreadPool> getOrSetPool(
        CurrentMetrics::Metric metric_threads_,
        CurrentMetrics::Metric metric_active_threads_,
        CurrentMetrics::Metric metric_scheduled_jobs_)
    {
        std::lock_guard lock(mutex);
        if (!pool)
            pool = std::make_shared<ThreadPool>(
                metric_threads_,
                metric_active_threads_,
                metric_scheduled_jobs_,
                max_threads,
                max_threads,
                max_threads + 1); /// avoid deadlock
        return pool;
    }

private:
    mutable std::mutex mutex;

    size_t max_threads;
    size_t threads_per_stream;

    size_t free_threads;
    size_t num_streams;

    std::shared_ptr<ThreadPool> pool;
};

using SharedParsingThreadPoolPtr = std::shared_ptr<SharedParsingThreadPool>;

}
