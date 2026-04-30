#pragma once

#include <Common/ThreadPool.h>

#include <future>
#include <functional>
#include <memory>

namespace DB
{

class Rope;

/// Shared bounded thread pool for prefetch tasks.
/// Replaces AsynchronousBoundedReadBuffer's async reader.
class PrefetchThreadPool
{
public:
    explicit PrefetchThreadPool(size_t pool_size)
        : pool(
            CurrentMetrics::end(),
            CurrentMetrics::end(),
            CurrentMetrics::end(),
            pool_size,
            pool_size,
            pool_size)
    {
    }

    std::future<Rope> submit(std::function<Rope()> task);

private:
    ThreadPool pool;
};

}
