#include <IO/IOThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Core/Field.h>

namespace CurrentMetrics
{
    extern const Metric IOThreads;
    extern const Metric IOThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::unique_ptr<ThreadPool> IOThreadPool::instance;

void IOThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The IO thread pool is initialized twice");
    }

    instance = std::make_unique<ThreadPool>(
        CurrentMetrics::IOThreads,
        CurrentMetrics::IOThreadsActive,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

ThreadPool & IOThreadPool::get()
{
    if (!instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The IO thread pool is not initialized");
    }

    return *instance;
}

}
