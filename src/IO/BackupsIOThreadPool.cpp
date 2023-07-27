#include <IO/BackupsIOThreadPool.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Core/Field.h>

namespace CurrentMetrics
{
    extern const Metric BackupsIOThreads;
    extern const Metric BackupsIOThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

std::unique_ptr<ThreadPool> BackupsIOThreadPool::instance;

void BackupsIOThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The BackupsIO thread pool is initialized twice");
    }

    instance = std::make_unique<ThreadPool>(
        CurrentMetrics::BackupsIOThreads,
        CurrentMetrics::BackupsIOThreadsActive,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

ThreadPool & BackupsIOThreadPool::get()
{
    if (!instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The BackupsIO thread pool is not initialized");
    }

    return *instance;
}

}
