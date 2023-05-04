#include <IO/SharedThreadPools.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Core/Field.h>

namespace CurrentMetrics
{
    extern const Metric IOThreads;
    extern const Metric IOThreadsActive;
    extern const Metric BackupsIOThreads;
    extern const Metric BackupsIOThreadsActive;
    extern const Metric OutdatedPartsLoadingThreads;
    extern const Metric OutdatedPartsLoadingThreadsActive;
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

std::unique_ptr<ThreadPool> OutdatedPartsLoadingThreadPool::instance;

void OutdatedPartsLoadingThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is initialized twice");
    }

    instance = std::make_unique<ThreadPool>(
        CurrentMetrics::OutdatedPartsLoadingThreads,
        CurrentMetrics::OutdatedPartsLoadingThreadsActive,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

ThreadPool & OutdatedPartsLoadingThreadPool::get()
{
    if (!instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is not initialized");
    }

    return *instance;
}

}
