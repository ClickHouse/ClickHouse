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
    extern const Metric MergeTreePartsLoaderThreads;
    extern const Metric MergeTreePartsLoaderThreadsActive;
    extern const Metric MergeTreeOutdatedPartsLoaderThreads;
    extern const Metric MergeTreeOutdatedPartsLoaderThreadsActive;
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

std::unique_ptr<ThreadPool> ActivePartsLoadingThreadPool::instance;

void ActivePartsLoadingThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The BackupsIO thread pool is initialized twice");
    }

    instance = std::make_unique<ThreadPool>(
        CurrentMetrics::MergeTreePartsLoaderThreads,
        CurrentMetrics::MergeTreePartsLoaderThreadsActive,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

ThreadPool & ActivePartsLoadingThreadPool::get()
{
    if (!instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The BackupsIO thread pool is not initialized");
    }

    return *instance;
}

std::unique_ptr<ThreadPool> OutdatedPartsLoadingThreadPool::instance;
size_t OutdatedPartsLoadingThreadPool::max_threads_turbo;

void OutdatedPartsLoadingThreadPool::initialize(size_t max_threads_, size_t max_threads_turbo_, size_t max_free_threads_, size_t queue_size_)
{
    if (instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is initialized twice");
    }

    max_threads_turbo = max_threads_turbo_;

    instance = std::make_unique<ThreadPool>(
        CurrentMetrics::MergeTreeOutdatedPartsLoaderThreads,
        CurrentMetrics::MergeTreeOutdatedPartsLoaderThreadsActive,
        max_threads_,
        max_free_threads_,
        queue_size_,
        /* shutdown_on_exception= */ false);
}

void OutdatedPartsLoadingThreadPool::turboMode()
{
    if (!instance)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The PartsLoadingThreadPool thread pool is not initialized");
    }

    instance->setMaxThreads(max_threads_turbo);
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
