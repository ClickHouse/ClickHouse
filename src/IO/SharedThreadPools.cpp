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
    extern const Metric MergeTreePartsCleanerThreads;
    extern const Metric MergeTreePartsCleanerThreadsActive;
    extern const Metric MergeTreeOutdatedPartsLoaderThreads;
    extern const Metric MergeTreeOutdatedPartsLoaderThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


StaticThreadPool::StaticThreadPool(
    const String & name_,
    CurrentMetrics::Metric threads_metric_,
    CurrentMetrics::Metric threads_active_metric_)
    : name(name_)
    , threads_metric(threads_metric_)
    , threads_active_metric(threads_active_metric_)
{
}

void StaticThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is initialized twice", name);

    /// By default enabling "turbo mode" won't affect the number of threads anyhow
    max_threads_turbo = max_threads;
    max_threads_normal = max_threads;
    instance = std::make_unique<ThreadPool>(
        threads_metric,
        threads_active_metric,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

void StaticThreadPool::reloadConfiguration(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    instance->setMaxThreads(turbo_mode_enabled > 0 ? max_threads_turbo : max_threads);
    instance->setMaxFreeThreads(max_free_threads);
    instance->setQueueSize(queue_size);
}


ThreadPool & StaticThreadPool::get()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    return *instance;
}

void StaticThreadPool::enableTurboMode()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    ++turbo_mode_enabled;
    if (turbo_mode_enabled == 1)
        instance->setMaxThreads(max_threads_turbo);
}

void StaticThreadPool::disableTurboMode()
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    --turbo_mode_enabled;
    if (turbo_mode_enabled == 0)
        instance->setMaxThreads(max_threads_normal);
}

void StaticThreadPool::setMaxTurboThreads(size_t max_threads_turbo_)
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

    max_threads_turbo = max_threads_turbo_;
    if (turbo_mode_enabled > 0)
        instance->setMaxThreads(max_threads_turbo);
}

StaticThreadPool & getIOThreadPool()
{
    static StaticThreadPool instance("IOThreadPool", CurrentMetrics::IOThreads, CurrentMetrics::IOThreadsActive);
    return instance;
}

StaticThreadPool & getBackupsIOThreadPool()
{
    static StaticThreadPool instance("BackupsIOThreadPool", CurrentMetrics::BackupsIOThreads, CurrentMetrics::BackupsIOThreadsActive);
    return instance;
}

StaticThreadPool & getActivePartsLoadingThreadPool()
{
    static StaticThreadPool instance("MergeTreePartsLoaderThreadPool", CurrentMetrics::MergeTreePartsLoaderThreads, CurrentMetrics::MergeTreePartsLoaderThreadsActive);
    return instance;
}

StaticThreadPool & getPartsCleaningThreadPool()
{
    static StaticThreadPool instance("MergeTreePartsCleanerThreadPool", CurrentMetrics::MergeTreePartsCleanerThreads, CurrentMetrics::MergeTreePartsCleanerThreadsActive);
    return instance;
}

StaticThreadPool & getOutdatedPartsLoadingThreadPool()
{
    static StaticThreadPool instance("MergeTreeOutdatedPartsLoaderThreadPool", CurrentMetrics::MergeTreeOutdatedPartsLoaderThreads, CurrentMetrics::MergeTreeOutdatedPartsLoaderThreadsActive);
    return instance;
}

}
