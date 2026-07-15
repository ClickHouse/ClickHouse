#include <IO/SharedThreadPools.h>
#include <Common/CurrentMetrics.h>
#include <Common/ThreadPool.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Core/Field.h>

namespace CurrentMetrics
{
    extern const Metric IOThreads;
    extern const Metric IOThreadsActive;
    extern const Metric IOThreadsScheduled;
    extern const Metric BackupsIOThreads;
    extern const Metric BackupsIOThreadsActive;
    extern const Metric BackupsIOThreadsScheduled;
    extern const Metric MergeTreeFetchPartitionThreads;
    extern const Metric MergeTreeFetchPartitionThreadsActive;
    extern const Metric MergeTreeFetchPartitionThreadsScheduled;
    extern const Metric MergeTreePartsLoaderThreads;
    extern const Metric MergeTreePartsLoaderThreadsActive;
    extern const Metric MergeTreePartsLoaderThreadsScheduled;
    extern const Metric MergeTreePartsCleanerThreads;
    extern const Metric MergeTreePartsCleanerThreadsActive;
    extern const Metric MergeTreePartsCleanerThreadsScheduled;
    extern const Metric MergeTreeOutdatedPartsLoaderThreads;
    extern const Metric MergeTreeOutdatedPartsLoaderThreadsActive;
    extern const Metric MergeTreeOutdatedPartsLoaderThreadsScheduled;
    extern const Metric MergeTreeUnexpectedPartsLoaderThreads;
    extern const Metric MergeTreeUnexpectedPartsLoaderThreadsActive;
    extern const Metric MergeTreeUnexpectedPartsLoaderThreadsScheduled;
    extern const Metric DatabaseCatalogThreads;
    extern const Metric DatabaseCatalogThreadsActive;
    extern const Metric DatabaseCatalogThreadsScheduled;
    extern const Metric DatabaseReplicatedCreateTablesThreads;
    extern const Metric DatabaseReplicatedCreateTablesThreadsActive;
    extern const Metric DatabaseReplicatedCreateTablesThreadsScheduled;
    extern const Metric MergeTreeSubcolumnsReaderThreads;
    extern const Metric MergeTreeSubcolumnsReaderThreadsActive;
    extern const Metric MergeTreeSubcolumnsReaderThreadsScheduled;
    extern const Metric FormatParsingThreads;
    extern const Metric FormatParsingThreadsActive;
    extern const Metric FormatParsingThreadsScheduled;
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
    CurrentMetrics::Metric threads_active_metric_,
    CurrentMetrics::Metric threads_scheduled_metric_)
    : name(name_)
    , threads_metric(threads_metric_)
    , threads_active_metric(threads_active_metric_)
    , threads_scheduled_metric(threads_scheduled_metric_)
{
}

void StaticThreadPool::initialize(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is initialized twice", name);

    std::call_once(init_flag, [&]
        {
            initializeImpl(max_threads, max_free_threads, queue_size);
        });
}

void StaticThreadPool::initializeWithDefaultSettingsIfNotInitialized()
{
    std::call_once(init_flag, [&]
        {
            size_t max_threads = getNumberOfCPUCoresToUse();
            initializeImpl(max_threads, /*max_free_threads*/ 0, /*queue_size*/ 10000);
        });
}

void StaticThreadPool::initializeImpl(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    /// By default enabling "turbo mode" won't affect the number of threads anyhow
    max_threads_turbo = max_threads;
    max_threads_normal = max_threads;
    instance = std::make_unique<ThreadPool>(
        threads_metric,
        threads_active_metric,
        threads_scheduled_metric,
        max_threads,
        max_free_threads,
        queue_size,
        /* shutdown_on_exception= */ false);
}

bool StaticThreadPool::isInitialized() const
{
    return instance.operator bool();
}

void StaticThreadPool::reloadConfiguration(size_t max_threads, size_t max_free_threads, size_t queue_size)
{
    if (!instance)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The {} is not initialized", name);

    std::lock_guard lock(mutex);

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
    static StaticThreadPool instance("IOThreadPool", CurrentMetrics::IOThreads, CurrentMetrics::IOThreadsActive, CurrentMetrics::IOThreadsScheduled);
    return instance;
}

StaticThreadPool & getBackupsIOThreadPool()
{
    static StaticThreadPool instance("BackupsIOThreadPool", CurrentMetrics::BackupsIOThreads, CurrentMetrics::BackupsIOThreadsActive, CurrentMetrics::BackupsIOThreadsScheduled);
    return instance;
}

StaticThreadPool & getFetchPartitionThreadPool()
{
    static StaticThreadPool instance("MergeTreeFetchPartitionThreadPool", CurrentMetrics::MergeTreeFetchPartitionThreads, CurrentMetrics::MergeTreeFetchPartitionThreadsActive, CurrentMetrics::MergeTreeFetchPartitionThreadsScheduled);
    return instance;
}

StaticThreadPool & getActivePartsLoadingThreadPool()
{
    static StaticThreadPool instance("MergeTreePartsLoaderThreadPool", CurrentMetrics::MergeTreePartsLoaderThreads, CurrentMetrics::MergeTreePartsLoaderThreadsActive, CurrentMetrics::MergeTreePartsLoaderThreadsScheduled);
    return instance;
}

StaticThreadPool & getPartsCleaningThreadPool()
{
    static StaticThreadPool instance("MergeTreePartsCleanerThreadPool", CurrentMetrics::MergeTreePartsCleanerThreads, CurrentMetrics::MergeTreePartsCleanerThreadsActive, CurrentMetrics::MergeTreePartsCleanerThreadsScheduled);
    return instance;
}

StaticThreadPool & getOutdatedPartsLoadingThreadPool()
{
    static StaticThreadPool instance("MergeTreeOutdatedPartsLoaderThreadPool", CurrentMetrics::MergeTreeOutdatedPartsLoaderThreads, CurrentMetrics::MergeTreeOutdatedPartsLoaderThreadsActive, CurrentMetrics::MergeTreeOutdatedPartsLoaderThreadsScheduled);
    return instance;
}

StaticThreadPool & getUnexpectedPartsLoadingThreadPool()
{
    static StaticThreadPool instance("MergeTreeUnexpectedPartsLoaderThreadPool", CurrentMetrics::MergeTreeUnexpectedPartsLoaderThreads, CurrentMetrics::MergeTreeUnexpectedPartsLoaderThreadsActive, CurrentMetrics::MergeTreeUnexpectedPartsLoaderThreadsScheduled);
    return instance;
}

StaticThreadPool & getDatabaseReplicatedCreateTablesThreadPool()
{
    static StaticThreadPool instance("CreateTablesThreadPool", CurrentMetrics::DatabaseReplicatedCreateTablesThreads, CurrentMetrics::DatabaseReplicatedCreateTablesThreadsActive, CurrentMetrics::DatabaseReplicatedCreateTablesThreadsScheduled);
    return instance;
}

/// ThreadPool used for dropping tables.
StaticThreadPool & getDatabaseCatalogDropTablesThreadPool()
{
    static StaticThreadPool instance("DropTablesThreadPool", CurrentMetrics::DatabaseCatalogThreads, CurrentMetrics::DatabaseCatalogThreadsActive, CurrentMetrics::DatabaseCatalogThreadsScheduled);
    return instance;
}

StaticThreadPool & getMergeTreePrefixesDeserializationThreadPool()
{
    static StaticThreadPool instance("MergeTreePrefixesDeserializationThreadPool", CurrentMetrics::MergeTreeSubcolumnsReaderThreads, CurrentMetrics::MergeTreeSubcolumnsReaderThreadsActive, CurrentMetrics::MergeTreeSubcolumnsReaderThreadsScheduled);
    return instance;
}

StaticThreadPool & getFormatParsingThreadPool()
{
    static StaticThreadPool instance("FormatParsingThreadPool", CurrentMetrics::FormatParsingThreads, CurrentMetrics::FormatParsingThreadsActive, CurrentMetrics::FormatParsingThreadsScheduled);
    return instance;
}

}
