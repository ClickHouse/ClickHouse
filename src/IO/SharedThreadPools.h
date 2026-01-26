#pragma once

#include <base/types.h>
#include <Common/ThreadPool_fwd.h>
#include <Common/CurrentMetrics.h>

#include <cstdlib>
#include <memory>
#include <mutex>


namespace DB
{

class StaticThreadPool
{
public:
    StaticThreadPool(
        const String & name_,
        CurrentMetrics::Metric threads_metric_,
        CurrentMetrics::Metric threads_active_metric_,
        CurrentMetrics::Metric threads_scheduled_metric_);

    ThreadPool & get();

    void initialize(size_t max_threads, size_t max_free_threads, size_t queue_size);
    bool isInitialized() const;
    void reloadConfiguration(size_t max_threads, size_t max_free_threads, size_t queue_size);

    /// At runtime we can increase the number of threads up the specified limit
    /// This is needed to utilize as much a possible resources to accomplish some task.
    void setMaxTurboThreads(size_t max_threads_turbo_);
    void enableTurboMode();
    void disableTurboMode();

private:
    const String name;
    const CurrentMetrics::Metric threads_metric;
    const CurrentMetrics::Metric threads_active_metric;
    const CurrentMetrics::Metric threads_scheduled_metric;

    std::unique_ptr<ThreadPool> instance;
    std::mutex mutex;
    size_t max_threads_turbo = 0;
    size_t max_threads_normal = 0;
    /// If this counter is > 0 - this specific mode is enabled
    size_t turbo_mode_enabled = 0;
};

/// ThreadPool used for the IO.
StaticThreadPool & getIOThreadPool();

/// ThreadPool used for the Backup IO.
StaticThreadPool & getBackupsIOThreadPool();

/// ThreadPool used for FETCH PARTITION
StaticThreadPool & getFetchPartitionThreadPool();

/// ThreadPool used for the loading of Outdated data parts for MergeTree tables.
StaticThreadPool & getActivePartsLoadingThreadPool();

/// ThreadPool used for deleting data parts for MergeTree tables.
StaticThreadPool & getPartsCleaningThreadPool();

/// This ThreadPool is used for the loading of Outdated data parts for MergeTree tables.
/// Normally we will just load Outdated data parts concurrently in background, but in
/// case when we need to synchronously wait for the loading to be finished, we can increase
/// the number of threads by calling enableTurboMode() :-)
StaticThreadPool & getOutdatedPartsLoadingThreadPool();

StaticThreadPool & getUnexpectedPartsLoadingThreadPool();

/// ThreadPool used for creating tables in DatabaseReplicated.
StaticThreadPool & getDatabaseReplicatedCreateTablesThreadPool();

/// ThreadPool used for dropping tables.
StaticThreadPool & getDatabaseCatalogDropTablesThreadPool();

/// ThreadPool used for parallel prefixes deserialization of subcolumns in Wide MergeTree parts.
StaticThreadPool & getMergeTreePrefixesDeserializationThreadPool();

}
