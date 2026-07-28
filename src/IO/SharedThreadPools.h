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

    void shutdown();
    static void shutdownAll();

    /// Server and clickhouse-local initialize all thread pools on startup, with settings from config.
    /// Client and misc tools may initialize the pools they use lazily using this method instead.
    void initializeWithDefaultSettingsIfNotInitialized();

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
    std::once_flag init_flag;
    std::mutex mutex;
    size_t max_threads_turbo = 0;
    size_t max_threads_normal = 0;
    /// If this counter is > 0 - this specific mode is enabled
    size_t turbo_mode_enabled = 0;

    void initializeImpl(size_t max_threads, size_t max_free_threads, size_t queue_size);
};

#define APPLY_FOR_STATIC_THREAD_POOLS(M) \
    M(IO, "IOThreadPool", IO) \
    M(BackupsIO, "BackupsIOThreadPool", BackupsIO) \
    M(FetchPartition, "MergeTreeFetchPartitionThreadPool", MergeTreeFetchPartition) \
    M(ActivePartsLoading, "MergeTreePartsLoaderThreadPool", MergeTreePartsLoader) \
    M(SnapshotCommit, "MergeTreeSnapshotCommitThreadPool", MergeTreeSnapshotCommit) \
    M(PartsCleaning, "MergeTreePartsCleanerThreadPool", MergeTreePartsCleaner) \
    M(OutdatedPartsLoading, "MergeTreeOutdatedPartsLoaderThreadPool", MergeTreeOutdatedPartsLoader) \
    M(UnexpectedPartsLoading, "MergeTreeUnexpectedPartsLoaderThreadPool", MergeTreeUnexpectedPartsLoader) \
    M(DatabaseReplicatedCreateTables, "CreateTablesThreadPool", DatabaseReplicatedCreateTables) \
    M(DatabaseCatalogDropTables, "DropTablesThreadPool", DatabaseCatalog) \
    M(MergeTreePrefixesDeserialization, "MergeTreePrefixesDeserializationThreadPool", MergeTreeSubcolumnsReader) \
    M(FormatParsing, "FormatParsingThreadPool", FormatParsing)

#define DECLARE_STATIC_THREAD_POOL_GETTER(SUFFIX, NAME, METRIC) StaticThreadPool & get##SUFFIX##ThreadPool();
APPLY_FOR_STATIC_THREAD_POOLS(DECLARE_STATIC_THREAD_POOL_GETTER)
#undef DECLARE_STATIC_THREAD_POOL_GETTER

}
