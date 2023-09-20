#pragma once

#include <QueryCoordination/Optimizer/Statistics/AsyncLoadingCache.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/IStatisticsStorage.h>


namespace DB
{

using TableStatistics = Float64;

class CachedStatisticsStorage : public IStatisticsStorage
{
public:
    explicit CachedStatisticsStorage(UInt64 refresh_interval_sec_, const String & load_thread_name_ = "bg_stats_update");

    StatisticsPtr get(const StorageID & table, const String & cluster_name) override;
    void collect(const StorageID & storage_id) override;

    void refreshAll() override;
    void shutdown() override;

    ~CachedStatisticsStorage() override;
private:
    void loadTask();

    /// Used to load statistics in background
    TableAndClusters table_and_clusters;
    /// Local cache
    StatisticsMap cache;

    StatisticsLoader loader;

    /// Background loading thread name
    String load_thread_name;

    ThreadFromGlobalPool load_thread;
    UInt64 refresh_interval_sec;

    mutable std::mutex mutex;
    std::atomic<bool> shutdown_called;

    Poco::Logger * log;
};

}
