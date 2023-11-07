#pragma once

#include <QueryCoordination/Optimizer/Statistics/AsyncLoadingCache.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/IStatisticsStorage.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>


namespace DB
{

using TableStatistics = Float64;

class CachedStatisticsStorage : public IStatisticsStorage
{
public:
    explicit CachedStatisticsStorage(UInt64 refresh_interval_sec_, const String & load_thread_name_ = "StatisticsStrge");

    StatisticsPtr get(const StorageID & table, const String & cluster_name) override;
    void collect(const StorageID & storage_id, const Names & columns, ContextMutablePtr context) override;

    void loadAll() override;
    void shutdown() override;

    ~CachedStatisticsStorage() override;

private:
    void loadTask();

    /// Used to load statistics in background
    TableAndClusters table_and_clusters;
    /// Local cache
    StatisticsMap cache;

    StatisticsLoader loader;
    StatisticsCollector collector;

    /// Background loading thread name
    String load_thread_name;

    ThreadFromGlobalPool load_thread;
    UInt64 refresh_interval_sec;

    mutable std::mutex mutex;
    std::atomic<bool> shutdown_called;

    Poco::Logger * log;
};

}
