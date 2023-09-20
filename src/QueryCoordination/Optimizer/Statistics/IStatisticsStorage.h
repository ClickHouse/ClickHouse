#pragma once

#include <Interpreters/StorageID.h>
#include <Interpreters/Cluster.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>

namespace DB
{

using TableStatistics = Float64;

struct ColumnID
{
    StorageID table_id;
    String column;
};

/// Load statistics from storage.
class StatisticsLoader
{
public:
    static StatisticsPtr load(const StorageID & table_id, const String & cluster_name);
};

/// Update statistics into storage.
class StatisticsUpdater
{
public:
    static void update(const StorageID & table_id);
};

/**Interface for statistics providing API for getting, updating statistics, details see 'CachedStatisticsStorage'.
 */
class IStatisticsStorage : public std::enable_shared_from_this<IStatisticsStorage>
{
public:

    using StatisticsMap = std::unordered_map<StorageID, StatisticsPtr, StorageID::DatabaseAndTableNameHash>;
    using TableAndClusters = std::unordered_map<StorageID, String, StorageID::DatabaseAndTableNameHash>;

    constexpr static std::string_view STATISTICS_DATABASE_NAME;
    constexpr static std::string_view TABLE_STATS_TABLE_NAME;
    constexpr static std::string_view COLUMN_STATS_TABLE_NAME;
    constexpr static std::string_view COLUMN_HISTOGRAM_STATS_TABLE_NAME;

    /// Get statistics for a table.
    /// 'cluster_name' is used to query statistics for all nodes.
    virtual StatisticsPtr get(const StorageID & table, const String & cluster_name);

    /// Collect statistics for a table.
    /// User can get the updated statistics once collecting is done.
    /// Note that the method only collect statistics for local node.
    virtual void collect(const StorageID & table);

    virtual void refreshAll();
//    virtual void collectAll();

    virtual void shutdown();

    virtual ~IStatisticsStorage() { shutdown(); }
};

using IStatisticsStoragePtr = std::shared_ptr<IStatisticsStorage>;

}
