#pragma once

#include <Interpreters/Cluster.h>
#include <Interpreters/StorageID.h>
#include <QueryCoordination/Optimizer/Statistics/ColumnStatistics.h>
#include <QueryCoordination/Optimizer/Statistics/Statistics.h>

namespace DB
{

using TableStatistics = Float64;

/// Load statistics from storage.
class StatisticsLoader
{
public:
    static StatisticsPtr load(const StorageID & table_id, const String & cluster_name);
};

/// Collect statistics and insert into storage.
class StatisticsCollector
{
public:
    static void collect(const StorageID & storage_id, const Names & columns, ContextMutablePtr context);
};

struct StatsTableDefinitionDesc
{
    virtual String getDataBaseName() = 0;
    virtual String getTableName() = 0;
    virtual String getDatabaseAndTable() = 0;
    virtual NamesAndTypesList getNamesAndTypesList() = 0;
    virtual String getStorage() = 0;

    virtual ~StatsTableDefinitionDesc() = default;
};

/// Description for table definition of table statistics.
struct StatsForTableDesc : public StatsTableDefinitionDesc
{
    String getDataBaseName() override;
    String getTableName() override;
    String getDatabaseAndTable() override;
    NamesAndTypesList getNamesAndTypesList() override;
    String getStorage() override;
};

/// Description for table definition of column statistics.
struct StatsForColumnDesc : public StatsTableDefinitionDesc
{
    String getDataBaseName() override;
    String getTableName() override;
    String getDatabaseAndTable() override;
    NamesAndTypesList getNamesAndTypesList() override;
    String getStorage() override;
};

/**Interface for statistics providing API for getting, updating statistics, details see 'CachedStatisticsStorage'.
 */
class IStatisticsStorage : public std::enable_shared_from_this<IStatisticsStorage>
{
public:
    using StatisticsMap = std::unordered_map<StorageID, StatisticsPtr, StorageID::DatabaseAndTableNameHash>;
    using TableAndClusters = std::unordered_map<StorageID, String, StorageID::DatabaseAndTableNameHash>;

    static constexpr auto STATISTICS_DATABASE_NAME = "system";
    static constexpr auto TABLE_STATS_TABLE_NAME = "statistics_table";
    static constexpr auto COLUMN_STATS_TABLE_NAME = "statistics_column_basic";
    static constexpr auto COLUMN_HISTOGRAM_STATS_TABLE_NAME = "statistics_column_histogram";

    /// Create tables if not exist.
    static void prepareTables(ContextPtr global_context);

    void initialize(ContextPtr) { }

    /// Get statistics for a table.
    /// 'storage_id' is local table and 'cluster_name' is used to query statistics for all nodes.
    virtual StatisticsPtr get(const StorageID & storage_id, const String & cluster_name);

    /// Collect statistics for a table.
    /// User can get the updated statistics once collecting is done.
    /// Note that the method only collect statistics for local node.
    virtual void collect(const StorageID & storage_id, const Names & columns, ContextMutablePtr context);

    virtual void loadAll();
    //    virtual void collectAll();

    virtual void shutdown();
    virtual ~IStatisticsStorage() { shutdown(); }
};

using IStatisticsStoragePtr = std::shared_ptr<IStatisticsStorage>;

}
