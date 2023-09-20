#include "IStatisticsStorage.h"
#include <Interpreters/Context.h>
#include <Interpreters/executeQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Executors/PushingAsyncPipelineExecutor.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


std::string_view STATISTICS_DATABASE_NAME = "system";
std::string_view TABLE_STATS_TABLE_NAME = "table_statistics";
std::string_view COLUMN_STATS_TABLE_NAME = "column_statistics_basic";
std::string_view COLUMN_HISTOGRAM_STATS_TABLE_NAME = "column_statistics_histogram";

namespace
{
    ContextMutablePtr createQueryContext();

    /// load
    std::optional<TableStatistics> loadTableStats(const StorageID & storage_id, const String & cluster_name);
    std::shared_ptr<ColumnStatisticsMap> loadColumnStats(const StorageID & storage_id, const String & cluster_name);

    /// update
    void updateTableStats(const StorageID & storage_id);
    void updateColumnStats(const StorageID & storage_id);
}

/// TODO add strategies if one node in cluster has no statistics
StatisticsPtr StatisticsLoader::load(const StorageID & storage_id, const String & cluster_name)
{
    /// 1. load table row count
    auto row_count = loadTableStats(storage_id, cluster_name);

    if (!row_count.has_value())
        return nullptr;

    /// 2. load column statistics
    auto column_stats = loadColumnStats(storage_id, cluster_name);

    return std::make_shared<Statistics>(*row_count, *column_stats);
}

void StatisticsUpdater::update(const StorageID & storage_id)
{
    /// 1. update table row count
    updateTableStats(storage_id);

    /// 2. update column statistics
    updateColumnStats(storage_id);
}

StatisticsPtr IStatisticsStorage::get(const StorageID &, const String &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

void IStatisticsStorage::collect(const StorageID &)
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

void IStatisticsStorage::refreshAll()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

void IStatisticsStorage::shutdown()
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method not implemented.");
}

namespace
{
    ContextMutablePtr createQueryContext()
    {
        auto query_context = Context::createCopy(Context::getGlobalContextInstance());

        query_context->makeQueryContext();
        query_context->setCurrentQueryId(""); /// Not use user query id

        SettingsChanges setting_changes;
        setting_changes.emplace_back("allow_experimental_query_coordination", false);
        query_context->applySettingsChanges(setting_changes);

        return query_context;
    }

    std::optional<TableStatistics> loadTableStats(const StorageID & storage_id, const String & cluster_name)
    {
        String sql = fmt::format(
            "select sum(row_count) from cluster({}, {}, {}) where db={} and table={}",
            cluster_name,
            STATISTICS_DATABASE_NAME,
            TABLE_STATS_TABLE_NAME,
            storage_id.getDatabaseName(),
            storage_id.getTableName());

        auto load_query_context = createQueryContext();
        auto block_io = executeQuery(sql, load_query_context, true);

        auto executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io.pipeline);

        Block block;
        executor->pull(block);

        if (!block)
            return std::nullopt;

        return block.getByPosition(0).column->getUInt(0);
    }

    std::shared_ptr<ColumnStatisticsMap> loadColumnStats(const StorageID & storage_id, const String & cluster_name)
    {
        String sql = fmt::format(
            "select column, uniqMerge(ndv) as ndv, min(min_value) as min_value, max(max_value) as max_value, avg(avg_row_size) as "
            "avg_row_size from cluster({}, {}, {}) where db={} and table={}",
            cluster_name,
            STATISTICS_DATABASE_NAME,
            COLUMN_STATS_TABLE_NAME,
            storage_id.getDatabaseName(),
            storage_id.getTableName());

        auto load_query_context = createQueryContext();
        auto block_io = executeQuery(sql, load_query_context, true);

        auto executor = std::make_unique<PullingAsyncPipelineExecutor>(block_io.pipeline);

        Block block;
        std::shared_ptr<ColumnStatisticsMap> column_stats_map = std::make_shared<ColumnStatisticsMap>();

        while (!block && executor->pull(block))
        {
            for (size_t i=0;i<block.rows();i++)
            {
                auto column = block.getByPosition(0).column->getDataAt(i);
                auto column_stats = std::make_shared<ColumnStatistics>();
                
                column_stats->setNdv(block.getByPosition(1).column->getFloat64(i));
                column_stats->setMinValue(block.getByPosition(2).column->getFloat64(i));
                column_stats->setMaxValue(block.getByPosition(3).column->getFloat64(i));
                column_stats->setAvgRowSize(block.getByPosition(4).column->getFloat64(i));
                
                column_stats_map->insert({column.toString(), column_stats});
            }
        }

        return column_stats_map;
    }

    void updateTableStats(const StorageID & storage_id)
    {

        Poco::Timestamp timestamp;
        auto event_time = Poco::DateTimeFormatter::format(timestamp, "%Y-%m-%d %H:%M:%S"); // TODO refer to query_log

        String sql = fmt::format(
            "insert into {}.{} select '{}', '{}', '{}', count(*) from {}",
            STATISTICS_DATABASE_NAME,
            TABLE_STATS_TABLE_NAME,
            event_time,
            "table",
            "column",
            storage_id.getFullNameNotQuoted());

        auto load_query_context = createQueryContext();
        auto block_io = executeQuery(sql, load_query_context, true);

        auto executor = std::make_unique<PushingAsyncPipelineExecutor>(block_io.pipeline);

        Block block;
        executor->push(block);
    }

    void updateColumnStats(const StorageID & storage_id)
    {
        Poco::Timestamp timestamp;
        auto event_time = Poco::DateTimeFormatter::format(timestamp, "%Y-%m-%d %H:%M:%S"); // TODO refer to query_log

        String sql = fmt::format(
            "insert into {}.{} select '{}', '{}', '{}', count(*) from {}",
            STATISTICS_DATABASE_NAME,
            TABLE_STATS_TABLE_NAME,
            event_time,
            "table",
            "column",
            storage_id.getFullNameNotQuoted());

        auto load_query_context = createQueryContext();
        auto block_io = executeQuery(sql, load_query_context, true);

        auto executor = std::make_unique<PushingAsyncPipelineExecutor>(block_io.pipeline);

        Block block;
        executor->push(block);
    }

}

}
