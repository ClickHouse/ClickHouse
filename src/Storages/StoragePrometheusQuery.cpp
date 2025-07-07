#include <Storages/StoragePrometheusQuery.h>

#include <Common/logger_useful.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

StoragePrometheusQuery::StoragePrometheusQuery(
    const StorageID & table_id_,
    const ColumnsDescription & columns_,
    const StorageID & time_series_storage_id_,
    const PrometheusQueryTree & promql_query_,
    const Field & evaluation_time_)
    : IStorage{table_id_}
    , time_series_storage_id{time_series_storage_id_}
    , promql_query{promql_query_}
    , evaluation_time{evaluation_time_}
    , log(getLogger("StoragePrometheusQuery"))
{
    StorageInMemoryMetadata storage_metadata;
    storage_metadata.setColumns(columns_);
    setInMemoryMetadata(storage_metadata);
}

void StoragePrometheusQuery::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & /* storage_snapshot */,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /* processed_stage */,
    size_t /* max_block_size */,
    size_t /* num_streams */)
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto data_table = time_series_storage->getTargetTable(ViewTarget::Data, context);
    auto data_table_metadata = data_table->getInMemoryMetadataPtr();
    PrometheusQueryToSQLConverter::TimeSeriesTableInfo time_series_table_info;
    time_series_table_info.storage_id = time_series_storage_id;
    time_series_table_info.timestamp_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp).type;
    time_series_table_info.value_data_type = data_table_metadata->columns.get(TimeSeriesColumnNames::Value).type;

    LOG_INFO(log, "Building SQL to evaluate promql: {}", promql_query);
    ASTPtr select_query = PrometheusQueryToSQLConverter{promql_query, evaluation_time, time_series_table_info, 5*60, 15}.getSQL();

    LOG_INFO(log, "Will execute query:\n{}", select_query->formatForLogging());

    auto options = SelectQueryOptions(QueryProcessingStage::Complete, 0, false, query_info.settings_limit_offset_done);
    InterpreterSelectQueryAnalyzer interpreter(select_query, context, options, column_names);
    interpreter.addStorageLimits(*query_info.storage_limits);
    query_plan = std::move(interpreter).extractQueryPlan();
}

}
