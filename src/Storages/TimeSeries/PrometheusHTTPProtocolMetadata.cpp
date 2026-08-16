#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/executeQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>

#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_FINAL;
}

namespace
{

ASTPtr makeMetadataColumnExpression(const String & column_name)
{
    return makeASTFunction(
        "ifNull",
        makeASTFunction("toString", make_intrusive<ASTIdentifier>(column_name)),
        make_intrusive<ASTLiteral>(String{}));
}

ASTSelectQuery & getSelectQuery(ASTPtr & query_ast)
{
    auto & select_with_union_query = query_ast->as<ASTSelectWithUnionQuery &>();
    return select_with_union_query.list_of_selects->children.front()->as<ASTSelectQuery &>();
}

void setFinal(ASTPtr & query_ast)
{
    auto & select_query = getSelectQuery(query_ast);
    auto & tables = select_query.refTables()->as<ASTTablesInSelectQuery &>();
    auto & table = tables.children.front()->as<ASTTablesInSelectQueryElement &>();
    table.table_expression->as<ASTTableExpression &>().final = true;
}

}

void PrometheusHTTPProtocolAPI::getMetadata(
    WriteBuffer & response,
    const String & metric_param,
    Int64 limit,
    Int64 limit_per_metric,
    QueryFinishCallback query_finish_callback)
{
    const auto metrics_table = time_series_storage->getTargetTable(ViewTarget::Metrics, getContext());
    const bool use_final = metrics_table->supportsFinal();
    const auto time_series_table_id = time_series_storage->getStorageID();

    checkTimeSeriesTableSelectAccess(getContext(), time_series_table_id);

    auto group_uniq_array = makeASTFunction(
        "groupUniqArray",
        makeASTFunction(
            "tuple",
            makeMetadataColumnExpression(TimeSeriesColumnNames::Type),
            makeMetadataColumnExpression(TimeSeriesColumnNames::Help),
            makeMetadataColumnExpression(TimeSeriesColumnNames::Unit)));
    if (limit_per_metric > 0)
    {
        /// Prometheus only limits the number of metadata entries returned for each family.
        /// A bounded aggregate keeps the query state proportional to that limit. The aggregate
        /// may choose any valid subset; arraySort only orders the entries that were selected.
        group_uniq_array = addParametersToAggregateFunction(
            std::move(group_uniq_array), make_intrusive<ASTLiteral>(limit_per_metric));
    }

    auto metric_family_expression = makeMetadataColumnExpression(TimeSeriesColumnNames::MetricFamilyName);
    metric_family_expression->setAlias("metric_family");
    auto metadata_expression = makeASTFunction("arraySort", std::move(group_uniq_array));
    metadata_expression->setAlias("metadata");

    PrometheusQueryToSQL::SelectQueryBuilder query_builder;
    query_builder.select_list = {std::move(metric_family_expression), std::move(metadata_expression)};
    query_builder.from_table_function = makeASTFunction(
        "timeSeriesMetrics",
        make_intrusive<ASTLiteral>(time_series_table_id.getDatabaseName()),
        make_intrusive<ASTLiteral>(time_series_table_id.getTableName()));

    if (!metric_param.empty())
    {
        /// Keep the filter on the raw key column so MergeTree can use its primary-key condition
        /// directly. The projection still normalizes external target column types below.
        query_builder.where = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            make_intrusive<ASTLiteral>(metric_param));
    }

    query_builder.group_by = {makeMetadataColumnExpression(TimeSeriesColumnNames::MetricFamilyName)};
    query_builder.order_by = {makeMetadataColumnExpression(TimeSeriesColumnNames::MetricFamilyName)};
    query_builder.order_direction = 1;
    if (limit >= 0)
        query_builder.limit = static_cast<size_t>(limit);

    auto query_ast = query_builder.getSelectQuery();
    auto query_without_final_ast = query_ast->clone();
    if (use_final)
        setFinal(query_ast);

    const String query = query_ast->formatWithSecretsOneLine();
    const String query_without_final = query_without_final_ast->formatWithSecretsOneLine();

    LOG_TRACE(log, "Prometheus metric metadata query: {}", query);

    bool response_started = false;
    auto execute_metadata_query = [&](const String & query_to_execute)
    {
        auto [ast, io] = executeQuery(query_to_execute, getContext(), {}, QueryProcessingStage::Complete);

        try
        {
            PullingAsyncPipelineExecutor executor(io.pipeline);
            Block block;

            auto pull_next_nonempty = [&]
            {
                while (executor.pull(block))
                {
                    if (block.rows() > 0)
                        return true;
                }
                return false;
            };

            /// Pull before writing the success envelope so an execution error can still be returned
            /// as a complete Prometheus error response by PrometheusRequestHandler::QueryImpl.
            bool has_output = pull_next_nonempty();
            response_started = true;
            writeString(R"({"status":"success","data":{)", response);

            bool first_metric = true;
            while (has_output)
            {
                const auto & metric_family_column = block.getByName("metric_family").column;
                const auto & metadata_array = typeid_cast<const ColumnArray &>(*block.getByName("metadata").column);
                const auto & offsets = metadata_array.getOffsets();
                const auto & tuple_column = typeid_cast<const ColumnTuple &>(metadata_array.getData());
                const auto & type_column = tuple_column.getColumn(0);
                const auto & help_column = tuple_column.getColumn(1);
                const auto & unit_column = tuple_column.getColumn(2);

                for (size_t i = 0; i < block.rows(); ++i)
                {
                    if (!first_metric)
                        writeString(",", response);
                    first_metric = false;

                    writeJSONString(metric_family_column->getDataAt(i), response, format_settings);
                    writeString(":[", response);

                    const size_t begin = (i == 0) ? 0 : offsets[i - 1];
                    const size_t end = offsets[i];
                    for (size_t j = begin; j < end; ++j)
                    {
                        if (j != begin)
                            writeString(",", response);

                        writeString(R"({"type":)", response);
                        writeJSONString(type_column.getDataAt(j), response, format_settings);
                        writeString(R"(,"help":)", response);
                        writeJSONString(help_column.getDataAt(j), response, format_settings);
                        writeString(R"(,"unit":)", response);
                        writeJSONString(unit_column.getDataAt(j), response, format_settings);
                        writeString("}", response);
                    }

                    writeString("]", response);
                }

                has_output = pull_next_nonempty();
            }

            writeString("}}", response);

            /// Store a pending query result cache write before the pulling executor is destroyed.
            io.pipeline.finalizeWriteInQueryResultCache();
        }
        catch (...)
        {
            io.onException();
            throw;
        }

        /// Release the query slot before a slow client finishes draining the response, then record QueryFinish.
        finishExecutedQuery(io, query_finish_callback);
    };

    try
    {
        execute_metadata_query(query);
    }
    catch (const Exception & e)
    {
        /// StorageDistributed advertises FINAL support at the initiator, but a remote target may reject it.
        /// Retry only before any response bytes are written, preserving FINAL for compatible targets.
        if (!use_final || response_started || !e.isRemoteException() || e.code() != ErrorCodes::ILLEGAL_FINAL)
            throw;

        LOG_TRACE(log, "Remote Metrics target rejected FINAL; retrying metadata query without FINAL");
        execute_metadata_query(query_without_final);
    }
}

}
