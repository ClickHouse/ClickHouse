#include <Storages/TimeSeries/PrometheusHTTPProtocolAPI.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Common/logger_useful.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/ThreadStatus.h>
#include <Core/Field.h>
#include <IO/WriteHelpers.h>
#include <Interpreters/Context.h>
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

String makeFinalProbeQuery(const StorageID & time_series_table_id, const String & metric_param)
{
    PrometheusQueryToSQL::SelectQueryBuilder query_builder;
    query_builder.select_list = {make_intrusive<ASTLiteral>(UInt64{1})};
    query_builder.from_table_function = makeASTFunction(
        "timeSeriesMetrics",
        make_intrusive<ASTLiteral>(time_series_table_id.getDatabaseName()),
        make_intrusive<ASTLiteral>(time_series_table_id.getTableName()));

    if (!metric_param.empty())
    {
        query_builder.where = makeASTFunction(
            "equals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            make_intrusive<ASTLiteral>(metric_param));
    }

    query_builder.limit = 1;

    auto query_ast = query_builder.getSelectQuery();
    setFinal(query_ast);
    return query_ast->formatWithSecretsOneLine();
}

void executeFinalProbe(const String & query, const ContextPtr & context)
{
    /// The probe reads the physical Metrics target directly. Use the same target context as the
    /// generated metadata query so a logical-table grant is sufficient for Distributed targets.
    auto query_context = getTimeSeriesTargetContext(context);
    query_context->setCurrentQueryId({});
    query_context->setSetting("log_queries", Field{false});
    /// The capability probe is an implementation detail and must not recursively trigger AST fuzzing
    /// inherited from the caller's settings.
    query_context->setSetting("ast_fuzzer_runs", Field(Float64(0)));

    /// The probe is a separate query, so run it in the copied context's thread group while preserving
    /// the surrounding HTTP request's group. This keeps its resource accounting isolated from the
    /// request's query context.
    ThreadGroupSwitcher thread_group_switcher(
        ThreadGroup::createForQuery(query_context), ThreadName::PROMETHEUS_HANDLER, /*allow_existing_group=*/ true);

    auto [ast, io] = executeQuery(
        query,
        query_context,
        QueryFlags{.internal = true, .ignore_quota = true},
        QueryProcessingStage::Complete);

    try
    {
        PullingAsyncPipelineExecutor executor(io.pipeline);
        Block block;
        while (executor.pull(block))
        {
        }

        io.pipeline.finalizeWriteInQueryResultCache();
    }
    catch (...)
    {
        io.onException();
        throw;
    }

    finishExecutedQuery(io, {});
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
    const bool remote_metrics_target = metrics_table->isRemote();
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

    bool execute_with_final = use_final;
    if (use_final && remote_metrics_target)
    {
        /// A zero-limit response does not read any rows, so it has no reason to probe or execute FINAL.
        if (limit == 0)
        {
            execute_with_final = false;
        }
        else if (const auto cached_support = time_series_storage->getCachedMetricsTargetFinalSupport())
        {
            execute_with_final = *cached_support;
        }
        else
        {
            /// StorageDistributed advertises FINAL support before the remote target is known to support it.
            /// Probe with a bounded, metric-aware internal query in a copied target context. The
            /// logical-table access check above remains in the caller context, while the physical
            /// target stays an implementation detail. The probe also avoids creating its own
            /// query-log or query-metric-log record or a top-level query span. Cache the capability
            /// briefly because Prometheus polls this endpoint frequently.
            try
            {
                executeFinalProbe(makeFinalProbeQuery(time_series_table_id, metric_param), getContext());
                time_series_storage->setCachedMetricsTargetFinalSupport(true);
            }
            catch (const Exception & e)
            {
                if (e.code() != ErrorCodes::ILLEGAL_FINAL)
                    throw;

                time_series_storage->setCachedMetricsTargetFinalSupport(false);
                execute_with_final = false;
                LOG_TRACE(log, "Remote Metrics target rejected FINAL; executing metadata query without FINAL");
            }
        }
    }

    bool response_started = false;
    auto execute_metadata_query = [&](const String & query_to_execute)
    {
        auto [ast, io] = executeQuery(
            query_to_execute,
            getContext(),
            QueryFlags{},
            QueryProcessingStage::Complete);

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
        execute_metadata_query(execute_with_final ? query : query_without_final);
    }
    catch (const Exception & e)
    {
        /// A cached capability can become stale if the remote target changes. Retry only before any
        /// response bytes are written, and invalidate the cache for the next request.
        if (!remote_metrics_target || !execute_with_final || response_started || !e.isRemoteException() || e.code() != ErrorCodes::ILLEGAL_FINAL)
            throw;

        time_series_storage->setCachedMetricsTargetFinalSupport(false);
        LOG_TRACE(log, "Remote Metrics target rejected cached FINAL capability; retrying metadata query without FINAL");
        execute_metadata_query(query_without_final);
    }
}

}
