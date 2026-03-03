#include <Common/DateLUTImpl.h>
#include <Common/Logger.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/formatReadable.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Common/thread_local_rng.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/FailPoint.h>
#include <Common/FieldVisitorToString.h>
#include <Common/SignalHandlers.h>

#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/copyData.h>

#include <QueryPipeline/BlockIO.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Formats/Impl/NullFormat.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryNormalization.h>
#include <Parsers/toOneLineQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Parsers/Kusto/parseKQLQuery.h>
#include <Parsers/Prometheus/ParserPrometheusQuery.h>

#include <Formats/FormatFactory.h>
#include <Storages/StorageInput.h>

#include <Access/ContextAccess.h>
#include <Access/EnabledQuota.h>
#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterTransactionControlQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/QueryMetricLog.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/ProfileEvents.h>
#include <Parsers/ASTSystemQuery.h>
#include <QueryPipeline/printPipeline.h>
#include <IO/Progress.h>
#include <Parsers/ASTIdentifier_fwd.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>

#include <IO/CompressionMethod.h>

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/RuntimeFilterLookup.h>
#include <QueryPipeline/QueryPipelineBuilder.h>

#include <Common/QueryFuzzer.h>
#include <Common/randomSeed.h>

#include <Poco/Net/SocketAddress.h>

#include <exception>
#include <memory>
#include <mutex>
#include <random>

#include <boost/algorithm/string/predicate.hpp>

namespace ProfileEvents
{
    extern const Event Query;
    extern const Event FailedQuery;
    extern const Event FailedInsertQuery;
    extern const Event FailedSelectQuery;
    extern const Event FailedInternalQuery;
    extern const Event FailedInternalInsertQuery;
    extern const Event FailedInternalSelectQuery;
    extern const Event FailedInitialQuery;
    extern const Event FailedInitialSelectQuery;
    extern const Event QueryTimeMicroseconds;
    extern const Event SelectQueryTimeMicroseconds;
    extern const Event InsertQueryTimeMicroseconds;
    extern const Event OtherQueryTimeMicroseconds;
    extern const Event ASTFuzzerQueries;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool allow_experimental_kusto_dialect;
    extern const SettingsBool allow_experimental_prql_dialect;
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool ast_fuzzer_any_query;
    extern const SettingsFloat ast_fuzzer_runs;
    extern const SettingsBool async_insert;
    extern const SettingsBool calculate_text_stack_trace;
    extern const SettingsBool deduplicate_blocks_in_dependent_materialized_views;
    extern const SettingsDialect dialect;
    extern const SettingsOverflowMode distinct_overflow_mode;
    extern const SettingsBool enable_global_with_statement;
    extern const SettingsBool enable_reads_from_query_cache;
    extern const SettingsBool enable_writes_to_query_cache;
    extern const SettingsSetOperationMode except_default_mode;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsBool implicit_transaction;
    extern const SettingsUInt64 interactive_delay;
    extern const SettingsSetOperationMode intersect_default_mode;
    extern const SettingsOverflowMode join_overflow_mode;
    extern const SettingsString log_comment;
    extern const SettingsBool log_formatted_queries;
    extern const SettingsBool log_profile_events;
    extern const SettingsUInt64 log_queries_cut_to_length;
    extern const SettingsBool log_queries;
    extern const SettingsMilliseconds log_queries_min_query_duration_ms;
    extern const SettingsLogQueriesType log_queries_min_type;
    extern const SettingsFloat log_queries_probability;
    extern const SettingsBool log_query_settings;
    extern const SettingsUInt64 max_ast_depth;
    extern const SettingsUInt64 max_ast_elements;
    extern const SettingsNonZeroUInt64 max_block_size;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_result_bytes;
    extern const SettingsUInt64 max_result_rows;
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsUInt64 output_format_compression_zstd_window_log;
    extern const SettingsBool query_cache_compress_entries;
    extern const SettingsUInt64 query_cache_max_entries;
    extern const SettingsUInt64 query_cache_max_size_in_bytes;
    extern const SettingsMilliseconds query_cache_min_query_duration;
    extern const SettingsUInt64 query_cache_min_query_runs;
    extern const SettingsQueryResultCacheNondeterministicFunctionHandling query_cache_nondeterministic_function_handling;
    extern const SettingsBool query_cache_share_between_users;
    extern const SettingsBool query_cache_squash_partial_results;
    extern const SettingsQueryResultCacheSystemTableHandling query_cache_system_table_handling;
    extern const SettingsSeconds query_cache_ttl;
    extern const SettingsInt64 query_metric_log_interval;
    extern const SettingsOverflowMode read_overflow_mode;
    extern const SettingsOverflowMode read_overflow_mode_leaf;
    extern const SettingsOverflowMode result_overflow_mode;
    extern const SettingsOverflowMode set_overflow_mode;
    extern const SettingsOverflowMode sort_overflow_mode;
    extern const SettingsBool throw_on_unsupported_query_inside_transaction;
    extern const SettingsOverflowMode timeout_overflow_mode;
    extern const SettingsOverflowMode transfer_overflow_mode;
    extern const SettingsSetOperationMode union_default_mode;
    extern const SettingsBool use_query_cache;
    extern const SettingsBool wait_for_async_insert;
    extern const SettingsSeconds wait_for_async_insert_timeout;
    extern const SettingsBool implicit_select;
    extern const SettingsBool enforce_strict_identifier_format;
    extern const SettingsMap http_response_headers;
    extern const SettingsBool apply_mutations_on_fly;
    extern const SettingsFloat min_os_cpu_wait_time_ratio_to_throw;
    extern const SettingsFloat max_os_cpu_wait_time_ratio_to_throw;
    extern const SettingsBool allow_experimental_time_series_table;
    extern const SettingsString promql_database;
    extern const SettingsString promql_table;
    extern const SettingsFloatAuto promql_evaluation_time;
    extern const SettingsBool enable_shared_storage_snapshot_in_query;
    extern const SettingsUInt64Auto insert_quorum;
    extern const SettingsBool insert_quorum_parallel;
    extern const SettingsBool ignore_format_null_for_explain;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt64 os_cpu_busy_time_threshold;
}

namespace ErrorCodes
{
    extern const int QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS;
    extern const int QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE;
    extern const int QUERY_CACHE_USED_WITH_SYSTEM_TABLE;
    extern const int INTO_OUTFILE_NOT_ALLOWED;
    extern const int INVALID_TRANSACTION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int QUERY_WAS_CANCELLED_BY_CLIENT;
    extern const int SYNTAX_ERROR;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
    extern const int ABORTED;
    extern const int UNSUPPORTED_PARAMETER;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char execute_query_calling_empty_set_result_func_on_exception[];
    extern const char terminate_with_exception[];
    extern const char terminate_with_std_exception[];
    extern const char libcxx_hardening_out_of_bounds_assertion[];
}

static void checkASTSizeLimits(const IAST & ast, const Settings & settings)
{
    if (settings[Setting::max_ast_depth])
        ast.checkDepth(settings[Setting::max_ast_depth]);
    if (settings[Setting::max_ast_elements])
        ast.checkSize(settings[Setting::max_ast_elements]);
}


/// Log query into text log (not into system table).
static void logQuery(const String & query, ContextPtr context, bool internal, QueryProcessingStage::Enum stage)
{
    if (internal)
    {
        LOG_DEBUG(getLogger("executeQuery"), "(internal) {} (stage: {})", toOneLineQuery(query), QueryProcessingStage::toString(stage));
    }
    else
    {
        const auto & client_info = context->getClientInfo();

        const auto & current_query_id = client_info.current_query_id;
        const auto & initial_query_id = client_info.initial_query_id;
        const auto & current_user = client_info.current_user;

        String comment = context->getSettingsRef()[Setting::log_comment];
        size_t max_query_size = context->getSettingsRef()[Setting::max_query_size];

        if (comment.size() > max_query_size)
            comment.resize(max_query_size);

        if (!comment.empty())
            comment = fmt::format(" (comment: {})", comment);

        String line_info;
        if (client_info.script_line_number)
            line_info = fmt::format(" (query {}, line {})", client_info.script_query_number, client_info.script_line_number);

        String transaction_info;
        if (auto txn = context->getCurrentTransaction())
            transaction_info = fmt::format(" (TID: {}, TIDH: {})", txn->tid, txn->tid.getHash());

        LOG_DEBUG(getLogger("executeQuery"), "(from {}{}{}){}{}{} {} (stage: {})",
            client_info.current_address->toString(),
            (current_user != "default" ? ", user: " + current_user : ""),
            (!initial_query_id.empty() && current_query_id != initial_query_id ? ", initial_query_id: " + initial_query_id : std::string()),
            transaction_info,
            comment,
            line_info,
            toOneLineQuery(query),
            QueryProcessingStage::toString(stage));

        if (client_info.client_trace_context.trace_id != UUID())
        {
            LOG_TRACE(getLogger("executeQuery"),
                "OpenTelemetry traceparent '{}'",
                client_info.client_trace_context.composeTraceparentHeader());
        }
    }
}

/// Log exception (with query info) into text log (not into system table).
static void logException(ContextPtr context, QueryLogElement & elem, bool log_error = true)
{
    String comment;
    if (!elem.log_comment.empty())
        comment = fmt::format(" (comment: {})", elem.log_comment);

    /// Message patterns like "{} (from {}){} (in query: {})" are not really informative,
    /// so we pass elem.exception_format_string as format string instead.
    PreformattedMessage message;
    message.format_string = elem.exception_format_string;
    message.format_string_args = elem.exception_format_string_args;

    const auto & client_info = context->getClientInfo();
    String line_info;
    if (client_info.script_line_number)
        line_info = fmt::format(" (query {}, line {})", client_info.script_query_number, client_info.script_line_number);

    if (elem.stack_trace.empty() || !log_error)
        message.text = fmt::format("{} (from {}){}{} (in query: {})", elem.exception,
                        context->getClientInfo().current_address->toString(),
                        comment,
                        line_info,
                        toOneLineQuery(elem.query));
    else
        message.text = fmt::format(
            "{} (from {}){}{} (in query: {}), Stack trace (when copying this message, always include the lines below):\n\n{}",
            elem.exception,
            context->getClientInfo().current_address->toString(),
            comment,
            line_info,
            toOneLineQuery(elem.query),
            elem.stack_trace);

    if (log_error)
        LOG_ERROR(getLogger("executeQuery"), message);
    else
        LOG_INFO(getLogger("executeQuery"), message);
}

static void
addPrivilegesInfoToQueryLogElement(QueryLogElement & element, const ContextPtr context_ptr)
{
    const auto & privileges_info = context_ptr->getQueryPrivilegesInfo();
    {
        std::lock_guard lock(privileges_info.mutex);
        element.used_privileges = privileges_info.used_privileges;
        element.missing_privileges = privileges_info.missing_privileges;
    }
}

static void
addStatusInfoToQueryLogElement(QueryLogElement & element, const QueryStatusInfo & info, const ASTPtr query_ast, const ContextPtr context_ptr, std::chrono::system_clock::time_point time)
{
    UInt64 elapsed_microseconds = info.elapsed_microseconds;
    element.event_time = timeInSeconds(time);
    element.event_time_microseconds = timeInMicroseconds(time);
    element.query_duration_ms = elapsed_microseconds / 1000;

    ProfileEvents::increment(ProfileEvents::QueryTimeMicroseconds, elapsed_microseconds);
    if (!query_ast || query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
    {
        ProfileEvents::increment(ProfileEvents::SelectQueryTimeMicroseconds, elapsed_microseconds);
    }
    else if (query_ast->as<ASTInsertQuery>())
    {
        ProfileEvents::increment(ProfileEvents::InsertQueryTimeMicroseconds, elapsed_microseconds);
    }
    else
    {
        ProfileEvents::increment(ProfileEvents::OtherQueryTimeMicroseconds, elapsed_microseconds);
    }

    element.read_rows = info.read_rows;
    element.read_bytes = info.read_bytes;

    element.written_rows = info.written_rows;
    element.written_bytes = info.written_bytes;

    element.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;

    element.thread_ids = info.thread_ids;
    element.peak_threads_usage = info.peak_threads_usage;
    element.profile_counters = info.profile_counters;

    /// We need to refresh the access info since dependent views might have added extra information, either during
    /// creation of the view (PushingToViews chain) or while executing its internal SELECT
    const auto & access_info = context_ptr->getQueryAccessInfo();
    {
        std::lock_guard lock(access_info.mutex);
        element.query_databases.insert(access_info.databases.begin(), access_info.databases.end());
        element.query_tables.insert(access_info.tables.begin(), access_info.tables.end());
        element.query_columns.insert(access_info.columns.begin(), access_info.columns.end());
        element.query_partitions.insert(access_info.partitions.begin(), access_info.partitions.end());
        element.query_projections.insert(access_info.projections.begin(), access_info.projections.end());
        element.query_views.insert(access_info.views.begin(), access_info.views.end());
    }

    /// We copy QueryFactoriesInfo for thread-safety, because it is possible that query context can be modified by some processor even
    /// after query is finished
    const auto & factories_info(context_ptr->getQueryFactoriesInfo());
    {
        std::lock_guard lock(factories_info.mutex);
        element.used_aggregate_functions = factories_info.aggregate_functions;
        element.used_aggregate_function_combinators = factories_info.aggregate_function_combinators;
        element.used_database_engines = factories_info.database_engines;
        element.used_data_type_families = factories_info.data_type_families;
        element.used_dictionaries = factories_info.dictionaries;
        element.used_formats = factories_info.formats;
        element.used_functions = factories_info.functions;
        element.used_storages = factories_info.storages;
        element.used_table_functions = factories_info.table_functions;
        element.used_executable_user_defined_functions = factories_info.executable_user_defined_functions;
        element.used_sql_user_defined_functions = factories_info.sql_user_defined_functions;
    }

    element.async_read_counters = context_ptr->getAsyncReadCounters();
    addPrivilegesInfoToQueryLogElement(element, context_ptr);
}

static UInt64 getQueryMetricLogInterval(ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    auto interval_milliseconds = settings[Setting::query_metric_log_interval];
    if (interval_milliseconds < 0)
        interval_milliseconds = context->getConfigRef().getUInt64("query_metric_log.collect_interval_milliseconds", 1000);

    return interval_milliseconds;
}

QueryLogElement logQueryStart(
    const std::chrono::time_point<std::chrono::system_clock> & query_start_time,
    const ContextMutablePtr & context,
    const String & query_for_logging,
    UInt64 normalized_query_hash,
    const ASTPtr & query_ast,
    const QueryPipeline & pipeline,
    const IInterpreter * interpreter,
    bool internal,
    const String & query_database,
    const String & query_table,
    bool async_insert)
{
    const Settings & settings = context->getSettingsRef();

    QueryLogElement elem;

    elem.type = QueryLogElementType::QUERY_START;
    elem.event_time = timeInSeconds(query_start_time);
    elem.event_time_microseconds = timeInMicroseconds(query_start_time);
    elem.query_start_time = timeInSeconds(query_start_time);
    elem.query_start_time_microseconds = timeInMicroseconds(query_start_time);

    elem.current_database = context->getCurrentDatabase();
    elem.query = query_for_logging;
    if (query_ast && settings[Setting::log_formatted_queries])
        elem.formatted_query = query_ast->formatWithSecretsOneLine();
    elem.normalized_query_hash = normalized_query_hash;
    elem.query_kind = query_ast ? query_ast->getQueryKind() : IAST::QueryKind::Select;

    elem.client_info = context->getClientInfo();

    elem.is_internal = internal;

    if (auto txn = context->getCurrentTransaction())
        elem.tid = txn->tid;

    bool log_queries = settings[Setting::log_queries];

    auto query_log = context->getQueryLog();
    if (!query_log)
        return elem;

    /// Log into system table start of query execution, if need.
    if (log_queries)
    {
        /// This check is not obvious, but without it 01220_scalar_optimization_in_alter fails.
        if (pipeline.initialized())
        {
            const auto & info = context->getQueryAccessInfo();
            std::lock_guard lock(info.mutex);
            elem.query_databases = info.databases;
            elem.query_tables = info.tables;
            elem.query_columns = info.columns;
            elem.query_partitions = info.partitions;
            elem.query_projections = info.projections;
            elem.query_views = info.views;
        }

        if (async_insert)
            InterpreterInsertQuery::extendQueryLogElemImpl(elem, context);
        else if (interpreter)
            interpreter->extendQueryLogElem(elem, query_ast, context, query_database, query_table);

        if (settings[Setting::log_query_settings])
            elem.query_settings = std::make_shared<Settings>(context->getSettingsRef());

        elem.log_comment = settings[Setting::log_comment];
        if (elem.log_comment.size() > settings[Setting::max_query_size])
            elem.log_comment.resize(settings[Setting::max_query_size]);

        if (elem.type >= settings[Setting::log_queries_min_type] && !settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
        {
            if (!settings[Setting::log_query_settings] && settings[Setting::log_query_settings].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query settings to 'system.query_log' since setting `log_query_settings` is false"
                    " (the setting was changed for the query).");

            query_log->add(elem);
        }
        else if (elem.type < settings[Setting::log_queries_min_type])
        {
            if (settings[Setting::log_queries_min_type].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query start record to 'system.query_log' because the query type is smaller than setting `log_queries_min_type`"
                    " (the setting was changed for the query).");
        }
        else if (settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
        {
            if (settings[Setting::log_queries_min_query_duration_ms].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query start record to 'system.query_log' since setting `log_queries_min_query_duration_ms` > 0"
                    " (the setting was changed for the query).");
        }
    }
    else if (!internal && !settings[Setting::log_queries])
    {
        if (settings[Setting::log_queries].changed)
            LOG_TRACE(
                getLogger("executeQuery"),
                "Not adding query to 'system.query_log' since setting `log_queries` is false"
                " (the setting was changed for the query).");
    }

    if (auto query_metric_log = context->getQueryMetricLog(); query_metric_log && !internal)
    {
        auto interval_milliseconds = getQueryMetricLogInterval(context);
        if (interval_milliseconds > 0)
            query_metric_log->startQuery(elem.client_info.current_query_id, query_start_time, interval_milliseconds);
    }

    return elem;
}

void logQueryMetricLogFinish(ContextPtr context, bool internal, String query_id, std::chrono::system_clock::time_point finish_time, QueryStatusInfoPtr info)
{
    if (auto query_metric_log = context->getQueryMetricLog(); query_metric_log && !internal)
    {
        auto interval_milliseconds = getQueryMetricLogInterval(context);
        if (info && interval_milliseconds > 0)
        {
            /// Only collect data on query finish if the elapsed time exceeds the interval to collect.
            /// If we don't do this, it's counter-intuitive to have a single entry for every quick query
            /// where the data is basically a subset of the query_log.
            /// On the other hand, it's very convenient to have a new entry whenever the query finishes
            /// so that we can get nice time-series querying only query_metric_log without the need
            /// to query the final state in query_log.
            auto collect_on_finish = info->elapsed_microseconds > interval_milliseconds * 1000;
            auto query_info = collect_on_finish ? info : nullptr;
            query_metric_log->finishQuery(query_id, finish_time, query_info);
        }
        else
        {
            query_metric_log->finishQuery(query_id, finish_time, nullptr);
        }
    }
}

static ResultProgress flushQueryProgress(const QueryPipeline & pipeline, bool pulling_pipeline, const ProgressCallback & progress_callback, QueryStatusPtr process_list_elem)
{
    ResultProgress res(0, 0, 0);

    if (pulling_pipeline)
    {
        pipeline.tryGetResultRowsAndBytes(res.result_rows, res.result_bytes);
    }
    else if (process_list_elem) /// will be used only for ordinary INSERT queries
    {
        auto progress_out = process_list_elem->getProgressOut();
        res.result_rows = progress_out.written_rows;
        res.result_bytes = progress_out.written_bytes;
    }

    /// Report same memory_usage in X-ClickHouse-Summary as in query_log
    if (process_list_elem)
        res.memory_usage = std::max<Int64>(process_list_elem->getInfo().peak_memory_usage, 0);

    if (progress_callback)
    {
        Progress p;
        p.incrementPiecewiseAtomically(Progress{res});
        progress_callback(p);
    }

    return res;
}

QueryPipelineFinalizedInfo finalizeQueryPipelineBeforeLogging(QueryPipeline && query_pipeline, QueryResultCacheUsage query_result_cache_usage, bool pulling_pipeline)
{
    if (query_result_cache_usage == QueryResultCacheUsage::Write)
        /// Trigger the actual write of the buffered query result into the query result cache. This is done explicitly to
        /// prevent partial/garbage results in case of exceptions during query execution.
        query_pipeline.finalizeWriteInQueryResultCache();

    std::vector<IProcessor::ProcessorsProfileLogInfo> processors_profile_infos = getProcessorsProfileLogInfo(query_pipeline.getProcessors());

    String pipeline_dump;
    {
        WriteBufferFromString out(pipeline_dump);
        printPipeline(query_pipeline.getProcessors(), out, true);
    }

    std::optional<ResultProgress> result_progress;
    if (pulling_pipeline)
    {
        UInt64 result_rows = 0;
        UInt64 result_bytes = 0;
        query_pipeline.tryGetResultRowsAndBytes(result_rows, result_bytes);
        result_progress = std::make_optional<ResultProgress>(result_rows, result_bytes, 0);
    }

    /// Reset pipeline before fetching profile counters
    query_pipeline.reset();

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    return QueryPipelineFinalizedInfo{
        .result_progress = std::move(result_progress),
        .processors_profile_infos = std::move(processors_profile_infos),
        .pipeline_dump = std::move(pipeline_dump)};
}

void logQueryFinishImpl(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const ASTPtr & query_ast,
    const QueryPipelineFinalizedInfo & query_pipeline_finalized_info,
    bool pulling_pipeline,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    QueryResultCacheUsage query_result_cache_usage,
    bool internal,
    std::chrono::system_clock::time_point time)
{
    const Settings & settings = context->getSettingsRef();
    auto log_queries = settings[Setting::log_queries];

    if (QueryStatusPtr process_list_elem = context->getProcessListElement())
    {
        {
            ResultProgress result_progress(0, 0, 0);

            chassert((query_pipeline_finalized_info.result_progress != std::nullopt) == pulling_pipeline);

            if (query_pipeline_finalized_info.result_progress)
            {
                result_progress = *query_pipeline_finalized_info.result_progress;
            }
            else if (!pulling_pipeline)
            {
                auto progress_out = process_list_elem->getProgressOut();
                result_progress.result_rows = progress_out.written_rows;
                result_progress.result_bytes = progress_out.written_bytes;
            }

            if (auto progress_callback = context->getProgressCallback())
            {
                Progress p;
                p.incrementPiecewiseAtomically(Progress{result_progress});
                progress_callback(p);
            }

            elem.result_rows = result_progress.result_rows;
            elem.result_bytes = result_progress.result_bytes;
        }

        QueryStatusInfo info = process_list_elem->getInfo(true, settings[Setting::log_profile_events]);
        logQueryMetricLogFinish(context, internal, elem.client_info.current_query_id, time, std::make_shared<QueryStatusInfo>(info));

        elem.type = QueryLogElementType::QUERY_FINISH;

        addStatusInfoToQueryLogElement(elem, info, query_ast, context, time);

        if (elem.read_rows != 0)
        {
            double elapsed_seconds = static_cast<double>(info.elapsed_microseconds) / 1000000.0;
            double rows_per_second = static_cast<double>(elem.read_rows) / elapsed_seconds;
            double bytes_per_second = static_cast<double>(elem.read_bytes) / elapsed_seconds;
            LOG_DEBUG(
                getLogger("executeQuery"),
                "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
                elem.read_rows,
                ReadableSize(elem.read_bytes),
                elapsed_seconds,
                rows_per_second,
                ReadableSize(bytes_per_second));
        }

        context->getRuntimeFilterLookup()->logStats();

        elem.query_result_cache_usage = query_result_cache_usage;

        elem.is_internal = internal;

        if (log_queries && elem.type >= settings[Setting::log_queries_min_type]
            && static_cast<Int64>(elem.query_duration_ms) >= settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
        {
            if (auto query_log = context->getQueryLog())
                query_log->add(elem);
        }

    }

    if (query_span && query_span->isTraceEnabled())
    {
        query_span->addAttribute("db.statement", elem.query);
        query_span->addAttribute("clickhouse.query_id", elem.client_info.current_query_id);
        query_span->addAttribute("clickhouse.query_status", "QueryFinish");
        query_span->addAttributeIfNotEmpty("clickhouse.tracestate", OpenTelemetry::CurrentContext().tracestate);
        query_span->addAttributeIfNotZero("clickhouse.read_rows", elem.read_rows);
        query_span->addAttributeIfNotZero("clickhouse.read_bytes", elem.read_bytes);
        query_span->addAttributeIfNotZero("clickhouse.written_rows", elem.written_rows);
        query_span->addAttributeIfNotZero("clickhouse.written_bytes", elem.written_bytes);
        query_span->addAttributeIfNotZero("clickhouse.memory_usage", elem.memory_usage);

        if (context)
        {
            std::string user_name = context->getUserName();
            query_span->addAttribute("clickhouse.user", user_name);
        }

        if (settings[Setting::log_query_settings])
        {
            auto changes = settings.changes();
            for (const auto & change : changes)
            {
                query_span->addAttribute(fmt::format("clickhouse.setting.{}", change.name), convertFieldToString(change.value));
            }
        }
        query_span->finish(time);
    }

    if (!query_pipeline_finalized_info.processors_profile_infos.empty())
        logProcessorProfile(context, query_pipeline_finalized_info.processors_profile_infos, query_pipeline_finalized_info.pipeline_dump);
}

void logQueryFinish(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const ASTPtr & query_ast,
    QueryPipeline && query_pipeline,
    bool pulling_pipeline,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    QueryResultCacheUsage query_result_cache_usage,
    bool internal)
{
    const auto time_now = std::chrono::system_clock::now();
    auto query_pipeline_finalized_info = finalizeQueryPipelineBeforeLogging(std::move(query_pipeline), query_result_cache_usage, pulling_pipeline);
    logQueryFinishImpl(elem, context, query_ast, query_pipeline_finalized_info, pulling_pipeline, query_span, query_result_cache_usage, internal, time_now);
}

void logQueryException(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const Stopwatch & start_watch,
    const ASTPtr & query_ast,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    bool internal,
    bool log_error)
{
    const Settings & settings = context->getSettingsRef();
    auto log_queries = settings[Setting::log_queries];

    elem.type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;
    elem.exception_code = getCurrentExceptionCode();
    auto exception_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false);
    elem.exception = std::move(exception_message.text);
    elem.exception_format_string = exception_message.format_string;
    elem.exception_format_string_args = exception_message.format_string_args;

    QueryStatusPtr process_list_elem = context->getProcessListElement();

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();
    const auto time_now = std::chrono::system_clock::now();
    elem.event_time = timeInSeconds(time_now);
    elem.event_time_microseconds = timeInMicroseconds(time_now);

    ProfileEvents::increment(ProfileEvents::FailedQuery);
    if (!query_ast || query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
        ProfileEvents::increment(ProfileEvents::FailedSelectQuery);
    else if (query_ast->as<ASTInsertQuery>())
        ProfileEvents::increment(ProfileEvents::FailedInsertQuery);

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        ProfileEvents::increment(ProfileEvents::FailedInitialQuery);
        if (!query_ast || query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInitialSelectQuery);
    }

    if (internal)
    {
        ProfileEvents::increment(ProfileEvents::FailedInternalQuery);
        if (!query_ast || query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInternalSelectQuery);
        else if (query_ast->as<ASTInsertQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInternalInsertQuery);
    }

    QueryStatusInfoPtr info;
    if (process_list_elem)
    {
        info = std::make_shared<QueryStatusInfo>(process_list_elem->getInfo(true, settings[Setting::log_profile_events], false));
        addStatusInfoToQueryLogElement(elem, *info, query_ast, context, time_now);
    }
    else
    {
        elem.query_duration_ms = start_watch.elapsedMilliseconds();
    }
    logQueryMetricLogFinish(context, internal, elem.client_info.current_query_id, time_now, info);

    elem.query_result_cache_usage = QueryResultCacheUsage::None;

    elem.is_internal = internal;

    if (settings[Setting::calculate_text_stack_trace] && log_error)
        elem.stack_trace = getExceptionStackTraceString(std::current_exception());
    logException(context, elem, log_error);

    /// In case of exception we log internal queries also
    if (log_queries && elem.type >= settings[Setting::log_queries_min_type]
        && static_cast<Int64>(elem.query_duration_ms) >= settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
    {
        if (auto query_log = context->getQueryLog())
            query_log->add(elem);
    }

    if (query_span)
    {
        query_span->addAttribute("db.statement", elem.query);
        query_span->addAttribute("clickhouse.query_id", elem.client_info.current_query_id);
        query_span->addAttribute("clickhouse.exception", elem.exception);
        query_span->addAttribute("clickhouse.exception_code", elem.exception_code);
        query_span->finish(time_now);
    }
}

void logExceptionBeforeStart(
    const String & query_for_logging,
    UInt64 normalized_query_hash,
    ContextPtr context,
    ASTPtr ast,
    const std::shared_ptr<OpenTelemetry::SpanHolder> & query_span,
    UInt64 elapsed_milliseconds,
    bool internal)
{
    auto query_end_time = std::chrono::system_clock::now();

    /// Exception before the query execution.
    if (auto quota = context->getQuota())
        quota->used(QuotaType::ERRORS, 1, /* check_exceeded = */ false);

    const Settings & settings = context->getSettingsRef();

    const auto & client_info = context->getClientInfo();

    /// Log the start of query execution into the table if necessary.
    QueryLogElement elem;

    elem.type = QueryLogElementType::EXCEPTION_BEFORE_START;
    elem.event_time = timeInSeconds(query_end_time);
    elem.event_time_microseconds = timeInMicroseconds(query_end_time);
    elem.query_start_time = client_info.initial_query_start_time;
    elem.query_start_time_microseconds = client_info.initial_query_start_time_microseconds;
    elem.query_duration_ms = elapsed_milliseconds;

    elem.current_database = context->getCurrentDatabase();
    elem.query = query_for_logging;
    elem.normalized_query_hash = normalized_query_hash;

    // Log query_kind if ast is valid
    if (ast)
    {
        elem.query_kind = ast->getQueryKind();
        if (settings[Setting::log_formatted_queries])
            elem.formatted_query = ast->formatWithSecretsOneLine();
    }

    addPrivilegesInfoToQueryLogElement(elem, context);

    // We don't calculate databases, tables and columns when the query isn't able to start

    elem.exception_code = getCurrentExceptionCode();
    auto exception_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false);
    elem.exception = std::move(exception_message.text);
    elem.exception_format_string = exception_message.format_string;
    elem.exception_format_string_args = exception_message.format_string_args;

    elem.client_info = context->getClientInfo();

    elem.log_comment = settings[Setting::log_comment];
    if (elem.log_comment.size() > settings[Setting::max_query_size])
        elem.log_comment.resize(settings[Setting::max_query_size]);

    if (auto txn = context->getCurrentTransaction())
        elem.tid = txn->tid;

    if (settings[Setting::log_query_settings])
        elem.query_settings = std::make_shared<Settings>(settings);

    if (settings[Setting::calculate_text_stack_trace])
        elem.stack_trace = getExceptionStackTraceString(std::current_exception());

    elem.is_internal = internal;

    bool log_error = elem.exception_code != ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT && elem.exception_code !=  ErrorCodes::QUERY_WAS_CANCELLED;
    logException(context, elem, log_error);

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    ProfileEvents::increment(ProfileEvents::FailedQuery);
    if (!ast || ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
        ProfileEvents::increment(ProfileEvents::FailedSelectQuery);
    else if (ast->as<ASTInsertQuery>())
        ProfileEvents::increment(ProfileEvents::FailedInsertQuery);

    if (context->getClientInfo().query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        ProfileEvents::increment(ProfileEvents::FailedInitialQuery);
        if (!ast || ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInitialSelectQuery);
    }

    QueryStatusInfoPtr info;
    if (QueryStatusPtr process_list_elem = context->getProcessListElementSafe())
    {
        info = std::make_shared<QueryStatusInfo>(process_list_elem->getInfo(true, settings[Setting::log_profile_events], false));
        addStatusInfoToQueryLogElement(elem, *info, ast, context, query_end_time);
    }
    logQueryMetricLogFinish(context, /*internal=*/ false, elem.client_info.current_query_id, query_end_time, info);

    if (auto query_log = context->getQueryLog())
    {
        if (settings[Setting::log_queries] && elem.type >= settings[Setting::log_queries_min_type]
            && !settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
        {
            if (!settings[Setting::log_query_settings] && settings[Setting::log_query_settings].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query settings to 'system.query_log' since setting `log_query_settings` is false"
                    " (the setting was changed for the query).");

            query_log->add(elem);
        }
        else if (!settings[Setting::log_queries])
        {
            if (settings[Setting::log_queries].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query to 'system.query_log' since setting `log_queries` is false"
                    " (the setting was changed for the query).");
        }
        else if (elem.type < settings[Setting::log_queries_min_type])
        {
            if (settings[Setting::log_queries_min_type].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query to 'system.query_log' since the query type is smaller than setting `log_queries_min_type`"
                    " (the setting was changed for the query).");
        }
        else if (settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
        {
            if (settings[Setting::log_queries_min_query_duration_ms].changed)
                LOG_TRACE(
                    getLogger("executeQuery"),
                    "Not adding query to 'system.query_log' since setting `log_queries_min_query_duration_ms` > 0 and the query failed before start"
                    " (the setting was changed for the query).");
        }
    }

    if (query_span)
    {
        query_span->addAttribute("clickhouse.exception_code", elem.exception_code);
        query_span->addAttribute("clickhouse.exception", elem.exception);
        query_span->addAttribute("db.statement", elem.query);
        query_span->addAttribute("clickhouse.query_id", elem.client_info.current_query_id);
        query_span->finish(query_end_time);
    }
}

void validateAnalyzerSettings(ASTPtr ast, bool context_value)
{
    if (ast->as<ASTSetQuery>())
        return;

    bool top_level = context_value;

    auto field_to_bool = [](const Field & f) -> bool
    {
        if (f.getType() == Field::Types::String)
            return stringToBool(f.safeGet<String>());
        else
            return f.safeGet<bool>();
    };

    std::vector<ASTPtr> nodes_to_process{ ast };
    while (!nodes_to_process.empty())
    {
        auto node = nodes_to_process.back();
        nodes_to_process.pop_back();

        if (auto * set_query = node->as<ASTSetQuery>())
        {
            if (auto * value = set_query->changes.tryGet("allow_experimental_analyzer"))
            {
                if (top_level != field_to_bool(*value))
                    throw Exception(ErrorCodes::INCORRECT_QUERY, "Setting 'allow_experimental_analyzer' is changed in the subquery. Top level value: {}", top_level);
            }

            if (auto * value = set_query->changes.tryGet("enable_analyzer"))
            {
                if (top_level != field_to_bool(*value))
                    throw Exception(ErrorCodes::INCORRECT_QUERY, "Setting 'enable_analyzer' is changed in the subquery. Top level value: {}", top_level);
            }
        }

        for (auto child : node->children)
        {
            if (child)
                nodes_to_process.push_back(std::move(child));
        }
    }
}

class ImplicitTransactionControlExecutor
{
public:
    void begin(const ContextMutablePtr & query_context)
    {
        ASTPtr tcl_ast = make_intrusive<ASTTransactionControl>(ASTTransactionControl::BEGIN);
        InterpreterTransactionControlQuery tc(tcl_ast, query_context);
        tc.execute();
        auto txn = query_context->getCurrentTransaction();
        LOG_TRACE(getLogger("ImplicitTransactionControlExecutor"), "Begin implicit transaction {}", txn->tid);

        transaction_running = true;
    }

    void commit(const ContextMutablePtr & query_context)
    {
        chassert(transaction_running);

        auto txn = query_context->getCurrentTransaction();
        chassert(txn);
        LOG_TRACE(getLogger("ImplicitTransactionControlExecutor"), "Commit implicit transaction {}", txn->tid);

        SCOPE_EXIT({ transaction_running = false; });

        ASTPtr tcl_ast = make_intrusive<ASTTransactionControl>(ASTTransactionControl::COMMIT);
        InterpreterTransactionControlQuery tc(tcl_ast, query_context);
        tc.execute();
    }

    void rollback(const ContextMutablePtr & query_context)
    {
        chassert(transaction_running);

        auto txn = query_context->getCurrentTransaction();
        chassert(txn);
        LOG_TRACE(getLogger("ImplicitTransactionControlExecutor"), "Rollback implicit transaction {}", txn->tid);

        SCOPE_EXIT({ transaction_running = false; });

        ASTPtr tcl_ast = make_intrusive<ASTTransactionControl>(ASTTransactionControl::ROLLBACK);
        InterpreterTransactionControlQuery tc(tcl_ast, query_context);
        tc.execute();
    }
    bool transactionRunning() const { return transaction_running; }

private:
    bool transaction_running{false};
};

using ImplicitTransactionControlExecutorPtr = std::shared_ptr<ImplicitTransactionControlExecutor>;


static BlockIO executeQueryImpl(
    const char * begin,
    const char * end,
    ContextMutablePtr context,
    QueryFlags flags,
    QueryProcessingStage::Enum stage,
    ReadBufferUniquePtr & istr,
    ASTPtr & out_ast,
    ImplicitTransactionControlExecutorPtr implicit_tcl_executor,
    HTTPContinueCallback http_continue_callback,
    QueryResultDetails & result_details)
{
    const bool internal = flags.internal;

    /// query_span is a special span, when this function exits, it's lifetime is not ended, but ends when the query finishes.
    /// Some internal queries might call this function recursively by setting 'internal' parameter to 'true',
    /// to make sure SpanHolders in current stack ends in correct order, we disable this span for these internal queries
    ///
    /// This does not have impact on the final span logs, because these internal queries are issued by external queries,
    /// we still have enough span logs for the execution of external queries.
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = internal ? nullptr : std::make_shared<OpenTelemetry::SpanHolder>("query");
    if (query_span && query_span->trace_id != UUID{})
        LOG_TRACE(getLogger("executeQuery"), "Query span trace_id for opentelemetry log: {}", query_span->trace_id);

    /// Used for logging query start time in system.query_log
    auto query_start_time = std::chrono::system_clock::now();

    /// Used for:
    /// * Setting the watch in QueryStatus (controls timeouts and progress) and the output formats
    /// * Logging query duration (system.query_log)
    Stopwatch start_watch{CLOCK_MONOTONIC};

    const auto & client_info = context->getClientInfo();

    if (client_info.initial_query_start_time == 0)
    {
        // If we don't see an initial_query_start_time yet, initialize it to current time.
        // It's possible to have unset initial_query_start_time for non-initial queries. For
        // example, the query is from an initiator that is running an old version of clickhouse.
        // On the other hand, if it's initialized then take it as the start of the query
        context->setInitialQueryStartTime(query_start_time);
    }

    assert(internal || CurrentThread::get().getQueryContext());
    assert(internal || CurrentThread::get().getQueryContext()->getCurrentQueryId() == CurrentThread::getQueryId());

    const Settings & settings = context->getSettingsRef();

    size_t max_query_size = settings[Setting::max_query_size];
    /// Don't limit the size of internal queries or distributed subquery.
    if (internal || client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        max_query_size = 0;

    String query;
    String query_for_logging;
    UInt64 normalized_query_hash;
    size_t log_queries_cut_to_length = settings[Setting::log_queries_cut_to_length];

    /// Parse the query from string.
    try
    {
        if (stage == QueryProcessingStage::QueryPlan)
        {
            /// Do not parse Query
            /// Increment ProfileEvents::Query here because Interpreter is not created.
            ProfileEvents::increment(ProfileEvents::Query);
        }
        else if (settings[Setting::dialect] == Dialect::kusto && !internal)
        {
            if (!settings[Setting::allow_experimental_kusto_dialect])
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Support for Kusto Query Engine (KQL) is disabled (turn on setting 'allow_experimental_kusto_dialect')");
            ParserKQLStatement parser(end, settings[Setting::allow_settings_after_format_in_insert]);
            /// TODO: parser should fail early when max_query_size limit is reached.
            out_ast = parseKQLQuery(parser, begin, end, "", max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        }
        else if (settings[Setting::dialect] == Dialect::prql && !internal)
        {
            if (!settings[Setting::allow_experimental_prql_dialect])
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Support for PRQL is disabled (turn on setting 'allow_experimental_prql_dialect')");
            ParserPRQLQuery parser(max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
            out_ast = parseQuery(parser, begin, end, "", max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        }
        else if (settings[Setting::dialect] == Dialect::promql && !internal)
        {
            if (!settings[Setting::allow_experimental_time_series_table])
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Support for PromQL dialect is disabled (turn on setting 'allow_experimental_time_series_table')");
            ParserPrometheusQuery parser(settings[Setting::promql_database], settings[Setting::promql_table], Field{settings[Setting::promql_evaluation_time]});
            out_ast = parseQuery(parser, begin, end, "", max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        }
        else
        {
            ParserQuery parser(end, settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);
            /// TODO: parser should fail early when max_query_size limit is reached.
            out_ast = parseQuery(parser, begin, end, "", max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);

#ifndef NDEBUG
            try
            {
                /// Verify that AST formatting is consistent:
                /// If you format AST, parse it back, and format it again, you get the same string.
                std::string_view original_query{begin, static_cast<size_t>(end - begin)};

                auto format_ast = [](ASTPtr ast)
                {
                    return ast->formatWithPossiblyHidingSensitiveData(
                        /*max_length=*/0,
                        /*one_line=*/true,
                        /*show_secrets=*/true,
                        /*print_pretty_type_names=*/false,
                        /*identifier_quoting_rule=*/IdentifierQuotingRule::WhenNecessary,
                        /*identifier_quoting_style=*/IdentifierQuotingStyle::Backticks);
                };

                String formatted1 = format_ast(out_ast);

                /// The query can become more verbose after formatting, so:
                size_t size_t_max = -1;
                size_t new_max_query_size = 0;
                if (max_query_size == 0)
                    new_max_query_size = 0;
                else if (max_query_size > (size_t_max - 1000) / 2)
                    new_max_query_size = size_t_max;
                else
                    new_max_query_size = 1000 + 2 * max_query_size;

                ASTPtr ast2;
                try
                {
                    ast2 = parseQuery(
                        parser,
                        formatted1.data(),
                        formatted1.data() + formatted1.size(),
                        "",
                        new_max_query_size,
                        settings[Setting::max_parser_depth],
                        settings[Setting::max_parser_backtracks]);
                }
                catch (const Exception & e)
                {
                    if (e.code() == ErrorCodes::SYNTAX_ERROR)
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                            "Inconsistent AST formatting: the query:\n{}\ncannot parse query back from {}",
                            formatted1, original_query);
                    else
                        throw;
                }

                chassert(ast2);

                String formatted2 = format_ast(ast2);

                if (formatted1 != formatted2)
                {
                    struct ASTDifference
                    {
                        enum class Type : uint8_t
                        {
                            ID,
                            FORMAT
                        };

                        ASTPtr lhs;
                        ASTPtr rhs;
                        Type type;
                    };

                    const auto search_difference_in_asts = [&](this const auto & self, ASTPtr lhs, ASTPtr rhs) -> std::optional<ASTDifference>
                    {
                        if (lhs->getID() != rhs->getID())
                            return std::make_optional(ASTDifference{lhs, rhs, ASTDifference::Type::ID});

                        size_t size_children = std::min(lhs->children.size(), rhs->children.size());
                        for (size_t i = 0; i < size_children; ++i)
                        {
                            const auto & child_lhs = lhs->children[i];
                            const auto & child_rhs = rhs->children[i];
                            if (auto difference = self(child_lhs, child_rhs))
                            {
                                /// In case the format strings are different, use parent nodes for a better debug output.
                                if (difference->type == ASTDifference::Type::FORMAT)
                                    return std::make_optional(ASTDifference{lhs, rhs, ASTDifference::Type::ID});

                                return difference;
                            }
                        }

                        if (format_ast(lhs) != format_ast(rhs))
                            return std::make_optional(ASTDifference{lhs, rhs, ASTDifference::Type::FORMAT});

                        return std::nullopt;
                    };

                    /// Try to find the problematic part of the AST (it's not guaranteed to find it correctly though)
                    if (auto difference = search_difference_in_asts(out_ast, ast2))
                    {
                        auto [lhs, rhs, _] = difference.value();

                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                                        "Inconsistent AST formatting between '{}' and '{}' in the query:\n{}\n"
                                        "Formatted as:\n{}\nParsed and formatted back as:\n{}\n"
                                        "Difference formatted as:\n{}\n{}\nDifference parsed and formatted back as:\n{}\n{}",
                                        lhs->getID(), rhs->getID(),
                                        original_query,
                                        formatted1, formatted2,
                                        format_ast(lhs), lhs->dumpTree(),
                                        format_ast(rhs), rhs->dumpTree());
                    }
                    else
                    {
                        throw Exception(ErrorCodes::LOGICAL_ERROR,
                                        "Inconsistent AST formatting in the query:\n{}\nFormatted as:\n{}\nWas parsed and formatted back as:\n{}",
                                        original_query, formatted1, formatted2);

                    }

                }
            }
            catch (const Exception & e)
            {
                /// Method formatImpl is not supported by MySQLParser::ASTCreateQuery. That code would fail under debug build.
                if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
                    throw;
            }
#endif
        }

        const char * query_end = end;
        bool is_create_parameterized_view = false;

        if (out_ast)
        {
            if (const auto * insert_query = out_ast->as<ASTInsertQuery>(); insert_query && insert_query->data)
                query_end = insert_query->data;

            if (const auto * create_query = out_ast->as<ASTCreateQuery>())
            {
                is_create_parameterized_view = create_query->isParameterizedView();
            }
            else if (const auto * explain_query = out_ast->as<ASTExplainQuery>())
            {
                if (!explain_query->children.empty())
                    if (const auto * create_of_explain_query = explain_query->children[0]->as<ASTCreateQuery>())
                        is_create_parameterized_view = create_of_explain_query->isParameterizedView();
            }
        }

        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        /// Even if we don't have parameters in query_context, check that AST doesn't have unknown parameters
        bool probably_has_params = find_first_symbols<'{'>(begin, end) != end;
        if (out_ast && !is_create_parameterized_view && probably_has_params)
        {
            ReplaceQueryParameterVisitor visitor(context->getQueryParameters());
            visitor.visit(out_ast);
            if (visitor.getNumberOfReplacedParameters())
                query = out_ast->formatWithSecretsOneLine();
            else
                query.assign(begin, query_end);
        }
        else
        {
            /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
            query.assign(begin, query_end);
        }

        /// Wipe any sensitive information (e.g. passwords) from the query.
        /// MUST go before any modification (except for prepared statements,
        /// since it substitute parameters and without them query does not contain
        /// parameters), to keep query as-is in query_log and server log.
        if (out_ast && out_ast->hasSecretParts())
        {
            /// IAST::formatForLogging() wipes secret parts in AST and then calls wipeSensitiveDataAndCutToLength().
            query_for_logging = out_ast->formatForLogging(log_queries_cut_to_length);
        }
        else
        {
            query_for_logging = wipeSensitiveDataAndCutToLength(query, log_queries_cut_to_length, true);
        }

        normalized_query_hash = normalizedQueryHash(query_for_logging, false);
    }
    catch (...)
    {
        /// Anyway log the query.
        if (query.empty())
            query.assign(begin, std::min(static_cast<size_t>(end - begin), max_query_size));

        query_for_logging = wipeSensitiveDataAndCutToLength(query, log_queries_cut_to_length, true);
        logQuery(query_for_logging, context, internal, stage);

        normalized_query_hash = normalizedQueryHash(query_for_logging, false);
        logExceptionBeforeStart(query_for_logging, normalized_query_hash, context, out_ast, query_span, start_watch.elapsedMilliseconds(), internal);
        throw;
    }

    /// Avoid early destruction of process_list_entry if it was not saved to `res` yet (in case of exception)
    ProcessList::EntryPtr process_list_entry;
    QueryMetadataCachePtr query_metadata_cache;
    BlockIO res;
    String query_database;
    String query_table;

    try
    {
        if (auto txn = context->getCurrentTransaction())
        {
            if (txn->getState() == MergeTreeTransaction::COMMITTING)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction {} is in a committing state", txn->tid);
            if (txn->getState() == MergeTreeTransaction::COMMITTED)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction {} has been already committed", txn->tid);
            bool is_special_query = out_ast && (out_ast->as<ASTTransactionControl>() || out_ast->as<ASTExplainQuery>());
            if (txn->getState() == MergeTreeTransaction::ROLLED_BACK && !is_special_query)
                throw Exception(
                    ErrorCodes::INVALID_TRANSACTION,
                    "Cannot execute query because current transaction failed. Expecting ROLLBACK statement");
        }

        /// There is an option of probabilistic logging of queries.
        /// If it is used - do the random sampling and "collapse" the settings.
        /// It allows to consistently log queries with all the subqueries in distributed query processing
        /// (subqueries on remote nodes will receive these "collapsed" settings)
        if (settings[Setting::log_queries] && settings[Setting::log_queries_probability] < 1.0)
        {
            std::bernoulli_distribution should_write_log{settings[Setting::log_queries_probability]};

            context->setSetting("log_queries", should_write_log(thread_local_rng));
            context->setSetting("log_queries_probability", 1.0);
        }

        logQuery(query_for_logging, context, internal, stage);

        if (out_ast)
        {
            /// Interpret SETTINGS clauses as early as possible (before invoking the corresponding interpreter),
            /// to allow settings to take effect.
            InterpreterSetQuery::applySettingsFromQuery(out_ast, context);
            validateAnalyzerSettings(out_ast, settings[Setting::allow_experimental_analyzer]);

            if (settings[Setting::enforce_strict_identifier_format])
            {
                WriteBufferFromOwnString buf;
                IAST::FormatSettings enforce_strict_identifier_format_settings(true);
                enforce_strict_identifier_format_settings.enforce_strict_identifier_format = true;
                out_ast->format(buf, enforce_strict_identifier_format_settings);
            }

            if (auto * insert_query = out_ast->as<ASTInsertQuery>())
                insert_query->tail = std::move(istr);

            if (const auto * query_with_table_output = dynamic_cast<const ASTQueryWithTableAndOutput *>(out_ast.get()))
            {
                query_database = query_with_table_output->getDatabase();
                query_table = query_with_table_output->getTable();
            }

            /// Propagate WITH statement to children ASTSelect.
            if (settings[Setting::enable_global_with_statement])
            {
                ApplyWithGlobalVisitor::visit(out_ast);
            }

            {
                SelectIntersectExceptQueryVisitor::Data data{settings[Setting::intersect_default_mode], settings[Setting::except_default_mode]};
                SelectIntersectExceptQueryVisitor{data}.visit(out_ast);
            }

            {
                /// Normalize SelectWithUnionQuery
                NormalizeSelectWithUnionQueryVisitor::Data data{settings[Setting::union_default_mode]};
                NormalizeSelectWithUnionQueryVisitor{data}.visit(out_ast);
            }

            /// Check the limits.
            checkASTSizeLimits(*out_ast, settings);
        }

        /// Put query to process list. But don't put SHOW PROCESSLIST query itself.
        if (!(out_ast && out_ast->as<ASTShowProcesslistQuery>()))
        {
            /// processlist also has query masked now, to avoid secrets leaks though SHOW PROCESSLIST by other users.
            process_list_entry = context->getProcessList().insert(query_for_logging, normalized_query_hash, out_ast.get(), context, start_watch.getStart(), internal);
            context->setProcessListElement(process_list_entry->getQueryStatus());
        }

        /// Load external tables if they were provided
        context->initializeExternalTablesIfSet();
        std::shared_ptr<QueryPlanAndSets> query_plan;
        if (stage == QueryProcessingStage::QueryPlan)
            query_plan = context->getDeserializedQueryPlan();

        ASTInsertQuery * insert_query = nullptr;
        if (out_ast)
            insert_query = out_ast->as<ASTInsertQuery>();
        bool async_insert_enabled = settings[Setting::async_insert];

        /// Resolve database before trying to use async insert feature - to properly hash the query.
        if (insert_query)
        {
            if (insert_query->table_id)
                insert_query->table_id = context->resolveStorageID(insert_query->table_id);
            else if (auto table = insert_query->getTable(); !table.empty())
                insert_query->table_id = context->resolveStorageID(StorageID{insert_query->getDatabase(), table});

            if (insert_query->table_id)
                if (auto table = DatabaseCatalog::instance().tryGetTable(insert_query->table_id, context))
                    async_insert_enabled |= table->areAsynchronousInsertsEnabled();
        }

        if (insert_query && insert_query->select)
        {
            /// Prepare Input storage before executing interpreter if we already got a buffer with data.
            if (insert_query->tail)
            {
                ASTPtr input_function;
                insert_query->tryFindInputFunction(input_function);
                if (input_function)
                {
                    StoragePtr storage = context->executeTableFunction(input_function, insert_query->select->as<ASTSelectQuery>());
                    auto & input_storage = dynamic_cast<StorageInput &>(*storage);
                    auto input_metadata_snapshot = input_storage.getInMemoryMetadataPtr();

                    auto pipe = getSourceFromASTInsertQuery(
                        out_ast, true, input_metadata_snapshot->getSampleBlock(), context, input_function);

                    input_storage.setPipe(std::move(pipe));
                }
            }

            insert_query->tail.reset();
        }
        else
        {
            /// reset Input callbacks if query is not INSERT SELECT
            context->resetInputCallbacks();
        }

        StreamLocalLimits limits;
        std::shared_ptr<const EnabledQuota> quota;
        std::unique_ptr<IInterpreter> interpreter;

        bool async_insert = false;
        auto * queue = context->tryGetAsynchronousInsertQueue();
        auto logger = getLogger("executeQuery");

        if (insert_query && async_insert_enabled)
        {
            String reason;

            if (!queue)
                reason = "asynchronous insert queue is not configured";
            else if (insert_query->select)
                reason = "insert query has select";
            else if (insert_query->hasInlinedData())
                async_insert = true;

            if (!reason.empty())
                LOG_DEBUG(logger, "Setting async_insert=1, but INSERT query will be executed synchronously (reason: {})", reason);
        }


        bool quota_checked = false;

        if (async_insert)
        {
            if (context->getCurrentTransaction() && settings[Setting::throw_on_unsupported_query_inside_transaction])
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async inserts inside transactions are not supported");
            if (settings[Setting::implicit_transaction] && settings[Setting::throw_on_unsupported_query_inside_transaction])
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async inserts with 'implicit_transaction' are not supported");

            auto quorum_is_enabled = settings[Setting::insert_quorum].valueOr(0) > 1 || settings[Setting::insert_quorum].is_auto;
            if (quorum_is_enabled && !settings[Setting::insert_quorum_parallel])
                throw Exception(
                    ErrorCodes::UNSUPPORTED_PARAMETER,
                    "Async inserts with quorum only make sense with enabled insert_quorum_parallel setting, either disable quorum or set insert_quorum_parallel=1 or do not use async inserts");

            quota = context->getQuota();
            if (quota)
            {
                quota_checked = true;
                quota->used(QuotaType::QUERY_INSERTS, 1);
                quota->used(QuotaType::QUERIES, 1);
                quota->checkExceeded(QuotaType::ERRORS);
            }

            /// Invoke HTTP 100-Continue callback after async insert quota checks are completed
            if (http_continue_callback && !internal)
                http_continue_callback();

            auto result = queue->pushQueryWithInlinedData(out_ast, context);

            if (result.status == AsynchronousInsertQueue::PushResult::OK)
            {
                if (settings[Setting::wait_for_async_insert])
                {
                    auto timeout = settings[Setting::wait_for_async_insert_timeout].totalMilliseconds();
                    auto source = std::make_shared<WaitForAsyncInsertSource>(std::move(result.future), timeout);
                    res.pipeline = QueryPipeline(Pipe(std::move(source)));
                    res.pipeline.complete(std::make_shared<NullOutputFormat>(std::make_shared<const Block>(Block())));
                }

                const auto & table_id = insert_query->table_id;
                if (!table_id.empty())
                    context->setInsertionTable(table_id);
            }
            else if (result.status == AsynchronousInsertQueue::PushResult::TOO_MUCH_DATA)
            {
                async_insert = false;

                if (insert_query->data)
                {
                    /// Reset inlined data because it will be
                    /// available from tail read buffer.
                    insert_query->end = insert_query->data;
                    insert_query->data = nullptr;
                }

                insert_query->tail = std::move(result.insert_data_buffer);
                LOG_DEBUG(logger, "Setting async_insert=1, but INSERT query will be executed synchronously because it has too much data");
            }
        }

        if (!async_insert && async_insert_enabled)
        {
            /// Invoke HTTP 100-Continue callback if it was not invoked yet
            if (http_continue_callback && !internal)
                http_continue_callback();
        }

        QueryResultCachePtr query_result_cache = context->getQueryResultCache();
        const bool can_use_query_result_cache = query_result_cache != nullptr && settings[Setting::use_query_cache] && !internal
            && client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY
            && (out_ast->as<ASTSelectQuery>() || out_ast->as<ASTSelectWithUnionQuery>());
        QueryResultCacheUsage query_result_cache_usage = QueryResultCacheUsage::None;

        /// Bug 67476: If the query runs with a non-THROW overflow mode and hits a limit, the query result cache will store a truncated
        /// result (if enabled). This is incorrect. Unfortunately it is hard to detect from the perspective of the query result cache that
        /// the query result is truncated. Therefore throw an exception, to notify the user to disable either the query result cache or use
        /// another overflow mode.
        if (settings[Setting::use_query_cache] && (settings[Setting::read_overflow_mode] != OverflowMode::THROW
            || settings[Setting::read_overflow_mode_leaf] != OverflowMode::THROW
            || settings[Setting::group_by_overflow_mode] != OverflowMode::THROW
            || settings[Setting::sort_overflow_mode] != OverflowMode::THROW
            || settings[Setting::result_overflow_mode] != OverflowMode::THROW
            || settings[Setting::timeout_overflow_mode] != OverflowMode::THROW
            || settings[Setting::set_overflow_mode] != OverflowMode::THROW
            || settings[Setting::join_overflow_mode] != OverflowMode::THROW
            || settings[Setting::transfer_overflow_mode] != OverflowMode::THROW
            || settings[Setting::distinct_overflow_mode] != OverflowMode::THROW))
            throw Exception(ErrorCodes::QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE, "use_query_cache and overflow_mode != 'throw' cannot be used together");

        /// If the query runs with "use_query_cache = 1", we first probe if the query result cache already contains the query result (if
        /// yes: return result from cache). If doesn't, we execute the query normally and write the result into the query result cache. Both
        /// steps use a hash of the AST, the current database and the settings as cache key. Unfortunately, the settings are in some places
        /// internally modified between steps 1 and 2 (= during query execution) - this is silly but hard to forbid. As a result, the hashes
        /// no longer match and the cache is rendered ineffective. Therefore make a copy of the settings and use it for steps 1 and 2.
        std::optional<Settings> settings_copy;
        if (can_use_query_result_cache)
            settings_copy = settings;

        if (!async_insert)
        {
            /// If it is a non-internal SELECT, and passive (read) use of the query result cache is enabled, and the cache knows the query,
            /// then set a pipeline with a source populated by the query result cache.
            auto get_result_from_query_result_cache = [&]()
            {
                if (out_ast && can_use_query_result_cache && settings[Setting::enable_reads_from_query_cache])
                {
                    QueryResultCache::Key key(out_ast, context->getCurrentDatabase(), *settings_copy, context->getCurrentQueryId(), context->getUserID(), context->getCurrentRoles());
                    QueryResultCacheReader reader = query_result_cache->createReader(key);
                    if (reader.hasCacheEntryForKey())
                    {
                        result_details.query_cache_entry_created_at = reader.entryCreatedAt();
                        result_details.query_cache_entry_expires_at = reader.entryExpiresAt();

                        QueryPipeline pipeline;
                        pipeline.readFromQueryResultCache(reader.getSource(), reader.getSourceTotals(), reader.getSourceExtremes());
                        res.pipeline = std::move(pipeline);
                        query_result_cache_usage = QueryResultCacheUsage::Read;

                        return true;
                    }
                }
                return false;
            };

            if (!get_result_from_query_result_cache())
            {
                /// We need to start the (implicit) transaction before getting the interpreter as this will get links to the latest snapshots
                if (!context->getCurrentTransaction() && settings[Setting::implicit_transaction] && !(out_ast && out_ast->as<ASTTransactionControl>()))
                {
                    try
                    {
                        if (context->isGlobalContext())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot create transactions");

                        implicit_tcl_executor->begin(context);
                    }
                    catch (Exception & e)
                    {
                        e.addMessage("while starting a transaction with 'implicit_transaction'");
                        throw;
                    }
                }

                if (settings[Setting::enable_shared_storage_snapshot_in_query])
                {
                    query_metadata_cache = std::make_shared<QueryMetadataCache>();
                    context->setQueryMetadataCache(query_metadata_cache);
                }

                if (out_ast)
                    interpreter = InterpreterFactory::instance().get(out_ast, context, SelectQueryOptions(stage).setInternal(internal));

                const auto & query_settings = context->getSettingsRef();
                if (interpreter && context->getCurrentTransaction() && query_settings[Setting::throw_on_unsupported_query_inside_transaction])
                {
                    if (!interpreter->supportsTransactions())
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported for this type of query ({})", out_ast->getID());

                    if (query_settings[Setting::apply_mutations_on_fly])
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported with enabled setting 'apply_mutations_on_fly'");
                }

                // InterpreterSelectQueryAnalyzer does not build QueryPlan in the constructor.
                // We need to force to build it here to check if we need to ignore quota.
                if (auto * interpreter_with_analyzer = dynamic_cast<InterpreterSelectQueryAnalyzer *>(interpreter.get()))
                    interpreter_with_analyzer->getQueryPlan();

                if (!(interpreter && interpreter->ignoreQuota()) && !quota_checked)
                {
                    quota = context->getQuota();
                    if (quota)
                    {
                        if (query_plan || out_ast->as<ASTSelectQuery>() || out_ast->as<ASTSelectWithUnionQuery>())
                        {
                            quota->used(QuotaType::QUERY_SELECTS, 1);
                        }
                        else if (out_ast->as<ASTInsertQuery>())
                        {
                            quota->used(QuotaType::QUERY_INSERTS, 1);
                        }
                        quota->used(QuotaType::QUERIES, 1);
                        quota->checkExceeded(QuotaType::ERRORS);
                    }
                }

                /// Invoke HTTP 100-Continue callback after quota checks are completed
                if (http_continue_callback && !internal)
                    http_continue_callback();

                if (interpreter)
                {
                    if (!interpreter->ignoreLimits())
                    {
                        limits.mode = LimitsMode::LIMITS_CURRENT;
                        limits.size_limits = SizeLimits(settings[Setting::max_result_rows], settings[Setting::max_result_bytes], settings[Setting::result_overflow_mode]);
                    }

                    if (auto * create_interpreter = typeid_cast<InterpreterCreateQuery *>(interpreter.get()))
                    {
                        create_interpreter->setIsRestoreFromBackup(flags.distributed_backup_restore);
                        create_interpreter->setInternal(internal);
                    }

                    std::unique_ptr<OpenTelemetry::SpanHolder> span;
                    if (OpenTelemetry::CurrentContext().isTraceEnabled())
                    {
                        auto * raw_interpreter_ptr = interpreter.get();
                        String class_name = raw_interpreter_ptr ? demangle(typeid(*raw_interpreter_ptr).name()) : "QueryPlan";
                        span = std::make_unique<OpenTelemetry::SpanHolder>(class_name + "::execute()");
                    }

                    res = interpreter->execute();

                    /// If it is a non-internal SELECT query, and active (write) use of the query result cache is enabled, then add a
                    /// processor on top of the pipeline which stores the result in the query result cache.
                    if (can_use_query_result_cache && settings[Setting::enable_writes_to_query_cache])
                    {
                        /// Only use the query result cache if the query does not contain non-deterministic functions or system tables (which are typically non-deterministic)

                        const bool ast_contains_nondeterministic_functions = astContainsNonDeterministicFunctions(out_ast, context);
                        const bool ast_contains_system_tables = astContainsSystemTables(out_ast, context);

                        const QueryResultCacheNondeterministicFunctionHandling nondeterministic_function_handling
                            = settings[Setting::query_cache_nondeterministic_function_handling];
                        const QueryResultCacheSystemTableHandling system_table_handling = settings[Setting::query_cache_system_table_handling];

                        if (ast_contains_nondeterministic_functions && nondeterministic_function_handling == QueryResultCacheNondeterministicFunctionHandling::Throw)
                            throw Exception(ErrorCodes::QUERY_CACHE_USED_WITH_NONDETERMINISTIC_FUNCTIONS,
                                "The query result was not cached because the query contains a non-deterministic function."
                                " Use setting `query_cache_nondeterministic_function_handling = 'save'` or `= 'ignore'` to cache the query result regardless or to omit caching");

                        if (ast_contains_system_tables && system_table_handling == QueryResultCacheSystemTableHandling::Throw)
                            throw Exception(ErrorCodes::QUERY_CACHE_USED_WITH_SYSTEM_TABLE,
                                "The query result was not cached because the query contains a system table."
                                " Use setting `query_cache_system_table_handling = 'save'` or `= 'ignore'` to cache the query result regardless or to omit caching");

                        if ((!ast_contains_nondeterministic_functions || nondeterministic_function_handling == QueryResultCacheNondeterministicFunctionHandling::Save)
                            && (!ast_contains_system_tables || system_table_handling == QueryResultCacheSystemTableHandling::Save))
                        {
                            auto created_at = std::chrono::system_clock::now();
                            auto expires_at = created_at + std::chrono::seconds(settings[Setting::query_cache_ttl].totalSeconds());

                            QueryResultCache::Key key(
                                out_ast, context->getCurrentDatabase(), *settings_copy, res.pipeline.getSharedHeader(),
                                context->getCurrentQueryId(),
                                context->getUserID(), context->getCurrentRoles(),
                                settings[Setting::query_cache_share_between_users],
                                created_at, expires_at,
                                settings[Setting::query_cache_compress_entries]);

                            const size_t num_query_runs = settings[Setting::query_cache_min_query_runs] ? query_result_cache->recordQueryRun(key) : 1; /// try to avoid locking a mutex in recordQueryRun()
                            if (num_query_runs <= settings[Setting::query_cache_min_query_runs])
                            {
                                LOG_TRACE(getLogger("QueryResultCache"),
                                    "Skipped insert because the query ran {} times but the minimum required number of query runs to cache the query result is {}",
                                    num_query_runs, settings[Setting::query_cache_min_query_runs].value);
                            }
                            else
                            {
                                auto query_result_cache_writer = std::make_shared<QueryResultCacheWriter>(query_result_cache->createWriter(
                                     key,
                                     std::chrono::milliseconds(settings[Setting::query_cache_min_query_duration].totalMilliseconds()),
                                     settings[Setting::query_cache_squash_partial_results],
                                     settings[Setting::max_block_size],
                                     settings[Setting::query_cache_max_size_in_bytes],
                                     settings[Setting::query_cache_max_entries]));
                                res.pipeline.writeResultIntoQueryResultCache(query_result_cache_writer);
                                query_result_cache_usage = QueryResultCacheUsage::Write;
                            }

                            /// We will expose the info in HTTP headers, but only if the cache is enabled for reading (otherwise browsers should not cache either)
                            /// Set only "expires_at", not "Age" as the entry has not aged at this moment in time.
                            if (settings[Setting::enable_reads_from_query_cache])
                                result_details.query_cache_entry_expires_at = expires_at;
                        }
                    }
                }
            }
        }

        if (process_list_entry)
        {
            /// Query was killed before execution
            if (process_list_entry->getQueryStatus()->isKilled())
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                    "Query '{}' is killed in pending state", process_list_entry->getQueryStatus()->getInfo().client_info.current_query_id);
        }

        /// Hold element of process list till end of query execution.
        res.process_list_entries.push_back(process_list_entry);

        /// Hold query metadata cache till end of query execution.
        res.query_metadata_cache = std::move(query_metadata_cache);

        if (query_plan)
        {
            auto plan = QueryPlan::makeSets(std::move(*query_plan), context);

            plan.resolveStorages(context);
            plan.optimize(QueryPlanOptimizationSettings(context));

            WriteBufferFromOwnString buf;
            plan.explainPlan(buf, {.header=true, .actions=true});
            LOG_TRACE(getLogger("executeQuery"), "Deserialized Query Plan:\n{}", buf.str());

            auto pipeline = plan.buildQueryPipeline(
                    QueryPlanOptimizationSettings(context),
                    BuildQueryPipelineSettings(context),
                    /*do_optimize=*/ false);

            res.pipeline = QueryPipelineBuilder::getPipeline(std::move(*pipeline));
        }

        auto & pipeline = res.pipeline;

        if (pipeline.pulling() || pipeline.completed())
        {
            /// Limits on the result, the quota on the result, and also callback for progress.
            /// Limits apply only to the final result.
            pipeline.setProgressCallback(context->getProgressCallback());
            pipeline.setProcessListElement(context->getProcessListElement());
            if (stage == QueryProcessingStage::Complete && pipeline.pulling())
                pipeline.setLimitsAndQuota(limits, quota);
        }
        else if (pipeline.pushing())
        {
            pipeline.setProcessListElement(context->getProcessListElement());
        }

        /// Everything related to query log.
        {
            QueryLogElement elem = logQueryStart(
                query_start_time,
                context,
                query_for_logging,
                normalized_query_hash,
                out_ast,
                pipeline,
                interpreter.get(),
                internal,
                query_database,
                query_table,
                async_insert);

            /// Also make possible for caller to log successful query finish and exception during execution.

            /// The prepare callback flushes pipeline progress and resets the pipeline
            auto finish_callback_finalize_pipeline = [
                                     query_result_cache_usage,
                                     // Need to be cached, since will be changed after complete()
                                     pulling_pipeline = pipeline.pulling()](QueryPipeline && query_pipeline) mutable -> QueryPipelineFinalizedInfo
            {
                return finalizeQueryPipelineBeforeLogging(std::move(query_pipeline), query_result_cache_usage, pulling_pipeline);
            };

            /// The finish callback logs the query result
            auto finish_callback = [elem,
                                    context,
                                    out_ast,
                                    query_result_cache_usage,
                                    internal,
                                    implicit_tcl_executor,
                                    // Need to be cached, since will be changed after complete()
                                    pulling_pipeline = pipeline.pulling(),
                                    query_span](const QueryPipelineFinalizedInfo & query_pipeline_finalized_info, std::chrono::system_clock::time_point finish_time) mutable
            {
                logQueryFinishImpl(elem, context, out_ast, query_pipeline_finalized_info, pulling_pipeline, query_span, query_result_cache_usage, internal, finish_time);

                if (implicit_tcl_executor->transactionRunning())
                {
                    implicit_tcl_executor->commit(context);
                }
            };

            auto exception_callback =
                [start_watch, elem, context, out_ast, internal, my_quota(quota), implicit_tcl_executor, query_span](bool log_error) mutable
            {
                if (implicit_tcl_executor->transactionRunning())
                {
                    implicit_tcl_executor->rollback(context);
                }
                else if (auto txn = context->getCurrentTransaction())
                {
                    txn->onException();
                }

                /// If a query with internal query fails, only add one error to the quota.
                if (!internal)
                {
                    if (my_quota)
                        my_quota->used(QuotaType::ERRORS, 1, /* check_exceeded = */ false);
                }

                logQueryException(elem, context, start_watch, out_ast, query_span, internal, log_error);
            };

            res.finalize_query_pipeline = std::move(finish_callback_finalize_pipeline);
            res.finish_callbacks.push_back(std::move(finish_callback));
            res.exception_callbacks.push_back(std::move(exception_callback));
        }
    }
    catch (...)
    {
        if (implicit_tcl_executor->transactionRunning())
        {
            implicit_tcl_executor->rollback(context);
        }
        else if (auto txn = context->getCurrentTransaction())
        {
            txn->onException();
        }

        logExceptionBeforeStart(query_for_logging, normalized_query_hash, context, out_ast, query_span, start_watch.elapsedMilliseconds(), internal);

        throw;
    }

    return res;
}


std::pair<std::shared_ptr<QueryFuzzer>, std::unique_lock<std::mutex>> getGlobalASTFuzzer()
{
    static std::mutex mutex;
    static std::shared_ptr<QueryFuzzer> fuzzer = std::make_shared<QueryFuzzer>(randomSeed());
    return {fuzzer, std::unique_lock(mutex)};
}


static bool isReadOnlyQuery(const ASTPtr & ast)
{
    auto kind = ast->getQueryKind();
    return kind == IAST::QueryKind::Select
        || kind == IAST::QueryKind::Explain
        || kind == IAST::QueryKind::Show
        || kind == IAST::QueryKind::Describe
        || kind == IAST::QueryKind::Exists;
}


static void executeASTFuzzerQueries(const ASTPtr & ast, const ContextMutablePtr & context, Float64 ast_fuzzer_runs_value, bool any_query)
{
    if (!any_query && !isReadOnlyQuery(ast))
        return;

    size_t num_runs = static_cast<size_t>(ast_fuzzer_runs_value);
    double fractional = ast_fuzzer_runs_value - static_cast<double>(num_runs);
    if (fractional > 0)
    {
        std::bernoulli_distribution dist(fractional);
        if (dist(thread_local_rng))
            ++num_runs;
    }

    if (num_runs == 0)
        return;

    auto logger = getLogger("ASTFuzzer");

    ASTPtr base_ast = ast;

    for (size_t i = 0; i < num_runs; ++i)
    {
        ASTPtr fuzzed_ast;
        {
            auto [fuzzer, lock] = getGlobalASTFuzzer();
            fuzzed_ast = base_ast->clone();
            fuzzer->fuzzMain(fuzzed_ast);
        }

        WriteBufferFromOwnString fuzzed_query_buf;
        fuzzed_ast->format(fuzzed_query_buf, IAST::FormatSettings(/*one_line=*/true));
        String fuzzed_query = fuzzed_query_buf.str();

        if (fuzzed_query.size() > 10000)
        {
            LOG_TRACE(logger, "Fuzzed query too long ({} chars), skipping", fuzzed_query.size());
            continue;
        }

        ProfileEvents::increment(ProfileEvents::ASTFuzzerQueries);
        LOG_TRACE(logger, "Fuzzed query: {}", fuzzed_query);

        try
        {
            /// Reset the transaction (if any), it is stored in session and local context (see InterpreterTransactionControlQuery::executeBegin())
            context->getQueryContext()->getSessionContext()->setCurrentTransaction(NO_TRANSACTION_PTR);
            context->setCurrentTransaction(NO_TRANSACTION_PTR);

            auto fuzz_session_context = Context::createCopy(context);
            fuzz_session_context->makeSessionContext();

            auto fuzz_context = Context::createCopy(fuzz_session_context);
            fuzz_context->makeQueryContext();
            fuzz_context->setSetting("ast_fuzzer_runs", Field(Float64(0)));
            fuzz_context->setCurrentQueryId("");

            auto result = executeQuery(fuzzed_query, fuzz_context, QueryFlags{.internal = true});

            if (result.second.pipeline.initialized())
            {
                if (result.second.pipeline.pushing())
                {
                    /// Cannot execute pushing pipelines (e.g. INSERT) without providing input data, just cancel.
                    result.second.pipeline.cancel();
                }
                else
                {
                    if (result.second.pipeline.pulling())
                    {
                        result.second.pipeline.complete(std::make_shared<NullOutputFormat>(std::make_shared<const Block>(result.second.pipeline.getHeader())));
                    }
                    CompletedPipelineExecutor executor(result.second.pipeline);
                    executor.execute();
                }
            }

            base_ast = fuzzed_ast;
        }
        catch (...)
        {
            LOG_TRACE(logger, "Fuzzed query failed: {}", getCurrentExceptionMessage(/*with_stacktrace=*/false));
            auto [fuzzer, lock] = getGlobalASTFuzzer();
            fuzzer->notifyQueryFailed(fuzzed_ast);
        }
    }
}


std::pair<ASTPtr, BlockIO> executeQuery(
    const String & query,
    ContextMutablePtr context,
    QueryFlags flags,
    QueryProcessingStage::Enum stage)
{
    if (isCrashed())
        throw Exception(ErrorCodes::ABORTED, "The server is shutting down due to a fatal error");

    ProfileEvents::checkCPUOverload(context->getServerSettings()[ServerSetting::os_cpu_busy_time_threshold],
            context->getSettingsRef()[Setting::min_os_cpu_wait_time_ratio_to_throw],
            context->getSettingsRef()[Setting::max_os_cpu_wait_time_ratio_to_throw],
            /*should_throw*/ true);

    ASTPtr ast;
    BlockIO res;
    auto implicit_tcl_executor = std::make_shared<ImplicitTransactionControlExecutor>();
    ReadBufferUniquePtr no_input_buffer;
    QueryResultDetails result_details;
    res = executeQueryImpl(query.data(), query.data() + query.size(), context, flags, stage, no_input_buffer, ast, implicit_tcl_executor, {}, result_details);
    if (const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
    {
        String format_name = ast_query_with_output->format_ast
                ? getIdentifierName(ast_query_with_output->format_ast)
                : context->getDefaultFormat();

        const bool ignore_null_for_explain = context->getSettingsRef()[Setting::ignore_format_null_for_explain];
        if (boost::iequals(format_name, "Null") && !(ast->as<ASTExplainQuery>() && ignore_null_for_explain))
            res.null_format = true;
    }

    /// The 'SYSTEM ENABLE FAILPOINT terminate_with_exception' query itself should succeed.
    if (ast && !ast->as<ASTSystemQuery>())
    {
        fiu_do_on(FailPoints::terminate_with_exception,
        {
            try
            {
                throw Exception(ErrorCodes::FAULT_INJECTED, "Failpoint terminate_with_exception");
            }
            catch (...)
            {
                std::terminate();
            }
        });

        fiu_do_on(FailPoints::terminate_with_std_exception,
        {
            try
            {
                throw std::runtime_error("Failpoint terminate_with_std_exception");
            }
            catch (...)
            {
                std::terminate();
            }
        });

        fiu_do_on(FailPoints::libcxx_hardening_out_of_bounds_assertion,
        {
            std::vector<int> v;
            (void)v[0];
        });
    }

    if (!flags.internal && ast)
    {
        Float64 ast_fuzzer_runs_value = context->getSettingsRef()[Setting::ast_fuzzer_runs];
        if (ast_fuzzer_runs_value > 0)
        {
            bool any_query = context->getSettingsRef()[Setting::ast_fuzzer_any_query];
            res.finish_callbacks.emplace_back(
                [ast, context, ast_fuzzer_runs_value, any_query](const QueryPipelineFinalizedInfo &, std::chrono::system_clock::time_point)
                {
                    try
                    {
                        executeASTFuzzerQueries(ast, context, ast_fuzzer_runs_value, any_query);
                    }
                    catch (...)
                    {
                        tryLogCurrentException("ASTFuzzer");
                    }
                });
        }
    }

    return std::make_pair(std::move(ast), std::move(res));
}

void executeQuery(
    ReadBuffer & istr,
    WriteBuffer & ostr,
    ContextMutablePtr context,
    SetResultDetailsFunc set_result_details,
    QueryFlags flags,
    const std::optional<FormatSettings> & output_format_settings,
    HandleExceptionInOutputFormatFunc handle_exception_in_output_format,
    QueryFinishCallback query_finish_callback,
    HTTPContinueCallback http_continue_callback)
{
    executeQuery(
        wrapReadBufferReference(istr), ostr, context, std::move(set_result_details), flags,
        output_format_settings, std::move(handle_exception_in_output_format), std::move(query_finish_callback), std::move(http_continue_callback));
}

void executeQuery(
    ReadBufferUniquePtr istr,
    WriteBuffer & ostr,
    ContextMutablePtr context,
    SetResultDetailsFunc set_result_details,
    QueryFlags flags,
    const std::optional<FormatSettings> & output_format_settings,
    HandleExceptionInOutputFormatFunc handle_exception_in_output_format,
    QueryFinishCallback query_finish_callback,
    HTTPContinueCallback http_continue_callback)
{
    if (isCrashed())
        throw Exception(ErrorCodes::ABORTED, "The server is shutting down due to a fatal error");

    PODArray<char> parse_buf;
    const char * begin;
    const char * end;

    try
    {
        istr->nextIfAtEnd();
    }
    catch (...)
    {
        /// If buffer contains invalid data and we failed to decompress, we still want to have some information about the query in the log.
        logQuery("<cannot parse>", context, /* internal = */ false, QueryProcessingStage::Complete);
        throw;
    }

    size_t max_query_size = context->getSettingsRef()[Setting::max_query_size];

    ProfileEvents::checkCPUOverload(context->getServerSettings()[ServerSetting::os_cpu_busy_time_threshold],
            context->getSettingsRef()[Setting::min_os_cpu_wait_time_ratio_to_throw],
            context->getSettingsRef()[Setting::max_os_cpu_wait_time_ratio_to_throw],
            /*should_throw*/ true);

    if (istr->available() > max_query_size || http_continue_callback)
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        /// Also, if the HTTP 100 Continue response is deferred (which is the case if http_continue_callback is set),
        /// we should not attempt to read anything from the body. We expect the query (without insert data) to be present
        /// in the buffer already because it should have been extracted from the query parameter.
        begin = istr->position();
        end = istr->buffer().end();
        istr->position() += end - begin;
    }
    else
    {
        /// FIXME: this is an extra copy not required for async insertion.

        /// If not - copy enough data into 'parse_buf'.
        WriteBufferFromVector<PODArray<char>> out(parse_buf);
        LimitReadBuffer limit(*istr, {.read_no_more = max_query_size + 1});
        copyData(limit, out);
        out.finalize();

        begin = parse_buf.data();
        end = begin + parse_buf.size();
    }

    QueryResultDetails result_details
    {
        .query_id = context->getClientInfo().current_query_id,
        .timezone = DateLUT::instance().getTimeZone(),
    };

    /// Set the result details in case of any exception raised during query execution
    SCOPE_EXIT({
        /// Either the result_details have been set in the flow below or the caller of this function does not provide this callback
        if (!set_result_details)
            return;

        try
        {
            set_result_details(result_details);
        }
        catch (...)
        {
            /// This exception can be ignored.
            /// because if the code goes here, it means there's already an exception raised during query execution,
            /// and that exception will be propagated to outer caller,
            /// there's no need to report the exception thrown here.
        }
    });

    ASTPtr ast;
    BlockIO streams;
    String format_name;
    OutputFormatPtr output_format;

    auto update_format_on_exception_if_needed = [&]()
    {
        if (!output_format)
        {
            try
            {
                const ASTQueryWithOutput * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
                format_name = ast_query_with_output && ast_query_with_output->format_ast != nullptr
                    ? getIdentifierName(ast_query_with_output->format_ast)
                    : context->getDefaultFormat();

                output_format = FormatFactory::instance().getOutputFormat(format_name, ostr, {}, context, output_format_settings);
                if (output_format && output_format->supportsWritingException())
                {
                    /// Force an update of the headers before we start writing
                    result_details.content_type = FormatFactory::instance().getContentType(format_name, output_format_settings);
                    result_details.format = format_name;

                    fiu_do_on(FailPoints::execute_query_calling_empty_set_result_func_on_exception,
                    {
                        // emulate calling empty set_result_details() callback
                        throw std::bad_function_call{};
                    });

                    if (set_result_details)
                    {
                        /// reset set_result_details func to avoid calling in SCOPE_EXIT()
                        auto set_result_details_copy = set_result_details;
                        set_result_details = {};
                        set_result_details_copy(result_details);
                    }
                }
            }
            catch (const Exception & e)
            {
                /// Ignore this exception and report the original one
                LOG_WARNING(getLogger("executeQuery"), getExceptionMessageAndPattern(e, true));
            }
        }
    };
    auto implicit_tcl_executor = std::make_shared<ImplicitTransactionControlExecutor>();
    try
    {
        streams = executeQueryImpl(begin, end, context, flags, QueryProcessingStage::Complete, istr, ast, implicit_tcl_executor, http_continue_callback, result_details);
    }
    catch (...)
    {
        if (handle_exception_in_output_format)
        {
            update_format_on_exception_if_needed();
            if (output_format)
                handle_exception_in_output_format(*output_format, format_name, context, output_format_settings);
        }
        /// The timezone was already set before query was processed,
        /// But `session_timezone` setting could be modified in the query itself, so we update the value.
        result_details.timezone = DateLUT::instance().getTimeZone();
        throw;
    }

    /// The timezone was already set before query was processed,
    /// But `session_timezone` setting could be modified in the query itself, so we update the value.
    result_details.timezone = DateLUT::instance().getTimeZone();

    const Map & additional_http_headers = context->getSettingsRef()[Setting::http_response_headers].value;
    if (!additional_http_headers.empty())
    {
        for (const auto & key_value : additional_http_headers)
        {
            if (key_value.getType() != Field::Types::Tuple
                || key_value.safeGet<Tuple>().size() != 2)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of the `additional_http_headers` setting must be a Map");

            if (key_value.safeGet<Tuple>().at(0).getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The keys of the `additional_http_headers` setting must be Strings");

            if (key_value.safeGet<Tuple>().at(1).getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The values of the `additional_http_headers` setting must be Strings");

            String key = key_value.safeGet<Tuple>().at(0).safeGet<String>();
            String value = key_value.safeGet<Tuple>().at(1).safeGet<String>();

            if (std::find_if(key.begin(), key.end(), isControlASCII) != key.end()
                || std::find_if(value.begin(), value.end(), isControlASCII) != value.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The values of the `additional_http_headers` cannot contain ASCII control characters");

            if (!result_details.additional_headers.emplace(key, value).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "There are duplicate entries in the `additional_http_headers` setting");
        }
    }

    auto & pipeline = streams.pipeline;
    bool pulling_pipeline = pipeline.pulling();

    try
    {
        if (pipeline.pushing())
        {
            auto pipe = getSourceFromASTInsertQuery(ast, true, pipeline.getHeader(), context, nullptr);
            pipeline.complete(std::move(pipe));
        }
        else if (pipeline.pulling())
        {
            const ASTQueryWithOutput * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());
            format_name = ast_query_with_output && ast_query_with_output->format_ast != nullptr
                ? getIdentifierName(ast_query_with_output->format_ast)
                : context->getDefaultFormat();

            const bool ignore_null_for_explain = context->getSettingsRef()[Setting::ignore_format_null_for_explain];
            if (boost::iequals(format_name, "Null") && ast->as<ASTExplainQuery>() && ignore_null_for_explain)
                format_name = context->getDefaultFormat();

            WriteBuffer * out_buf = &ostr;
            if (ast_query_with_output && ast_query_with_output->out_file)
                throw Exception(ErrorCodes::INTO_OUTFILE_NOT_ALLOWED, "INTO OUTFILE is not allowed");

            output_format = FormatFactory::instance().getOutputFormatParallelIfPossible(
                format_name,
                *out_buf,
                materializeBlock(pipeline.getHeader()),
                context,
                output_format_settings);

            output_format->setAutoFlush();

            /// Save previous progress callback if any. TODO Do it more conveniently.
            auto previous_progress_callback = context->getProgressCallback();

            /// NOTE Progress callback takes shared ownership of 'out'.
            pipeline.setProgressCallback([output_format, previous_progress_callback] (const Progress & progress)
            {
                if (previous_progress_callback)
                    previous_progress_callback(progress);
                output_format->onProgress(progress);
            });

            result_details.content_type = FormatFactory::instance().getContentType(format_name, output_format_settings);
            result_details.format = format_name;

            pipeline.complete(output_format);
        }
        else
        {
            pipeline.setProgressCallback(context->getProgressCallback());
        }

        /// input stream might be consumed into some source proceccors/format readers
        /// that is the case with insert queries, but select quries could read from it but they do not take ownership of the input stream,
        /// here we reset it in order not to hold the reference to the input stream
        istr.reset();

        if (set_result_details)
        {
            /// The call of set_result_details itself might throw exception,
            /// in such case there's no need to call this function again in the SCOPE_EXIT defined above.
            /// So the callback is cleared before its execution.
            auto set_result_details_copy = set_result_details;
            set_result_details = nullptr;

            set_result_details_copy(result_details);
        }

        if (pipeline.initialized())
        {
            CompletedPipelineExecutor executor(pipeline);
            executor.execute();
        }
        else
        {
            /// It's possible to have queries without input and output.
        }

        if (!flags.internal && ast)
        {
            Float64 ast_fuzzer_runs_value = context->getSettingsRef()[Setting::ast_fuzzer_runs];
            if (ast_fuzzer_runs_value > 0)
            {
                bool any_query = context->getSettingsRef()[Setting::ast_fuzzer_any_query];
                try
                {
                    executeASTFuzzerQueries(ast, context, ast_fuzzer_runs_value, any_query);
                }
                catch (...)
                {
                    tryLogCurrentException("ASTFuzzer");
                }
            }
        }

    }
    catch (...)
    {
        /// first execute on exception callback, it includes updating query_log
        /// otherwise closing record ('ExceptionWhileProcessing') can be not appended in query_log
        /// due to possible exceptions in functions called below (passed as parameter here)
        streams.onException();

        if (handle_exception_in_output_format)
        {
            update_format_on_exception_if_needed();
            if (output_format)
                handle_exception_in_output_format(*output_format, format_name, context, output_format_settings);
        }
        throw;
    }

    /// Query with `implicit_transaction` is committed here because:
    /// 1. `onFinish` is invoked after the transaction is committed.
    /// 2. When handling HTTP requests, in `HTTPHandler::processQuery`, there is `query_finish_callback` which is invoked before `onFinish`.
    /// It releases the session and finalizes the output. The client might use the same session to query other queries. Hence, the transaction must be committed before `query_finish_callback`.
    /// Refer: https://github.com/ClickHouse/ClickHouse/issues/80428
    if (implicit_tcl_executor->transactionRunning())
        implicit_tcl_executor->commit(context);

    /// We release query slot here to make sure client can safely reuse the slot with his next query, otherwise it will be released too late by BlockIO.
    context->releaseQuerySlot();

    /// The order is important here:
    /// - first we save the finish_time that will be used for the entry in query_log/opentelemetry_span_log.finish_time_us
    /// - then we flush the progress (to flush result_rows/result_bytes)
    /// - then call the query_finish_callback() - right now the only purpose is to flush the data over HTTP
    /// - then call onFinish() that will create entry in query_log/opentelemetry_span_log
    ///
    /// That way we have:
    /// - correct finish time of the query (regardless of how long does the query_finish_callback() takes)
    /// - correct progress for HTTP (X-ClickHouse-Summary requires result_rows/result_bytes)
    /// - correct NetworkSendElapsedMicroseconds/NetworkSendBytes in query_log
    const auto finish_time = std::chrono::system_clock::now();
    std::exception_ptr exception_ptr;
    if (query_finish_callback)
    {
        /// Dump result_rows/result_bytes, since query_finish_callback() will send final http header.
        flushQueryProgress(pipeline, pulling_pipeline, context->getProgressCallback(), context->getProcessListElement());

        try
        {
            query_finish_callback();
        }
        catch (...)
        {
            exception_ptr = std::current_exception();
        }
    }

    streams.onFinish(finish_time);

    if (exception_ptr)
        std::rethrow_exception(exception_ptr);
}

void executeTrivialBlockIO(BlockIO & streams, ContextPtr context, bool with_interactive_cancel)
{
    try
    {
        if (!streams.pipeline.initialized())
            return;

        if (!streams.pipeline.completed())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query pipeline requires output, but no output buffer provided, it's a bug");

        streams.pipeline.setProgressCallback(context->getProgressCallback());

        CompletedPipelineExecutor executor(streams.pipeline);

        if (auto callback = context->getInteractiveCancelCallback(); callback && with_interactive_cancel)
        {
            auto interactive_delay = context->getSettingsRef()[Setting::interactive_delay];
            executor.setCancelCallback(std::move(callback), interactive_delay / 1000);
        }

        executor.execute();
    }
    catch (...)
    {
        streams.onException();
        throw;
    }

    streams.onFinish();
}

}
