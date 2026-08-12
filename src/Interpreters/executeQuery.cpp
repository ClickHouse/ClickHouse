#include <Common/DateLUTImpl.h>
#include <Common/CurrentMetrics.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadGroupSwitcher.h>
#include <Common/Logger.h>
#include <Common/StringUtils.h>
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
#include <Common/quoteString.h>
#include <Common/Stopwatch.h>

#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>
#include <IO/ReadBuffer.h>
#include <IO/copyData.h>

#include <QueryPipeline/BlockIO.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>
#include <Processors/Formats/Impl/NullFormat.h>

#include <Parsers/ASTBackupQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTWithElement.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTDeleteQuery.h>
#include <Parsers/ASTHypotheticalIndexQuery.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTUndropQuery.h>
#include <Parsers/ASTOptimizeQuery.h>
#include <Parsers/ASTUpdateQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/TablePropertiesQueriesASTs.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTFromJSON.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryNormalization.h>
#include <Parsers/toOneLineQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Parsers/Polyglot/ParserPolyglotQuery.h>
#include <Parsers/Kusto/parseKQLQuery.h>
#include <Parsers/Prometheus/ParserPrometheusQuery.h>

#include <Formats/FormatFactory.h>
#include <Storages/StorageInput.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/AccessRightsElement.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledQuota.h>
#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>
#include <Interpreters/InterpreterExplainQuery.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterTransactionControlQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/QueryLog.h>
#include <IO/AsyncReadCounters.h>
#include <Interpreters/QueryMetricLog.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/getTableExpressions.h>
#include <Interpreters/misc.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/InDepthNodeVisitor.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Interpreters/MergeTreeTransaction/VersionMetadata.h>
#include <Interpreters/QueryMetadataCache.h>
#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Parsers/ASTSystemQuery.h>
#include <Parsers/stripQuerySettings.h>
#include <QueryPipeline/printPipeline.h>
#include <IO/Progress.h>
#include <Parsers/ASTIdentifier_fwd.h>
#if CLICKHOUSE_CLOUD
#include <Common/Licensing/LicenseChecker.h>
#endif
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>

#include <IO/CompressionMethod.h>

#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>

#include <unordered_map>
#include <unordered_set>
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
    extern const Event InsertQuery;
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
    extern const Event ASTFuzzerSkippedBackupRestore;
    extern const Event ASTFuzzerSkippedReplicatedDDLInternal;
    extern const Event QueryParseMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric IsServerShuttingDown;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool enable_json_ast_dialect;
    extern const SettingsBool allow_experimental_kusto_dialect;
    extern const SettingsBool allow_experimental_polyglot_dialect;
    extern const SettingsBool allow_experimental_prql_dialect;
    extern const SettingsBool allow_materialized_view_with_bad_select;
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool create_index_ignore_unique;
    extern const SettingsDefaultTableEngine default_table_engine;
    extern const SettingsDefaultTableEngine default_temporary_table_engine;
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
    extern const SettingsUInt64 output_format_compression_level;
    extern const SettingsString polyglot_dialect;
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
    extern const SettingsBool reattach_tables_before_query_execution;
    extern const SettingsFloat reattach_tables_before_query_execution_probability;
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
    extern const int QUERY_CACHE_USED_WITH_NON_THROW_OVERFLOW_MODE;
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
    extern const int UNKNOWN_TABLE;
}

namespace FailPoints
{
    extern const char execute_query_calling_empty_set_result_func_on_exception[];
    extern const char terminate_with_exception[];
    extern const char terminate_with_std_exception[];
    extern const char libcxx_hardening_out_of_bounds_assertion[];
    extern const char trigger_sanitizer_error[];
}

static TSA_NO_THREAD_SAFETY_ANALYSIS void triggerSanitizerError()
{
#if defined(ADDRESS_SANITIZER)
    const auto data = std::make_unique_for_overwrite<char[]>(16);
    [[maybe_unused]] volatile char c = data[16];
#elif defined(THREAD_SANITIZER)
    std::mutex mutex;
    mutex.unlock();
#elif defined(MEMORY_SANITIZER)
    const auto data = std::make_unique_for_overwrite<char[]>(16);
    if (data[7] == 42)
        __builtin_trap();
#endif
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
    if (info.profile_counters)
        element.profile_counters = *info.profile_counters;

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
        element.used_row_policies.insert(access_info.row_policies.begin(), access_info.row_policies.end());
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

    if (auto async_read_counters = context_ptr->getAsyncReadCounters())
    {
        auto add_counter = [&](const char * name, size_t value)
        {
            if (value)
                element.async_read_counters.emplace(name, value);
        };
        add_counter("max_parallel_read_tasks", async_read_counters->max_parallel_read_tasks.load(std::memory_order_relaxed));
        add_counter("max_parallel_prefetch_tasks", async_read_counters->max_parallel_prefetch_tasks.load(std::memory_order_relaxed));
        add_counter("total_prefetch_tasks", async_read_counters->total_prefetch_tasks.load(std::memory_order_relaxed));
    }
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
    bool log_as_internal,
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

    elem.is_internal = log_as_internal;

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
            elem.used_row_policies = info.row_policies;
        }

        if (async_insert)
            InterpreterInsertQuery::extendQueryLogElemImpl(elem, context);
        else if (interpreter)
            interpreter->extendQueryLogElem(elem, query_ast, context, query_database, query_table);

        if (settings[Setting::log_query_settings])
            elem.query_settings = context->getSettingsRef().changedToMap();

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

            query_log->add([&](QueryLogElement & e) { e = elem; });
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

static void logQueryMetricLogFinish(ContextPtr context, bool internal, String query_id, std::chrono::system_clock::time_point finish_time, QueryStatusInfoPtr info)
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

static QueryPipelineFinalizedInfo finalizeQueryPipelineBeforeLogging(QueryPipeline && query_pipeline, QueryResultCacheUsage /*query_result_cache_usage*/, bool pulling_pipeline)
{
    /// Trigger the actual write of the buffered query result into the query result cache. This is done explicitly to
    /// prevent partial/garbage results in case of exceptions during query execution.
    /// Always called (it's a no-op if no cache writers exist in the pipeline), because subqueries may have
    /// opted in to caching via explicit SETTINGS use_query_cache = true even when the outer query doesn't use the cache.
    query_pipeline.finalizeWriteInQueryResultCache();

    VectorWithMemoryTracking<IProcessor::ProcessorsProfileLogInfo> processors_profile_infos = getProcessorsProfileLogInfo(query_pipeline.getProcessors());

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

static void logQueryFinishImpl(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const ASTPtr & query_ast,
    const QueryPipelineFinalizedInfo & query_pipeline_finalized_info,
    bool pulling_pipeline,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    QueryResultCacheUsage query_result_cache_usage,
    bool internal,
    bool log_as_internal,
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
                "Read {} rows, {} in {:.3f} sec., {:.3f} rows/sec., {}/sec.",
                elem.read_rows,
                ReadableSize(elem.read_bytes),
                elapsed_seconds,
                rows_per_second,
                ReadableSize(bytes_per_second));
        }

        context->getRuntimeFilterLookup()->logStats();

        elem.query_result_cache_usage = query_result_cache_usage;

        elem.is_internal = log_as_internal;

        if (log_queries && elem.type >= settings[Setting::log_queries_min_type]
            && static_cast<Int64>(elem.query_duration_ms) >= settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
        {
            if (auto query_log = context->getQueryLog())
                query_log->add([&](QueryLogElement & e) { e = elem; });
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
    bool internal,
    bool log_as_internal)
{
    const auto time_now = std::chrono::system_clock::now();
    auto query_pipeline_finalized_info = finalizeQueryPipelineBeforeLogging(std::move(query_pipeline), query_result_cache_usage, pulling_pipeline);
    logQueryFinishImpl(elem, context, query_ast, query_pipeline_finalized_info, pulling_pipeline, query_span, query_result_cache_usage, internal, log_as_internal, time_now);
}

/// Bump the FailedQuery / FailedInsertQuery / FailedSelectQuery family of ProfileEvents.
/// Shared between `logQueryException` (failures during execution) and `logExceptionBeforeStart`
/// (failures before execution starts) so the two paths never drift.
static void incrementFailedQueryProfileEvents(const ASTPtr & ast, const ClientInfo & client_info, bool internal)
{
    ProfileEvents::increment(ProfileEvents::FailedQuery);
    if (!ast || ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
        ProfileEvents::increment(ProfileEvents::FailedSelectQuery);
    else if (ast->as<ASTInsertQuery>())
        ProfileEvents::increment(ProfileEvents::FailedInsertQuery);

    if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
    {
        ProfileEvents::increment(ProfileEvents::FailedInitialQuery);
        if (!ast || ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInitialSelectQuery);
    }

    if (internal)
    {
        ProfileEvents::increment(ProfileEvents::FailedInternalQuery);
        if (!ast || ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInternalSelectQuery);
        else if (ast->as<ASTInsertQuery>())
            ProfileEvents::increment(ProfileEvents::FailedInternalInsertQuery);
    }
}

void logQueryException(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const Stopwatch & start_watch,
    const ASTPtr & query_ast,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    bool internal,
    bool log_as_internal,
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

    incrementFailedQueryProfileEvents(query_ast, context->getClientInfo(), internal);

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

    elem.is_internal = log_as_internal;

    if (settings[Setting::calculate_text_stack_trace] && log_error)
        elem.stack_trace = getExceptionStackTraceString(std::current_exception());
    logException(context, elem, log_error);

    /// In case of exception we log internal queries also
    if (log_queries && elem.type >= settings[Setting::log_queries_min_type]
        && static_cast<Int64>(elem.query_duration_ms) >= settings[Setting::log_queries_min_query_duration_ms].totalMilliseconds())
    {
        if (auto query_log = context->getQueryLog())
            query_log->add([&](QueryLogElement & e) { e = elem; });
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
    bool internal,
    bool log_as_internal)
{
    auto query_end_time = std::chrono::system_clock::now();

    /// Exception before the query execution.
    if (auto quota = context->getQuota())
        quota->usedForQuery(normalized_query_hash, QuotaType::ERRORS, 1, /* check_exceeded = */ false);

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
        elem.query_settings = settings.changedToMap();

    if (settings[Setting::calculate_text_stack_trace])
        elem.stack_trace = getExceptionStackTraceString(std::current_exception());

    elem.is_internal = log_as_internal;

    bool log_error = elem.exception_code != ErrorCodes::QUERY_WAS_CANCELLED_BY_CLIENT && elem.exception_code !=  ErrorCodes::QUERY_WAS_CANCELLED;
    logException(context, elem, log_error);

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    incrementFailedQueryProfileEvents(ast, context->getClientInfo(), internal);

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

            query_log->add([&](QueryLogElement & e) { e = elem; });
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

/// Remove the resource-limit settings that executeASTFuzzerQueries pins on the fuzz context from the
/// query-level SETTINGS carriers of the fuzzed AST. These caps (row/time/memory/result/block-size
/// limits) keep a single fuzzed query from running away. They are applied to the fuzz context up front, but
/// executeQueryImpl re-applies the query's own SETTINGS on top of the context
/// (InterpreterSetQuery::applySettingsFromQuery), so a seed or fuzzed `SETTINGS max_rows_to_read = 0`
/// (or `= DEFAULT`, which resets the cap back to its unbounded default), including from a BACKUP or
/// CREATE clause, would otherwise silently lift the guard. Stripping them from the AST before
/// formatting makes the fuzz-context values authoritative.
/// removeSettingsFromQuery covers exactly the carriers applySettingsFromQuery reads, and also prunes
/// any SETTINGS clause that becomes empty, so a clause holding only these caps does not re-serialize
/// to a bare `SETTINGS` keyword (which would throw on re-parse and make the fuzzer silently skip the
/// query instead of running it under the caps).
static void stripFuzzerSafetyLimitSettings(const ASTPtr & ast)
{
    static constexpr std::string_view limit_settings[] = {
        "max_rows_to_read",
        "read_overflow_mode",
        "max_execution_time",
        "max_memory_usage",
        "max_result_rows",
        "max_result_bytes",
        "max_block_size",
        "min_insert_block_size_rows",
    };

    removeSettingsFromQuery(ast, limit_settings);
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

namespace
{

/// The object kind a query reference demands from the table it names. `EXISTS VIEW` / `SHOW CREATE VIEW`
/// and `EXISTS DICTIONARY` / `SHOW CREATE DICTIONARY` are metadata probes for a specific object kind: on a
/// name that resolves to a plain table they answer `0` or fail without ever touching that table's storage,
/// so reattaching the table first would be a side effect the outer query never implies. The reattach loop
/// skips a collected table whose resolved storage does not match the expected kind.
enum class ExpectedObjectKind : uint8_t
{
    Any,
    View,
    Dictionary,
};

struct CollectTablesData
{
    struct CollectedTable
    {
        StorageID id;
        /// The access the outer query is going to check on this table when its interpreter runs
        /// (recorded by the collector from the AST shape that referenced the table). Used by the
        /// preflight in `reattachTablesUsedInQuery` to keep access-rejected queries side-effect free.
        AccessFlags required_access;
        /// Whether the outer query fails when this table does not exist (recorded by the collector from
        /// the AST shape that referenced the table). Used by the existence preflight in
        /// `reattachTablesUsedInQuery` to keep queries referencing a missing table side-effect free.
        bool existence_required = true;
        /// The object kind the reference demands (recorded by the collector from the AST shape); the
        /// reattach loop skips the table when its resolved storage is not of this kind.
        ExpectedObjectKind expected_kind = ExpectedObjectKind::Any;
    };

    explicit CollectTablesData(ContextPtr context_) : context(std::move(context_)) {}

    const ContextPtr context;
    std::vector<CollectedTable> tables;

    /// Every expression alias declared anywhere in the query (`WITH expr AS name`, `SELECT expr AS name`,
    /// ...). Used to keep the bare-identifier right-hand side of `IN` from being mistaken for a table:
    /// unlike a `FROM` reference, an `IN` right-hand side resolves an expression alias in preference to a
    /// same-named table (`WITH (1, 2) AS rhs SELECT 1 IN rhs` never reads the table `rhs`). The set is
    /// query-wide rather than per-scope on purpose — a name collision only makes the collector skip a
    /// table, which is the safe direction for this hook.
    std::unordered_set<String> alias_names;

    void addTableIfNotEmpty(const String & database, const String & table, const std::unordered_set<String> & active_ctes, Context::StorageNamespace resolve_namespace, const AccessFlags & required_access, bool existence_required = true, ExpectedObjectKind expected_kind = ExpectedObjectKind::Any)
    {
        if (table.empty())
            return;

        /// Unqualified reference matching an in-scope CTE name: it refers to the CTE, not a real table.
        if (database.empty() && active_ctes.contains(table))
            return;

        StorageID storage_id = database.empty()
            ? StorageID("", table)
            : StorageID(database, table);

        /// Note: this resolves only the names (e.g. substitutes the current database) — it does not check
        /// that the table exists in the catalog. Existence is checked by the existence preflight in
        /// `reattachTablesUsedInQuery`, which is why `existence_required` is carried in `CollectedTable`.
        /// `resolve_namespace` mirrors the namespace the outer query's interpreter resolves this reference
        /// in: `ResolveAll` where a same-named session temporary table shadows the persistent one (so the
        /// reference resolves to the temporary table and is skipped below, exactly as the query never
        /// touches the persistent table), `ResolveOrdinary` for carriers whose interpreter looks the name
        /// up only in the persistent catalog (so a same-named temporary table must NOT hide the persistent
        /// table the query actually uses) — see `mainTableResolveNamespace` and the per-call-site notes in
        /// `collectTablesInQuery`.
        auto resolved = context->tryResolveStorageID(storage_id, resolve_namespace);
        if (!resolved)
            return;

        /// Skip temporary and external tables — detaching them makes no sense.
        if (resolved.getDatabaseName() == DatabaseCatalog::TEMPORARY_DATABASE)
            return;

        tables.emplace_back(CollectedTable{std::move(resolved), required_access, existence_required, expected_kind});
    }
};

/// The access the outer query will require on the table referenced by `ast` (one of the
/// `ASTQueryWithTableAndOutput` family), mirroring the checks the corresponding interpreters perform.
/// Where the exact requirement depends on execution-time details (or the query class is not enumerated
/// here), returns all table-level flags: over-requiring only makes the preflight skip randomization,
/// never lets a query that would fail its access check produce `DETACH`/`ATTACH` side effects.
AccessFlags requiredAccessForTableQuery(const IAST & ast)
{
    if (ast.as<ASTShowCreateTableQuery>() || ast.as<ASTShowCreateViewQuery>())
        return AccessType::SHOW_COLUMNS;
    if (ast.as<ASTShowCreateDictionaryQuery>() || ast.as<ASTExistsDictionaryQuery>())
        return AccessType::SHOW_DICTIONARIES;
    if (ast.as<ASTExistsTableQuery>() || ast.as<ASTExistsViewQuery>())
        return AccessType::SHOW_TABLES;
    if (ast.as<ASTCheckTableQuery>())
        return AccessType::CHECK;
    if (ast.as<ASTOptimizeQuery>())
        return AccessType::OPTIMIZE;
    /// `InterpreterAlterQuery::getRequiredAccessForCommand` checks per-command flags that are not all inside
    /// the `ALTER` group: `ATTACH PARTITION` needs `INSERT`, `REPLACE PARTITION ... FROM src` needs
    /// `ALTER_DELETE | INSERT` on the target (plus `SELECT` on `src`), and `MOVE PARTITION ... TO TABLE dst`
    /// needs `INSERT` on `dst`. Requiring only `ALTER` here would under-approximate, so a user who has `ALTER`
    /// but lacks e.g. `INSERT` would pass the preflight and get a real `DETACH`/`ATTACH` before the outer
    /// `ALTER` fails with `ACCESS_DENIED`. Over-approximate with all table-level flags instead — over-requiring
    /// only makes the preflight skip randomization, it never produces side effects for a failing query. The
    /// extra source/destination tables (`from_*`/`to_*`) are folded into the collection in `collectTablesInQuery`.
    if (ast.as<ASTAlterQuery>())
        return AccessFlags::allFlagsGrantableOnTableLevel();
    /// `InterpreterUpdateQuery::execute` governs the `UPDATE ... SET _row_exists = 0` lightweight-delete form by
    /// `ALTER_DELETE` (and by `ALTER_DELETE | ALTER_UPDATE` when it also assigns other columns) on
    /// MergeTree-family tables, where `_row_exists` is the hidden virtual marker. Whether that shortcut applies
    /// depends on execution-time details (the resolved storage's virtual columns), which are not available here,
    /// so requiring only `ALTER_UPDATE` would under-approximate and let a user who has `ALTER_UPDATE` but not
    /// `ALTER_DELETE` get a real `DETACH`/`ATTACH` before the outer `UPDATE` fails with `ACCESS_DENIED`.
    /// Over-approximate with all table-level flags, exactly as for `ALTER` above.
    if (ast.as<ASTUpdateQuery>())
        return AccessFlags::allFlagsGrantableOnTableLevel();
    if (ast.as<ASTDeleteQuery>())
        return AccessType::ALTER_DELETE;
    if (ast.as<ASTWatchQuery>())
        return AccessType::SELECT;
    return AccessFlags::allFlagsGrantableOnTableLevel();
}

/// Whether the query fails when its main table does not exist. References for which a missing table is a
/// legitimate outcome keep the collector's ignore-on-miss behavior instead of raising
/// `has_unresolved_required_table`: `CREATE`/`ATTACH` name the table they are about to register (for
/// `CREATE OR REPLACE` an existing table is possible but not required), `EXISTS ...` answers `0` instead of
/// failing, `DROP`/`DETACH ... IF EXISTS` is a no-op on a missing table, and `UNDROP` names a table that is
/// currently dropped, hence never resolvable through the catalog. Everything else (e.g. `SHOW CREATE`,
/// `CHECK TABLE`, `OPTIMIZE`, `ALTER`, `TRUNCATE`, plain `DROP`) requires the table to exist.
bool mainTableExistenceRequired(const IAST & ast)
{
    if (ast.as<ASTCreateQuery>())
        return false;
    if (ast.as<ASTExistsTableQuery>() || ast.as<ASTExistsViewQuery>() || ast.as<ASTExistsDictionaryQuery>())
        return false;
    if (const auto * drop = ast.as<ASTDropQuery>())
        return !drop->if_exists;
    if (ast.as<ASTUndropQuery>())
        return false;
    return true;
}

/// Whether the query's interpreter actually touches an existing table its main-table reference names, so
/// detaching and attaching that table exercises a code path the query really takes. Plain `CREATE`/`ATTACH
/// TABLE dst` and `CREATE ... IF NOT EXISTS dst` never touch an existing `dst`: `InterpreterCreateQuery`
/// either throws `TABLE_ALREADY_EXISTS` or turns the statement into a no-op before reaching the table's
/// storage. The same holds for `UNDROP TABLE dst`: `InterpreterUndropQuery::executeToTable` throws
/// `TABLE_ALREADY_EXISTS` when an active `dst` already exists. A `DETACH`/`ATTACH` of such a target would
/// give a no-op or failing query a side effect on a table it never touches, breaking the side-effect-free
/// invariant this hook keeps for failing queries, so those targets are not eligible. The
/// `CREATE OR REPLACE`/`REPLACE` forms do replace an existing object, but even they validate the new
/// definition before the replacement path reaches it, so they cannot keep the target eligible either
/// (the tables a `CREATE` reads — the `AS src` source, the populating `SELECT` — are eligible or not
/// independently of its destination: see `createQueryStopsBeforeSources`, which suppresses them exactly
/// when the statement stops on the destination first). The index-management statements that also travel through
/// `ASTQueryWithTableAndOutput` are handled below for the same reason.
bool mainTableTouchedIfExists(const IAST & ast, const ContextPtr & context)
{
    if (ast.as<ASTCreateQuery>())
    {
        /// Even the replacing forms — the only `CREATE` shapes that touch an existing destination —
        /// validate the new definition before the replacement path ever reaches it. A source-carrying
        /// form analyzes its populating `SELECT` in `getTablePropertiesAndNormalizeCreateQuery` (so
        /// `CREATE OR REPLACE TABLE dst AS SELECT missing_col FROM src` throws with `dst` untouched) and
        /// validates an `AS src` source in `setEngine`; a source-less form can still be rejected there as
        /// incomplete — `CREATE OR REPLACE TABLE dst` with no column list throws `INCORRECT_QUERY`, and a
        /// bare definition may fail the engine's own requirements — again with `dst` untouched. Whether
        /// that validation passes cannot be predicted here, so every replacing destination conservatively
        /// stays out of scope — erring toward suppressing randomization for a succeeding statement rather
        /// than detaching the destination of a failing one.
        return false;
    }
    if (ast.as<ASTUndropQuery>())
        return false;
    /// `CREATE INDEX` reaches the table only through the `ALTER TABLE ... ADD INDEX` statement
    /// `InterpreterCreateIndexQuery::execute` rewrites it to, and it rewrites only after
    /// `validateCreateIndexQuery` accepts the statement. `CREATE UNIQUE INDEX` throws `NOT_IMPLEMENTED`
    /// unless `create_index_ignore_unique` is set, and `CREATE INDEX` without a `TYPE` either throws
    /// `INCORRECT_QUERY` or (with `allow_create_index_without_type`) returns an empty `BlockIO` — in all
    /// of those cases the statement fails or no-ops before touching the table. Unlike `DROP INDEX`, which
    /// always rewrites, `CREATE INDEX` is therefore eligible only in the shape that really rewrites.
    if (const auto * create_index = ast.as<ASTCreateIndexQuery>())
    {
        if (create_index->unique && !context->getSettingsRef()[Setting::create_index_ignore_unique])
            return false;
        const auto * index_decl = create_index->index_decl ? create_index->index_decl->as<ASTIndexDeclaration>() : nullptr;
        return index_decl && index_decl->getType();
    }
    /// `CREATE`/`DROP HYPOTHETICAL INDEX` never mutates the table it names:
    /// `InterpreterHypotheticalIndexQuery` only reads the table's metadata and updates the session-local
    /// `HypotheticalIndexStore`. Detaching and attaching the table would be a side effect on live table
    /// state that the query never changes (and `DROP HYPOTHETICAL INDEX ... IF EXISTS` may be a pure
    /// no-op), so these references are not eligible.
    if (ast.as<ASTHypotheticalIndexQuery>())
        return false;
    return true;
}

/// The access `InterpreterCreateQuery::getRequiredAccess` checks on the statement's own destination — the
/// database for `CREATE DATABASE`, the dictionary/view/table being created otherwise, plus the engine grant.
/// The external `TO`/target tables are deliberately left out: the collector records them separately, with
/// their own required access, so the generic access preflight covers them.
AccessRightsElements createQueryDestinationAccess(const ASTCreateQuery & create, const String & database, const String & table)
{
    AccessRightsElements required_access;

    if (!create.table)
        required_access.emplace_back(AccessType::CREATE_DATABASE, database);
    else if (create.is_dictionary)
        required_access.emplace_back(AccessType::CREATE_DICTIONARY, database, table);
    else if (create.isView())
    {
        if (create.replace_view)
            required_access.emplace_back(AccessType::DROP_VIEW | AccessType::CREATE_VIEW, database, table);
        else if (create.isTemporary())
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_VIEW);
        else
            required_access.emplace_back(AccessType::CREATE_VIEW, database, table);
    }
    else if (create.isTemporary())
    {
        /// The default engine for a temporary table is `Memory`, and `default_table_engine` does not apply.
        if (create.storage && create.storage->engine && create.storage->engine->name != "Memory")
            required_access.emplace_back(AccessType::CREATE_ARBITRARY_TEMPORARY_TABLE);
        else
            required_access.emplace_back(AccessType::CREATE_TEMPORARY_TABLE);
    }
    else
    {
        if (create.replace_table)
            required_access.emplace_back(AccessType::DROP_TABLE, database, table);
        required_access.emplace_back(AccessType::CREATE_TABLE, database, table);
    }

    if (create.storage && create.storage->engine)
        required_access.emplace_back(AccessType::TABLE_ENGINE, create.storage->engine->name);

    return required_access;
}

/// Whether a `CREATE` statement stops on its own destination before its interpreter ever touches the
/// tables the statement reads. Unlike the destination itself (handled by `mainTableTouchedIfExists`),
/// those source tables — the `AS src` structure source, the populating `SELECT`, the external `TO dst`
/// targets — are collected from the statement's other fields and children, so they need this separate
/// guard: `InterpreterCreateQuery::execute` checks the destination-side access first, and the plain-create
/// path then short-circuits on a taken destination name, both before reading any source. Without the
/// guard, `CREATE VIEW v AS SELECT * FROM src` by a user lacking `CREATE VIEW`, or
/// `CREATE TABLE IF NOT EXISTS existing AS SELECT * FROM src`, would `DETACH`/`ATTACH src` and only then
/// fail or no-op, breaking the side-effect-free invariant this hook keeps for such queries.
///
/// Like the preflights in `reattachTablesUsedInQuery`, this is a best-effort, point-in-time check, and it
/// errs toward suppressing the hook: an unresolvable destination counts as stopping the statement.
bool createQueryStopsBeforeSources(const ASTCreateQuery & create, const ContextPtr & context)
{
    String destination_database = create.getDatabase();
    String destination_table = create.getTable();

    /// Temporary tables and views live outside databases and are session-local, so
    /// `InterpreterCreateQuery::createTable` rejects the forms that contradict that at its very top —
    /// `ATTACH` of a temporary with `SYNTAX_ERROR`, a database-qualified temporary with
    /// `BAD_DATABASE_FOR_TEMPORARY_TABLE`, and an `ON CLUSTER` temporary with `INCORRECT_QUERY` — before
    /// `getTablePropertiesAndNormalizeCreateQuery` ever touches the `AS src` structure source or the
    /// populating `SELECT`. Mirror those guards so such a rejected statement does not reattach its sources
    /// on the way to the rejection.
    if (create.isTemporary() && (create.attach || create.database || !create.cluster.empty()))
        return true;

    /// A persistent destination is resolved in the ordinary namespace (`InterpreterCreateQuery` never
    /// resolves it against session temporary tables); a temporary one carries no database at all.
    if (create.table && !create.isTemporary())
    {
        auto resolved = context->tryResolveStorageID(
            destination_database.empty() ? StorageID("", destination_table) : StorageID(destination_database, destination_table),
            Context::ResolveOrdinary);
        if (!resolved)
            return true;
        destination_database = resolved.getDatabaseName();
        destination_table = resolved.getTableName();
    }

    if (!context->getAccess()->isGranted(createQueryDestinationAccess(create, destination_database, destination_table)))
        return true;

    /// `createQueryDestinationAccess` covers the `TABLE ENGINE` grant only for an engine spelled out in the
    /// statement, but `getTablePropertiesAndNormalizeCreateQuery` re-checks that grant after `setEngine` has
    /// inferred an engine for the engine-less forms — and it does so before the populating `SELECT` is
    /// analyzed. Mirror the inference for the shapes whose engine comes from a setting, read without touching
    /// any collected source: a temporary table takes `default_temporary_table_engine` (and `setEngine` rejects
    /// a `Replicated`/`Shared`/`KeeperMap` default for it outright), a plain table with no `AS src` structure
    /// source takes `default_table_engine`, and a `None` default makes `setEngine` throw `ENGINE_REQUIRED` —
    /// all before any source is read. The remaining engine-less shapes never reach that grant check before
    /// their sources: an engine inherited from an `AS src` source is read from that source's stored
    /// definition, a materialized view keeps its inner engine in the target clause (which the re-check does
    /// not cover), an `ATTACH` takes the engine from stored metadata, and dictionaries and ordinary/window
    /// views take no engine at all.
    if (create.table && !create.attach && !create.is_dictionary && !create.isView() && !create.as_table_function
        && !(create.storage && create.storage->engine))
    {
        std::optional<DefaultTableEngine> default_engine;
        if (create.isTemporary())
            default_engine = context->getSettingsRef()[Setting::default_temporary_table_engine].value;
        else if (create.as_table.empty())
            default_engine = context->getSettingsRef()[Setting::default_table_engine].value;

        if (default_engine)
        {
            if (*default_engine == DefaultTableEngine::None)
                return true;

            const String engine_name = SettingFieldDefaultTableEngine(*default_engine).toString();
            if (create.isTemporary()
                && (engine_name.starts_with("Replicated") || engine_name.starts_with("Shared") || engine_name == "KeeperMap"))
                return true;

            if (!context->getAccess()->isGranted(AccessType::TABLE_ENGINE, engine_name))
                return true;
        }
    }

    /// A stub `ATTACH` (no engine and no column list) applies the table definition from stored metadata
    /// and rejects any user-supplied clause it would otherwise silently drop — `AS src`, `AS SELECT`,
    /// `TO dst`, `EMPTY`, `CLONE`, engine-level clauses, and so on: `InterpreterCreateQuery::createTable`
    /// throws `BAD_ARGUMENTS` on such a statement before reading any source or target table. Mirror that
    /// guard (`has_dropped_clauses` there) so `ATTACH TABLE detached_dst AS live_src` or
    /// `ATTACH MATERIALIZED VIEW detached_mv TO dst AS SELECT * FROM src` does not reattach the tables
    /// collected from those fields on its way to the rejection.
    if (create.attach && (!create.storage || !create.storage->engine) && !create.columns_list)
    {
        bool has_dropped_clauses = false;

        if (create.storage)
        {
            const auto & storage = *create.storage;
            has_dropped_clauses
                = storage.partition_by != nullptr
                || storage.primary_key != nullptr
                || storage.order_by != nullptr
                || storage.sample_by != nullptr
                || storage.ttl_table != nullptr
                || storage.unique_key != nullptr
                || storage.settings != nullptr;
        }

        has_dropped_clauses = has_dropped_clauses
            || create.comment != nullptr
            || create.refresh_strategy != nullptr
            || create.sql_security != nullptr
            || create.select != nullptr
            || create.targets != nullptr
            || create.as_table_function != nullptr
            || create.aliases_list != nullptr
            || create.is_create_empty
            || create.is_clone_as
            || !create.as_database.empty()
            || !create.as_table.empty()
            || create.has_attach_from_path
            || create.has_uuid_clause
            || create.has_inner_uuid_clause;

        if (has_dropped_clauses)
            return true;
    }

    /// External `TimeSeries` targets (`CREATE TABLE ts ENGINE = TimeSeries SAMPLES samples_table TAGS
    /// tags_table ...`) are resolved and type-checked by `normalizeTimeSeriesDefinition`
    /// (`readTypesFromExternalTargets`) before the interpreter reads any source table — a missing or
    /// type-incompatible external `SAMPLES`/`TAGS` target makes the statement throw before its `AS src`
    /// structure source is touched. Whether an existing target passes that type check cannot be predicted
    /// here, so any external `SAMPLES`/`TAGS` target conservatively stops the statement — erring toward
    /// suppressing randomization for a succeeding statement rather than detaching a source of a failing
    /// one. `ATTACH` is exempt: it skips the external-target resolution (`check_external_targets` is
    /// false there), because the targets are allowed not to be loaded yet.
    if (!create.attach && create.targets)
        for (const auto & target : create.targets->targets)
            if ((target.kind == ViewTarget::Samples || target.kind == ViewTarget::Tags) && !target.table_id.table_name.empty())
                return true;

    /// The replacing forms really do replace an existing destination, so a taken name does not stop them.
    /// `CREATE DATABASE` has no destination table to collide with.
    if (!create.table || create.replace_table || create.replace_view || create.create_or_replace)
        return false;

    if (create.isTemporary())
        return static_cast<bool>(context->tryResolveStorageID(StorageID("", create.getTable()), Context::ResolveExternal));

    /// Probing the destination must itself be side-effect free: in databases that do not support
    /// detaching tables even `isTableExist` can act on behalf of the query — e.g.
    /// `DatabaseRemote::isTableExist` reaches out to the remote server under the caller's credentials
    /// and propagates transport/authentication failures. Whether such a statement stops before its
    /// sources cannot be predicted here, so conservatively report that it does — erring toward
    /// suppressing randomization for a succeeding statement rather than probing on its behalf.
    if (const auto database = DatabaseCatalog::instance().tryGetDatabase(destination_database);
        database && !database->supportsDetachingTables())
        return true;

    /// A taken destination name makes the statement throw `TABLE_ALREADY_EXISTS` (or, with
    /// `IF NOT EXISTS`, a pure no-op) before the populating `SELECT` runs — see the `is_plain_create`
    /// branch of `InterpreterCreateQuery::doCreateTableAsSelectViaTemporaryTable` and the existence check
    /// at the top of `InterpreterCreateQuery::doCreateTable`.
    if (DatabaseCatalog::instance().isTableExist(StorageID(destination_database, destination_table), context))
        return true;

    /// The name may also be reserved by a table in a detached state: its metadata file is present even
    /// though the table is not in the catalog. That stops a plain create in the same way. `ATTACH` is the
    /// exception — it is what such a metadata file is for.
    if (!create.attach)
    {
        if (auto database = DatabaseCatalog::instance().tryGetDatabase(destination_database))
        {
            try
            {
                database->checkMetadataFilenameAvailability(destination_table);
            }
            catch (const Exception &)
            {
                return true;
            }
        }
    }

    return false;
}

/// The object kind the query's main-table reference demands (see `ExpectedObjectKind`). Everything not
/// enumerated here — including `SHOW CREATE TABLE`, `EXISTS TABLE` and plain `DROP`/`DETACH TABLE`, which
/// accept any object kind — places no constraint on the resolved storage. The kind-specific `DROP`/`DETACH`
/// forms are constrained because `InterpreterDropQuery::executeToTableImpl` throws `INCORRECT_QUERY` on an
/// `is_view`/`is_dictionary` mismatch before touching the table's storage.
ExpectedObjectKind mainTableExpectedObjectKind(const IAST & ast)
{
    if (ast.as<ASTExistsViewQuery>() || ast.as<ASTShowCreateViewQuery>())
        return ExpectedObjectKind::View;
    if (ast.as<ASTExistsDictionaryQuery>() || ast.as<ASTShowCreateDictionaryQuery>())
        return ExpectedObjectKind::Dictionary;
    if (const auto * drop = ast.as<ASTDropQuery>())
    {
        if (drop->is_view)
            return ExpectedObjectKind::View;
        if (drop->is_dictionary)
            return ExpectedObjectKind::Dictionary;
    }
    return ExpectedObjectKind::Any;
}

/// The namespace the query's interpreter resolves its main-table reference in (for the non-`TEMPORARY`
/// forms — `... TEMPORARY TABLE t` references are skipped by the collector before resolution). This must
/// mirror the interpreter exactly: resolving here more broadly than the interpreter does would let a
/// same-named session temporary table hide a persistent table the query actually uses (the collector
/// resolves the temporary hit, drops it, and never records the persistent table), silently disabling the
/// hook for it; resolving more narrowly would detach a persistent table the query never touches.
///
/// `ResolveOrdinary` carriers (the interpreter looks the unqualified name up only in the persistent
/// catalog, via `Context::resolveDatabase` + catalog or an explicit `ResolveOrdinary`):
/// `EXISTS TABLE`/`EXISTS VIEW`/`EXISTS DICTIONARY` (`InterpreterExistsQuery`), `SHOW CREATE VIEW`/
/// `SHOW CREATE DICTIONARY` (`InterpreterShowCreateQuery`), `CREATE`/`ATTACH` targets
/// (`InterpreterCreateQuery`), `UNDROP` (`InterpreterUndropQuery`), `UPDATE` (`InterpreterUpdateQuery`),
/// `DELETE` (`InterpreterDeleteQuery`), and `WATCH` (`InterpreterWatchQuery`).
///
/// `ResolveAll` carriers (a session temporary table legitimately shadows the persistent one):
/// `SHOW CREATE TABLE` (`InterpreterShowCreateQuery`), `ALTER` (`InterpreterAlterQuery`), `OPTIMIZE`
/// (`InterpreterOptimizeQuery`), `CHECK TABLE` (`InterpreterCheckQuery`), and `DROP`/`DETACH`/`TRUNCATE`
/// (`InterpreterDropQuery` tries `ResolveExternal` first for an unqualified name).
Context::StorageNamespace mainTableResolveNamespace(const IAST & ast)
{
    if (ast.as<ASTExistsTableQuery>() || ast.as<ASTExistsViewQuery>() || ast.as<ASTExistsDictionaryQuery>()
        || ast.as<ASTShowCreateViewQuery>() || ast.as<ASTShowCreateDictionaryQuery>()
        || ast.as<ASTCreateQuery>() || ast.as<ASTUndropQuery>()
        || ast.as<ASTUpdateQuery>() || ast.as<ASTDeleteQuery>() || ast.as<ASTWatchQuery>())
        return Context::ResolveOrdinary;
    return Context::ResolveAll;
}

/// Collect the names of all expression aliases declared in the query (see `CollectTablesData::alias_names`).
void collectAliasNames(const ASTPtr & ast, std::unordered_set<String> & alias_names)
{
    if (!ast)
        return;

    if (const auto & alias = ast->tryGetAlias(); !alias.empty())
        alias_names.insert(alias);

    for (const auto & child : ast->children)
        collectAliasNames(child, alias_names);
}

/// Walk the AST and collect tables, tracking CTE scope so that an in-scope CTE name does not cause us to
/// treat an unqualified table reference of the same name as a real table. `active_ctes` is passed by value
/// so each `ASTSelectQuery`'s WITH names propagate only to its descendants, not to siblings or ancestors.
///
/// Scope — this defines the contract of `reattach_tables_before_query_execution`: tables are extracted
/// from `SELECT` (FROM/JOIN/IN, with the CTE shadowing rules below), `INSERT`, and the `ASTQueryWithTableAndOutput`
/// family (`SHOW CREATE TABLE`, `EXISTS TABLE`, `CHECK TABLE`, `OPTIMIZE`, `ALTER`, ...) — including the extra
/// source/destination tables an `ALTER ... REPLACE/ATTACH/MOVE PARTITION` names in its `from_*`/`to_*` fields
/// the `AS` source of a `CREATE ... AS src` and the external view targets of a `CREATE ... TO dst`
/// — see the `ASTQueryWithTableAndOutput` branch. Query classes that
/// keep table references in other AST shapes — `SHOW COLUMNS`, `SHOW INDEXES`, `DESCRIBE`, `RENAME`/`EXCHANGE`,
/// and all `BACKUP` and `RESTORE` forms — are deliberately not covered: `RENAME` and `EXCHANGE`
/// manipulate the tables' catalog registration themselves; `BACKUP` and `RESTORE` can both fail before ever
/// touching the named local table, so reattaching it here would give a failing query a `DETACH`/`ATTACH`
/// side effect on a table it never touches. For `RESTORE TABLE`, `RestorerFromBackup::run` first resolves
/// the source objects inside the backup (`findDatabasesAndTablesInBackup`) and only later reaches the local
/// destination, so a restore whose source entry is missing from the backup never touches an existing
/// destination table. For `BACKUP TABLE`, `BackupsWorker::BackupStarter::doBackup` opens and validates the
/// destination (`openBackupForWriting`) before it builds `BackupEntriesCollector`, so a backup with an
/// invalid destination — an unknown disk, a bad path, an already existing backup — fails before the source
/// table is ever read; proving the destination valid here would require opening the backup in this
/// collector. The whole-database/whole-server forms additionally name no explicit table and expand into
/// per-table work only during execution. Broadening the randomized `DETACH`/`ATTACH` to those queries would
/// add churn to the test suite without exercising the data-integrity paths this testing hook targets.
///
/// Only the `WITH name AS (subquery)` form (`ASTWithElement`) shadows a table identifier in FROM. A scalar
/// `WITH expr AS alias` binding (e.g. `WITH 1 AS t`) does not: `WITH 1 AS t SELECT * FROM t` reads the table
/// `t`. A CTE's own name is also not active while walking its own definition body, because the analyzer hides
/// only the CTE currently being resolved (`WITH t AS (SELECT * FROM t) ...` reads the real table `t` inside).
/// The bare-identifier right-hand side of `IN` follows the opposite rule — any expression alias wins there —
/// and is handled separately through `CollectTablesData::alias_names`.
void collectTablesInQuery(const ASTPtr & ast, CollectTablesData & data, std::unordered_set<String> active_ctes)
{
    if (!ast)
        return;

    if (const auto * select = ast->as<ASTSelectQuery>())
    {
        const ASTPtr with = select->with();

        /// Collect only the real CTE names declared in this select's WITH clause. Only the
        /// `WITH name AS (subquery)` form (`ASTWithElement`) introduces a name that shadows a table
        /// identifier in FROM. A scalar `WITH expr AS alias` binding does not: `WITH 1 AS t SELECT * FROM t`
        /// still reads the table `t`, so such aliases must not be treated as shadowing CTE names.
        std::unordered_set<String> this_level_ctes;
        if (with)
        {
            for (const auto & with_child : with->children)
                if (const auto * with_element = with_child->as<ASTWithElement>())
                    this_level_ctes.insert(with_element->name);

            /// Walk each WITH child (CTE definitions and scalar aliases). The analyzer hides only the CTE
            /// currently being resolved, so a CTE body may still reference a real table with the same name:
            /// `WITH t AS (SELECT * FROM t) SELECT * FROM t` reads the real table `t` inside the definition.
            /// Therefore the element's own name is not active while walking its own body, but its siblings are.
            ///
            /// `WITH RECURSIVE` is more subtle: the analyzer resolves the non-recursive *seed* term (the
            /// first `UNION` member) before the recursive temporary table exists, so the seed reads a real
            /// table of the same name; only the recursive members (after the first) resolve the name through
            /// the recursive temporary table. So the element's own name must shadow references inside the
            /// recursive members (otherwise `WITH RECURSIVE t AS (SELECT 1 UNION ALL SELECT ... FROM t) ...`
            /// would `DETACH`/`ATTACH` a table the query does not read), but must NOT shadow references in
            /// the seed term (otherwise `WITH RECURSIVE t AS (SELECT ... FROM t UNION ALL ...) ...` would
            /// miss the real table `t` that the seed term actually reads).
            for (const auto & with_child : with->children)
            {
                auto body_ctes = active_ctes;
                body_ctes.insert(this_level_ctes.begin(), this_level_ctes.end());

                const auto * with_element = with_child->as<ASTWithElement>();

                if (!select->recursive_with || !with_element)
                {
                    /// Non-recursive CTE (or a scalar `WITH expr AS alias` binding): the element's own name
                    /// does not shadow references inside its own body, because the analyzer hides only the
                    /// CTE currently being resolved.
                    if (with_element)
                        body_ctes.erase(with_element->name);
                    collectTablesInQuery(with_child, data, body_ctes);
                    continue;
                }

                /// `WITH RECURSIVE name AS (seed [UNION ALL recursive ...])`: locate the `UNION` member list
                /// so the seed term can be walked with `name` un-shadowed and the recursive members with
                /// `name` shadowed.
                const ASTSelectWithUnionQuery * union_query = nullptr;
                if (with_element->subquery)
                {
                    union_query = with_element->subquery->as<ASTSelectWithUnionQuery>();
                    if (!union_query)
                        for (const auto & sub_child : with_element->subquery->children)
                            if ((union_query = sub_child->as<ASTSelectWithUnionQuery>()))
                                break;
                }

                const ASTPtr members = union_query ? union_query->list_of_selects : nullptr;
                if (!members || members->children.empty())
                {
                    /// Unrecognized recursive body shape: conservatively keep the name shadowed across the
                    /// whole body (it is better to miss a real table than to detach one the query does not read).
                    collectTablesInQuery(with_child, data, body_ctes);
                    continue;
                }

                /// Seed (first) member: the name is not yet bound to the recursive temporary table, so a
                /// same-named real table read here is a real table the query reads — do not shadow it.
                auto seed_ctes = body_ctes;
                seed_ctes.erase(with_element->name);
                collectTablesInQuery(members->children.front(), data, seed_ctes);

                /// Recursive members: the name resolves to the recursive temporary table — keep it shadowed.
                for (size_t i = 1; i < members->children.size(); ++i)
                    collectTablesInQuery(members->children[i], data, body_ctes);
            }
        }

        /// For the rest of the query (FROM and below) all of this select's CTE names shadow table identifiers.
        active_ctes.insert(this_level_ctes.begin(), this_level_ctes.end());

        /// Walk the FROM table expressions directly so the original (un-resolved) database name is
        /// preserved. `getDatabaseAndTables` would substitute the current database for unqualified
        /// references, which defeats the CTE-name check: a CTE shadows only unqualified table references.
        for (const auto * table_expression : getTableExpressions(*select))
        {
            if (!table_expression || !table_expression->database_and_table_name)
                continue;
            if (const auto * id = table_expression->database_and_table_name->as<ASTTableIdentifier>())
                data.addTableIfNotEmpty(id->getDatabaseName(), id->shortName(), active_ctes, Context::ResolveAll, AccessType::SELECT);
        }

        /// Recurse into the remaining children with the CTE names active. The WITH subtree was already
        /// walked above with per-element scope, so skip it here to avoid re-adding the element's own name.
        for (const auto & child : ast->children)
        {
            if (child == with)
                continue;
            collectTablesInQuery(child, data, active_ctes);
        }
        return;
    }
    else if (const auto * insert = ast->as<ASTInsertQuery>())
    {
        /// `InterpreterInsertQuery::getTable` resolves the target with the default `ResolveAll`, so a
        /// session temporary table legitimately shadows a persistent one as the `INSERT` destination.
        data.addTableIfNotEmpty(insert->getDatabase(), insert->getTable(), active_ctes, Context::ResolveAll, AccessType::INSERT);
    }
    else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithTableAndOutput *>(ast.get()))
    {
        /// `... TEMPORARY TABLE t` (e.g. `EXISTS TEMPORARY TABLE t`, `DROP TEMPORARY TABLE t`,
        /// `SHOW CREATE TEMPORARY TABLE t`) names a session-local temporary table, and its name is
        /// unqualified. Resolving it through the persistent catalog would detach an unrelated persistent
        /// table of the same name that the query never touches, so skip temporary-table references.
        ///
        /// A `CREATE` that stops on its own destination — its destination-side access check fails, or the
        /// plain-create path short-circuits on a taken name — never reaches the tables it reads, so the
        /// whole statement is skipped here, children included (see `createQueryStopsBeforeSources`).
        if (const auto * create_query = ast->as<ASTCreateQuery>())
            if (createQueryStopsBeforeSources(*create_query, data.context))
                return;

        /// Targets whose query never touches an existing table of that name (plain `CREATE`/`ATTACH`,
        /// `CREATE ... IF NOT EXISTS`, `UNDROP`, the failing/no-op shapes of `CREATE INDEX`, and the
        /// session-local hypothetical-index statements — see `mainTableTouchedIfExists`) are skipped too:
        /// resolving them would detach a table the statement is about to fail on or no-op against.
        if (!query_with_output->isTemporary() && mainTableTouchedIfExists(*ast, data.context))
            data.addTableIfNotEmpty(query_with_output->getDatabase(), query_with_output->getTable(), active_ctes, mainTableResolveNamespace(*ast), requiredAccessForTableQuery(*ast), mainTableExistenceRequired(*ast), mainTableExpectedObjectKind(*ast));

        /// Some `ASTQueryWithTableAndOutput` classes reference additional real tables that live neither in the
        /// main `database`/`table` nor in child AST nodes, yet the outer query's own access check validates them
        /// at interpretation time. Collect those here too, so the access preflight stays complete and an
        /// access-rejected query never produces `DETACH`/`ATTACH` side effects (see the preflight in
        /// `reattachTablesUsedInQuery`). Their required access is over-approximated with all table-level flags —
        /// over-requiring only makes the preflight skip randomization, it never lets a failing query detach.
        if (const auto * alter = ast->as<ASTAlterQuery>())
        {
            /// `ALTER ... REPLACE PARTITION ... FROM src` / `ATTACH PARTITION ... FROM src` name a source table
            /// in `from_*`, and `MOVE PARTITION ... TO TABLE dst` names a destination table in `to_*` (see
            /// `InterpreterAlterQuery::getRequiredAccessForCommand`). These are plain strings in `ASTAlterCommand`,
            /// not child AST nodes, so the generic recursion below never reaches them.
            if (alter->command_list)
                for (const auto & command_child : alter->command_list->children)
                    if (const auto * command = command_child->as<ASTAlterCommand>())
                    {
                        /// `MergeTreeData::alterPartition` completes an unqualified `from_*`/`to_*` name
                        /// with the current database directly — no temporary-table shadowing — hence
                        /// `ResolveOrdinary`.
                        data.addTableIfNotEmpty(command->from_database, command->from_table, active_ctes, Context::ResolveOrdinary, AccessFlags::allFlagsGrantableOnTableLevel());
                        data.addTableIfNotEmpty(command->to_database, command->to_table, active_ctes, Context::ResolveOrdinary, AccessFlags::allFlagsGrantableOnTableLevel());
                    }
        }
        else if (const auto * create = ast->as<ASTCreateQuery>())
        {
            /// `CREATE ... AS src` / `CREATE ... CLONE AS src` reads `src`'s structure; `InterpreterCreateQuery`
            /// checks `SHOW_COLUMNS` on `create.as_database`/`create.as_table` before reading it. Without this,
            /// `CREATE OR REPLACE TABLE dst AS src` would detach an existing `dst` before that check fails. The
            /// source is a plain string in `ASTCreateQuery`, not a child AST node. The interpreter completes
            /// an unqualified source with `Context::resolveDatabase` and reads it from the persistent
            /// catalog — no temporary-table shadowing — hence `ResolveOrdinary`.
            data.addTableIfNotEmpty(create->as_database, create->as_table, active_ctes, Context::ResolveOrdinary, AccessFlags::allFlagsGrantableOnTableLevel());

            /// External view targets (`CREATE MATERIALIZED VIEW mv TO dst`, `CREATE WINDOW VIEW ... TO dst`,
            /// the `TimeSeries` targets, ...) live in `create.targets`, not in child AST nodes, and
            /// `InterpreterCreateQuery::getRequiredAccess` checks `SELECT | INSERT` on each of them. Without
            /// this, `CREATE MATERIALIZED VIEW mv TO dst AS SELECT * FROM src` would detach and attach `src`
            /// and only then fail on the missing access to `dst`. An inner (non-external) target carries no
            /// table name and is skipped by `addTableIfNotEmpty` naturally. The target is completed with the
            /// current database by `InterpreterCreateQuery` and never resolved against session temporary
            /// tables, hence `ResolveOrdinary`.
            ///
            /// Most targets are not required to exist, because a view may name a target that is created only
            /// later. The `TO dst` target of a `CREATE MATERIALIZED VIEW ... AS SELECT` is the exception:
            /// `InterpreterCreateQuery::validateMaterializedViewColumnsAndEngine` resolves it through
            /// `DatabaseCatalog::getTable` and rethrows unless `allow_materialized_view_with_bad_select` is
            /// set, and it does so before creating anything — so `CREATE MATERIALIZED VIEW mv TO missing_dst
            /// AS SELECT * FROM src` fails and must not detach `src` on the way. That validation runs for a
            /// `CREATE` only (`mode <= LoadingStrictnessLevel::CREATE`), hence the `attach` exclusion.
            if (create->targets)
            {
                const bool to_target_existence_required = create->is_materialized_view && create->select && !create->attach
                    && !data.context->getSettingsRef()[Setting::allow_materialized_view_with_bad_select];

                for (const auto & target : create->targets->targets)
                    data.addTableIfNotEmpty(
                        target.table_id.database_name, target.table_id.table_name, active_ctes,
                        Context::ResolveOrdinary, AccessType::SELECT | AccessType::INSERT,
                        /* existence_required */ target.kind == ViewTarget::To && to_target_existence_required);
            }
        }
    }
    else if (const auto * function = ast->as<ASTFunction>())
    {
        /// `IN table` / `GLOBAL IN table` (and the `NOT IN` / null-aware variants) keep the right-hand-side
        /// table as a bare identifier that is not part of the `FROM`/`JOIN` table expressions walked above,
        /// so collect it here (mirroring how `AddDefaultDatabaseVisitor` and `ActionsMatcher::makeSet` treat
        /// the second `IN` argument). A subquery right-hand side (`... IN (SELECT ...)`) is handled by the
        /// generic recursion into children below; a tuple/literal or table-function right-hand side names no
        /// table to detach. The referenced table needs `SELECT` just like a `FROM` table, so a user missing
        /// `SELECT` on it keeps the whole query side-effect free via the access preflight.
        ///
        /// An unqualified right-hand side that matches an expression alias declared in the query is not a
        /// table either: the analyzer resolves `rhs` in `WITH (1, 2) AS rhs SELECT 1 IN rhs` (and in
        /// `SELECT (1, 2) AS rhs, 5 IN rhs`) to the alias, so the same-named table is never read. Unlike a
        /// `FROM` reference — where a scalar alias does not shadow the table, hence the `ASTWithElement`-only
        /// rule for `active_ctes` above — the alias wins here, so consult `data.alias_names` as well.
        if (functionIsInOrGlobalInOperator(function->name) && function->arguments && function->arguments->children.size() == 2)
        {
            if (const auto * id = function->arguments->children[1]->as<ASTIdentifier>())
                if (auto table_id = id->createTable())
                    if (!table_id->getDatabaseName().empty() || !data.alias_names.contains(table_id->shortName()))
                        data.addTableIfNotEmpty(table_id->getDatabaseName(), table_id->shortName(), active_ctes, Context::ResolveAll, AccessType::SELECT);
        }
    }

    for (const auto & child : ast->children)
        collectTablesInQuery(child, data, active_ctes);
}

}

static void reattachTablesUsedInQuery(const ASTPtr & query, ContextMutablePtr context)
{
    CollectTablesData data(context);
    collectAliasNames(query, data.alias_names);
    collectTablesInQuery(query, data, /* active_ctes */ {});

    /// Deduplicate: the same table can appear multiple times (e.g. self-joins), possibly in contexts
    /// requiring different access (e.g. `INSERT INTO t SELECT ... FROM t`) — merge the required access.
    {
        std::unordered_map<StorageID, size_t, StorageID::DatabaseAndTableNameHash, StorageID::DatabaseAndTableNameEqual> index_in_deduped;
        std::vector<CollectTablesData::CollectedTable> deduped;
        deduped.reserve(data.tables.size());
        for (auto & table : data.tables)
        {
            auto [it, inserted] = index_in_deduped.emplace(table.id, deduped.size());
            if (inserted)
                deduped.push_back(std::move(table));
            else
            {
                deduped[it->second].required_access |= table.required_access;
                deduped[it->second].existence_required |= table.existence_required;
                /// References demanding different kinds can only coexist when at least one of them uses
                /// the table as a plain table, which legitimizes reattaching it — drop the constraint.
                if (deduped[it->second].expected_kind != table.expected_kind)
                    deduped[it->second].expected_kind = ExpectedObjectKind::Any;
            }
        }
        data.tables = std::move(deduped);
    }

    /// Existence preflight — keep queries referencing a missing table side-effect free. A required
    /// reference that is absent from the catalog means the query itself is going to fail (typically with
    /// `UNKNOWN_TABLE`) when its interpreter resolves the same name, so skip the hook for the whole query
    /// instead of first `DETACH`/`ATTACH`-ing the references that do exist (e.g. `src` in
    /// `SELECT * FROM src JOIN missing USING a` or `dst` in `CREATE OR REPLACE TABLE dst AS missing`).
    /// Optional references — ones the query is allowed to create or ignore, such as the target of a
    /// plain `CREATE` — legitimately may not exist and do not
    /// disable the hook; the reattach loop below skips them individually. Like the access preflight, this
    /// is a best-effort, point-in-time check: a table dropped concurrently after it is not caught here,
    /// and the loop below re-resolves each table before detaching it.
    ///
    /// The probe itself must be side-effect free, so databases that do not support detaching tables
    /// are never probed: in such databases even `isTableExist` can act on behalf of the query — e.g.
    /// `DatabaseRemote::isTableExist` reaches out to the remote server under the caller's credentials
    /// and propagates transport/authentication failures — while the reattach loop below skips their
    /// tables anyway. When such a reference is required, its existence cannot be verified here, so
    /// skip the hook for the whole query — erring toward skipping randomization, never toward
    /// producing side effects for a query that may be about to fail on the unverified reference.
    for (const auto & table : data.tables)
    {
        if (!table.existence_required)
            continue;
        if (const auto database = DatabaseCatalog::instance().tryGetDatabase(table.id.getDatabaseName());
            database && !database->supportsDetachingTables())
            return;
        if (!DatabaseCatalog::instance().isTableExist(table.id, context))
            return;
    }

    /// Access preflight — keep access-rejected queries side-effect free. The outer query's own access
    /// checks run only later, when its interpreter is constructed, so without this check a user who may
    /// `DETACH`/`ATTACH` a table but lacks the access the query itself needs on it (e.g. `SELECT`) would
    /// still trigger a real `DETACH`/`ATTACH` cycle and only then get `ACCESS_DENIED`. Require, for every
    /// collected table, the access recorded at collection time; if any check fails, skip the hook for the
    /// whole query — the missing access may concern a different table than the one to be detached.
    /// The preflight is scoped to what the AST-based collector sees: access enforced against objects that
    /// materialize only during execution (e.g. inner tables of views) cannot be validated here, and the
    /// table-level check is conservative with column-level grants (a user granted `SELECT` on a subset of
    /// columns fails it) — in all those cases the preflight errs toward skipping randomization, never
    /// toward producing side effects for a failing query.
    auto access = context->getAccess();
    for (const auto & table : data.tables)
        if (!access->isGranted(table.required_access, table.id.getDatabaseName(), table.id.getTableName()))
            return;

    for (const auto & collected : data.tables)
    {
        const auto & table_id = collected.id;
        if (table_id.getDatabaseName() == "system")
            continue;

        const auto & catalog = DatabaseCatalog::instance();

        /// Check the database before resolving the table: in databases that resolve tables dynamically,
        /// the resolution itself is not free of side effects — e.g. a `URL` database infers the table
        /// structure from the data and throws `ACCESS_DENIED` already in `tryGetTable` when the user
        /// lacks the read source grant (see `DatabaseURL::getTableImpl`), which would fail an outer
        /// query (e.g. `EXISTS TABLE`) that succeeds without the hook. All such databases do not
        /// support detaching tables, so resolving their tables is never needed here.
        const auto database = catalog.tryGetDatabase(table_id.getDatabaseName());
        if (!database || !database->supportsDetachingTables())
            continue;

        /// If table doesn't store data on disk, the data will be lost after detach.
        /// An action lock (e.g. from `SYSTEM STOP MERGES`) is held against the current storage object and
        /// is discarded by a `DETACH`/`ATTACH` cycle — exactly as by a manual `DETACH TABLE` + `ATTACH TABLE`.
        /// `hasAny` is a best-effort, point-in-time check that skips the common case (a test stopped some
        /// action before running queries); it is deliberately not atomic with the detach, so a lock installed
        /// concurrently after this check can still be lost. That matches manual `DETACH` semantics and is
        /// acceptable for this testing-only hook.
        auto table = catalog.tryGetTable(table_id, context);
        if (!table
            || !table->storesDataOnDisk()
            || context->getActionLocksManager()->hasAny(table))
            continue;

        /// A kind-specific metadata probe (`EXISTS VIEW`, `SHOW CREATE DICTIONARY`, ...) on a name that
        /// resolved to a different object kind never touches this table's storage — the outer query
        /// answers `0` or fails on the kind mismatch — so reattaching it would be a side effect the
        /// query never implies.
        if ((collected.expected_kind == ExpectedObjectKind::View && !table->isView())
            || (collected.expected_kind == ExpectedObjectKind::Dictionary && !table->isDictionary()))
            continue;

        /// Replicated tables (e.g. `ReplicatedMergeTree`) re-run their startup sequence on `ATTACH`,
        /// and that path can currently trip a broken-part invariant: `ReplicatedMergeTreeRestartingThread::tryStartup`
        /// calls `createLogEntriesToFetchBrokenParts`, which reaches `removePartAndEnqueueFetch(..., storage_init = true)`
        /// while the broken part is still in the working set and hits `chassert(!storage_init)`.
        /// Until that is hardened, skip replicated tables so this testing hook does not introduce a new
        /// startup exception instead of only exercising existing reattach safety.
        if (table->supportsReplication())
            continue;

        if (!catalog.getReferentialDependencies(table_id).empty()
            || !catalog.getReferentialDependents(table_id).empty()
            || !catalog.getLoadingDependencies(table_id).empty()
            || !catalog.getLoadingDependents(table_id).empty())
            continue;

        /// Tables with columns of dynamic structure (`Dynamic`, `JSON`, `Variant` and types containing them)
        /// can fail with logical errors during merges that race with DETACH/ATTACH, because part-level
        /// serialization metadata may differ between the original and re-attached table state.
        {
            bool has_dynamic_structure = false;
            const auto metadata_snapshot = table->getInMemoryMetadataPtr(context, false);
            for (const auto & column : metadata_snapshot->getColumns().getAllPhysical())
            {
                if (column.type->hasDynamicSubcolumns())
                {
                    has_dynamic_structure = true;
                    break;
                }
            }
            if (has_dynamic_structure)
                continue;
        }

        /// Tables with parts involved in `MergeTree` transactions are not safe to reattach yet.
        /// A part created (or being removed) by a still-running transaction keeps the storage
        /// referenced by that transaction until it commits or rolls back, so the internal
        /// `DETACH TABLE ... SYNC` below blocks until then — firing the hook on a non-transactional
        /// query while another session of a sequential test script holds such a part deadlocks the
        /// script (the other session cannot advance while this query is blocked). Independently,
        /// reloading parts with transactional version metadata on `ATTACH` is not hardened: CSN
        /// update and unknown-state resolution can race with the reattach and leave intersecting
        /// parts that fail the next server startup. Until that is hardened, skip tables having any
        /// active part involved in a transaction. Like the action-lock check above, this is a
        /// best-effort, point-in-time check.
        if (const auto * merge_tree = dynamic_cast<const MergeTreeData *>(table.get()))
        {
            bool has_transactional_parts = false;
            for (const auto & part : merge_tree->getDataPartsVectorForInternalUsage())
            {
                if (part->version->getInfo().wasInvolvedInTransaction())
                {
                    has_transactional_parts = true;
                    break;
                }
            }
            if (has_transactional_parts)
                continue;

            /// Tables with a TTL are not safe to reattach yet: `StorageMergeTree::scheduleDataProcessingJob`
            /// books a `max_number_of_merges_with_ttl_in_pool` slot when it selects a TTL merge, but the slot
            /// is released through the `MergeList` entry that `MergePlainMergeTreeTask::prepare` creates only
            /// when the task starts running. The internal `DETACH TABLE ... SYNC` below cancels still-queued
            /// background tasks (`removeTasksCorrespondingToStorage`), and `MergePlainMergeTreeTask::cancel`
            /// does not return the booked slot, so every cancelled selected-but-not-started TTL merge leaks a
            /// slot until server restart; two leaks disable TTL merges server-wide (the default limit is 2).
            /// Until that leak is fixed (https://github.com/ClickHouse/ClickHouse/pull/111925), skip tables
            /// whose metadata carries any TTL — TTL merges are only ever selected for those.
            const auto merge_tree_metadata = merge_tree->getInMemoryMetadataPtr(context, false);
            if (merge_tree_metadata->hasAnyTTL())
                continue;

            /// The `DETACH`/`ATTACH` cycle reloads the parts from disk, and an Outdated part that no Active
            /// part covers is reloaded as Active — resurrecting state the query would otherwise never see.
            /// Such parts exist until the asynchronous cleanup deletes them from disk: the empty covering
            /// part left by `ALTER TABLE ... DETACH PART`, an empty part removed from the working set by
            /// `clearEmptyParts`, or a part removed by `DROP PARTITION` (the same resurrection happens on a
            /// server restart in that window, so it is pre-existing behavior, but this hook must not turn it
            /// into a deterministic side effect of an unrelated query). A resurrected part changes `SELECT`
            /// results and part-level introspection, and a resurrected empty part additionally lacks the
            /// projections of its neighbors, making a subsequent `OPTIMIZE TABLE ... FINAL` a silent no-op
            /// ("Parts have different projection sets" in `MergeTreeMergePredicate::canMergeParts`). Like the
            /// checks above, this is a best-effort, point-in-time check.
            {
                const auto active_parts = merge_tree->getDataPartsVectorForInternalUsage();
                const auto outdated_parts = merge_tree->getDataPartsVectorForInternalUsage({MergeTreeData::DataPartState::Outdated});
                bool has_uncovered_outdated_parts = false;
                for (const auto & outdated : outdated_parts)
                {
                    bool covered = false;
                    for (const auto & active : active_parts)
                    {
                        if (active->info.contains(outdated->info))
                        {
                            covered = true;
                            break;
                        }
                    }
                    if (!covered)
                    {
                        has_uncovered_outdated_parts = true;
                        break;
                    }
                }
                if (has_uncovered_outdated_parts)
                    continue;
            }
        }

        /// The internal `DETACH TABLE` and `ATTACH TABLE` below authorize against the actual object kind:
        /// `InterpreterDropQuery` requires `DROP TABLE` for a plain table but `DROP VIEW`/`DROP DICTIONARY`
        /// for a view/dictionary, and the `ATTACH` path in `InterpreterCreateQuery` correspondingly requires
        /// `CREATE TABLE`, `CREATE VIEW` or `CREATE DICTIONARY`. For a plain table the `ATTACH` additionally
        /// checks the `TABLE ENGINE` grant for the table's engine when
        /// `access_control_improvements.table_engines_require_grant` is enabled; views and dictionaries have
        /// no engine of their own, so the engine grant does not apply to them. Mirror all of these
        /// requirements here; otherwise the hook could `DETACH` a table and then fail to `ATTACH` it back
        /// (e.g. with `ACCESS_DENIED` on the engine grant), leaving the table detached and failing later
        /// statements with `UNKNOWN_TABLE`. Skip the table if the user lacks any of them.
        /// (No view or dictionary kind currently reports `storesDataOnDisk`, so the kind-specific branches
        /// are defensive; the check above already keeps such objects out of the reattach loop.)
        /// `isGranted(TABLE_ENGINE, ...)` already accounts for the `table_engines_require_grant` setting.
        const AccessType drop_access = table->isView() ? AccessType::DROP_VIEW
            : table->isDictionary() ? AccessType::DROP_DICTIONARY
            : AccessType::DROP_TABLE;
        const AccessType create_access = table->isView() ? AccessType::CREATE_VIEW
            : table->isDictionary() ? AccessType::CREATE_DICTIONARY
            : AccessType::CREATE_TABLE;
        if (!access->isGranted(drop_access, table_id.getDatabaseName(), table_id.getTableName())
            || !access->isGranted(create_access, table_id.getDatabaseName(), table_id.getTableName()))
            continue;
        if (!table->isView() && !table->isDictionary()
            && !access->isGranted(AccessType::TABLE_ENGINE, table->getName()))
            continue;

        table.reset();

        auto quoted_name = backQuoteIfNeed(table_id.getDatabaseName()) + "." + backQuoteIfNeed(table_id.getTableName());
        auto detach_query = fmt::format("DETACH TABLE {} SYNC", quoted_name);
        auto attach_query = fmt::format("ATTACH TABLE {}", quoted_name);

        /// The outer query is already registered in the process list with its `query_id`.
        /// Internal `DETACH`/`ATTACH` queries must use a fresh context with their own
        /// `query_id`, otherwise `ProcessList::insert` will reject them with
        /// `QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING`.
        auto make_internal_context = [&]
        {
            auto internal_context = Context::createCopy(context);
            internal_context->makeQueryContext();
            internal_context->setCurrentQueryId({});
            return internal_context;
        };

        bool detached = false;
        try
        {
            {
                auto internal_context = make_internal_context();
                auto detach = executeQuery(detach_query, internal_context, QueryFlags{.internal = true}).second;
                executeTrivialBlockIO(detach, internal_context);
                detached = true;
            }

            {
                auto internal_context = make_internal_context();
                auto attach = executeQuery(attach_query, internal_context, QueryFlags{.internal = true}).second;
                executeTrivialBlockIO(attach, internal_context);
            }
        }
        catch (...)
        {
            tryLogCurrentException("reattachTablesUsedInQuery", "", LogsLevel::warning);

            /// If DETACH succeeded but ATTACH failed, try to re-attach the table
            /// to avoid leaving it in a detached state.
            /// Decide recovery from the actual post-exception state, not only from the `detached`
            /// flag: `DETACH TABLE ... SYNC` first removes the table from the database and only
            /// then waits for the detached table to become unused, so an exception from that wait
            /// (e.g. when the query is killed) leaves the table detached while `detached` is still
            /// `false`.
            ///
            /// But a concurrent query may have detached or dropped the table before our `DETACH`
            /// acquired the `DDLGuard`; then our `DETACH TABLE ... SYNC` fails with `UNKNOWN_TABLE`
            /// and we did not detach anything. In that case the table is missing because of the other
            /// query, not because of this hook, so re-attaching here would undo that query's detach.
            /// Only treat the table as detached-by-us when the failure was not `UNKNOWN_TABLE`.
            if (!detached && getCurrentExceptionCode() != ErrorCodes::UNKNOWN_TABLE)
                detached = !catalog.isTableExist(table_id, context);

            if (detached)
            {
                try
                {
                    auto internal_context = make_internal_context();
                    auto attach = executeQuery(attach_query, internal_context, QueryFlags{.internal = true}).second;
                    executeTrivialBlockIO(attach, internal_context);
                }
                catch (...)
                {
                    tryLogCurrentException("reattachTablesUsedInQuery",
                        fmt::format("Failed to re-attach table {} after failed DETACH/ATTACH cycle", quoted_name),
                        LogsLevel::error);
                    /// Re-throw so the outer query fails clearly instead of running against a permanently
                    /// detached table and producing confusing `UNKNOWN_TABLE` errors later.
                    throw;
                }
            }
        }
    }
}

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
    if (flags.internal)
        context->getClientInfo().is_internal = true;

    /// Gates concurrency limits, throttling, query-size limit, logging.
    const bool internal = flags.internal;
    /// Can be spoofed as it comes from the wire.
    const bool log_as_internal = context->getClientInfo().is_internal;

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

    chassert(internal || CurrentThread::get().tryGetQueryContext());
    chassert(internal || CurrentThread::get().tryGetQueryContext()->getCurrentQueryId() == CurrentThread::getQueryId());

    const Settings & settings = context->getSettingsRef();

    size_t max_query_size = settings[Setting::max_query_size];
    /// Don't limit the size of internal queries or distributed subquery.
    if (internal || client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        max_query_size = 0;

    String query;
    String query_for_logging;
    UInt64 normalized_query_hash = 0;
    size_t log_queries_cut_to_length = settings[Setting::log_queries_cut_to_length];

    /// Parse the query from string.
    try
    {
        ProfileEventTimeIncrement<Microseconds> parse_time_watch(ProfileEvents::QueryParseMicroseconds);

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
        else if (settings[Setting::dialect] == Dialect::polyglot && !internal)
        {
            /// Pass through to `ParserPolyglotQuery` which handles SET queries
            /// internally (via the standard parser) even when the feature gate
            /// is off.  This lets users recover from misconfigured profiles
            /// (e.g. `SET dialect = 'clickhouse'`) without being locked out.
            ParserPolyglotQuery parser(
                max_query_size,
                settings[Setting::max_parser_depth],
                settings[Setting::max_parser_backtracks],
                settings[Setting::polyglot_dialect],
                end,
                settings[Setting::allow_experimental_polyglot_dialect]);
            out_ast = parseQuery(parser, begin, end, "", max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        }
        else if (settings[Setting::dialect] == Dialect::clickhouse_json && !internal)
        {
            /// Allow `SET` queries in plain SQL so users can switch back to another dialect
            /// without being locked into JSON-only input. The experimental gate must be
            /// applied only to the JSON-deserialization branch — otherwise a session with
            /// `dialect = clickhouse_json` and `enable_json_ast_dialect = 0`
            /// cannot execute `SET dialect = 'clickhouse'` to recover.
            if (isClickHouseJSONSetEscape(begin, end, settings[Setting::max_query_size]))
            {
                ParserQuery parser(end, settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);
                out_ast = parseQuery(parser, begin, end, "", max_query_size, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
            }
            else
            {
                if (!settings[Setting::enable_json_ast_dialect])
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED,
                        "Support for clickhouse_json dialect is disabled "
                        "(turn on setting 'enable_json_ast_dialect')");

                if (max_query_size != 0 && static_cast<size_t>(end - begin) > max_query_size)
                    throw Exception(ErrorCodes::SYNTAX_ERROR,
                        "Max query size exceeded (can be increased with the `max_query_size` setting)");

                /// A single-statement `clickhouse_json` query may carry a trailing `;` delimiter, just as
                /// the SQL path and the JSON multiquery scanner accept one. `Poco::JSON::Parser` rejects
                /// any trailing non-whitespace ("Excess characters found after JSON end"), so strip one
                /// trailing `;` (and surrounding whitespace) before deserializing. Anything else after the
                /// object is still rejected by the JSON parser as excess input.
                const char * json_end = end;
                while (json_end > begin && isWhitespaceASCII(json_end[-1]))
                    --json_end;
                if (json_end > begin && json_end[-1] == ';')
                {
                    --json_end;
                    while (json_end > begin && isWhitespaceASCII(json_end[-1]))
                        --json_end;
                }

                out_ast = IAST::createFromJSON(String(begin, json_end),
                    settings[Setting::max_ast_depth],
                    settings[Setting::max_ast_elements]);
                checkASTSizeLimits(*out_ast, settings);
            }
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
                /// If you format AST, parse it back, you get the same AST, and if you format it again, you get the same string.
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

                if (out_ast->getTreeHash(false) != ast2->getTreeHash(false))
                {
                    WriteBufferFromOwnString ast_tree1;
                    WriteBufferFromOwnString ast_tree2;
                    out_ast->dumpTree(ast_tree1);
                    ast2->dumpTree(ast_tree2);

                    throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Inconsistent AST formatting: the original AST:\n{}\n differs from the result of parsing back formatted AST:\n{}\n",
                        ast_tree1.str(), ast_tree2.str());
                }

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
                /// Method formatImpl is not supported by MySQLParser::ASTCreateQuery. That code would fail under the debug build.
                if (e.code() != ErrorCodes::NOT_IMPLEMENTED)
                    throw;
            }
#endif
        }

        const char * query_end = end;

        if (out_ast)
        {
            if (const auto * insert_query = out_ast->as<ASTInsertQuery>(); insert_query && insert_query->data)
                query_end = insert_query->data;
        }

        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        /// Even if we don't have parameters in query_context, check that AST doesn't have unknown parameters.
        /// The visitor handles parameterized views internally: it substitutes parameters in
        /// DDL parts (database, table, columns, storage, targets) while preserving placeholders
        /// in the SELECT body, which form the view's parameterizable interface.
        bool probably_has_params = find_first_symbols<'{'>(begin, end) != end;
        if (out_ast && probably_has_params)
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
        /// Make the hash available to the parts of execution that account `NORMALIZED_QUERY_HASH`
        /// quotas but do not otherwise have it (e.g. the insert path).
        context->setNormalizedQueryHash(normalized_query_hash);
    }
    catch (...)
    {
        /// Anyway log the query.
        if (query.empty())
            query.assign(begin, std::min(static_cast<size_t>(end - begin), max_query_size));

        query_for_logging = wipeSensitiveDataAndCutToLength(query, log_queries_cut_to_length, true);
        logQuery(query_for_logging, context, internal, stage);

        normalized_query_hash = normalizedQueryHash(query_for_logging, false);
        logExceptionBeforeStart(query_for_logging, normalized_query_hash, context, out_ast, query_span, start_watch.elapsedMilliseconds(), internal, log_as_internal);
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
            /// `EXPLAIN ANALYZE` executes the inner `SELECT`, so after a transaction has failed it must
            /// be rejected exactly like the query it wraps. Other `EXPLAIN` kinds do not execute the
            /// inner query (they only print a plan) and stay special, as do transaction-control
            /// statements so that a failed transaction can still be rolled back.
            const auto * explain_query = out_ast ? out_ast->as<ASTExplainQuery>() : nullptr;
            const bool is_executing_explain_analyze = explain_query && explain_query->getKind() == ASTExplainQuery::Analyze;
            bool is_special_query = out_ast
                && (out_ast->as<ASTTransactionControl>() || (explain_query && !is_executing_explain_analyze));
            if (txn->getState() == MergeTreeTransaction::ROLLED_BACK && !is_special_query)
                throw Exception(
                    ErrorCodes::INVALID_TRANSACTION,
                    "Cannot execute query because current transaction failed. Expecting ROLLBACK statement");
        }

        /// There is an option of probabilistic logging of queries.
        /// If it is used - do the random sampling and "collapse" the settings.
        /// It allows to consistently log queries with all the subqueries in distributed query processing
        /// (subqueries on remote nodes will receive these "collapsed" settings)
        if (settings[Setting::log_queries] && static_cast<double>(settings[Setting::log_queries_probability]) < 1.0)
        {
            std::bernoulli_distribution should_write_log{static_cast<double>(settings[Setting::log_queries_probability])};

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

        /// Reattach tables only after AST validations pass, the query is admitted
        /// to the process list, and external tables are initialized — so queries
        /// rejected before this point do not produce DETACH/ATTACH side effects,
        /// and external tables correctly shadow persistent ones during resolution.
        /// Skip EXPLAIN: it should not mutate server state.
        if (out_ast)
        {
            bool is_initial_query = client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY;
            bool has_transaction = context->getCurrentTransaction() || settings[Setting::implicit_transaction];
            bool is_explain = out_ast->as<ASTExplainQuery>() != nullptr;
            /// An `ON CLUSTER` statement is skipped entirely: on the initiator the interpreter delegates
            /// to `executeDDLQueryOnCluster` before performing any local table operation, so reattaching
            /// here would give a side effect on a local table the query itself may never touch (the local
            /// host may not even be in the target cluster). The real per-host executions replayed by the
            /// `DDLWorker` are not `INITIAL_QUERY` (see `DDLTaskBase::makeQueryContext`), so they are
            /// filtered out by the gate above and are not randomized either.
            const auto * query_on_cluster = dynamic_cast<const ASTQueryWithOnCluster *>(out_ast.get());
            bool is_on_cluster = query_on_cluster && !query_on_cluster->cluster.empty();
            if (!internal && is_initial_query && !has_transaction && !is_explain && !is_on_cluster)
            {
                bool need_reattach_tables = settings[Setting::reattach_tables_before_query_execution];
                auto reattach_probability = std::clamp(
                    static_cast<double>(settings[Setting::reattach_tables_before_query_execution_probability]),
                    0.0, 1.0);

                if (!need_reattach_tables && reattach_probability > 0.0)
                {
                    std::bernoulli_distribution distribution(reattach_probability);
                    need_reattach_tables |= distribution(thread_local_rng);
                }

                if (need_reattach_tables)
                {
                    LOG_DEBUG(getLogger("executeQuery"), "Will DETACH and ATTACH back tables used in query");
                    reattachTablesUsedInQuery(out_ast, context);
                }
            }
        }
        std::shared_ptr<QueryPlanAndSets> query_plan;
        if (stage == QueryProcessingStage::QueryPlan)
            query_plan = context->getDeserializedQueryPlan();

        ASTInsertQuery * insert_query = nullptr;
        if (out_ast)
            insert_query = out_ast->as<ASTInsertQuery>();
        bool async_insert_enabled = settings[Setting::async_insert];

        /// Resolve database before trying to use async insert feature - to properly hash the query.
        StoragePtr insert_table;
        if (insert_query)
        {
            if (insert_query->table_id)
                insert_query->table_id = context->resolveStorageID(insert_query->table_id);
            else if (auto table = insert_query->getTable(); !table.empty())
                insert_query->table_id = context->resolveStorageID(StorageID{insert_query->getDatabase(), table});

            if (insert_query->table_id)
            {
                insert_table = DatabaseCatalog::instance().tryGetTable(insert_query->table_id, context);
                if (insert_table)
                    async_insert_enabled |= insert_table->areAsynchronousInsertsEnabled();
            }
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
                    /// For input('auto'), make sure that Context::insertion_table_info is set.
                    if (insert_table && !context->hasInsertionTableColumnsDescription())
                        InterpreterInsertQuery::setInsertContextValues(context, *insert_query, insert_table);

                    const ASTSelectQuery * select_query_hint = insert_query->select->as<ASTSelectQuery>();
                    if (!select_query_hint)
                    {
                        if (const auto * union_query = insert_query->select->as<ASTSelectWithUnionQuery>();
                            union_query && union_query->list_of_selects->children.size() == 1)
                            select_query_hint = union_query->list_of_selects->children.front()->as<ASTSelectQuery>();
                    }
                    StoragePtr storage = context->executeTableFunction(input_function, select_query_hint);
                    auto & input_storage = dynamic_cast<StorageInput &>(*storage);
                    auto input_metadata_snapshot = input_storage.getInMemoryMetadataPtr(context, false);

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
                /// Each governing quota is accounted appropriately: NORMALIZED_QUERY_HASH quotas
                /// track against per-hash intervals, the rest against shared session intervals.
                quota->usedForQuery(normalized_query_hash, QuotaType::QUERY_INSERTS, 1);
                quota->usedForQuery(normalized_query_hash, QuotaType::QUERIES, 1);
                quota->usedForQuery(normalized_query_hash, QuotaType::ERRORS, 0, /* check_exceeded = */ true);

                /// Track per-normalized-query-hash quota limits (works for all key types).
                quota->usedPerNormalizedHash(normalized_query_hash);
            }

            /// Invoke HTTP 100-Continue callback after async insert quota checks are completed
            if (http_continue_callback && !internal)
                http_continue_callback();

            auto result = queue->pushQueryWithInlinedData(out_ast, context);

            if (result.status == AsynchronousInsertQueue::PushResult::OK)
            {
                // Increment InsertQuery for async insert with inline data
                ProfileEvents::increment(ProfileEvents::InsertQuery);

                if (settings[Setting::wait_for_async_insert])
                {
                    auto timeout = settings[Setting::wait_for_async_insert_timeout].totalMilliseconds();
                    auto source = std::make_shared<WaitForAsyncInsertSource>(
                        std::move(result.future),
                        timeout,
                        context->getProcessListElement(),
                        context->getProgressCallback());
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
        context->setCanUseQueryResultCache(can_use_query_result_cache);
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
                    QueryResultCache::Key key(out_ast, context->getCurrentDatabase(), *settings_copy, context->getCurrentQueryId(), context->getUserID(), context->getCurrentRoles(), /* is_subquery = */ false);
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
                        /// EXPLAIN ANALYZE executes the inner SELECT only when it is an executable
                        /// analyze (inner SELECT, non-distributed), in which case it must be charged
                        /// against the select-query quota just like a normal SELECT. Rejected forms such
                        /// as EXPLAIN ANALYZE INSERT, EXPLAIN ANALYZE SYSTEM, or distributed EXPLAIN
                        /// ANALYZE never run an inner SELECT and stay counted as generic queries only, so
                        /// reuse the same predicate that gates execution instead of the AST kind alone.
                        const auto * explain_interpreter = dynamic_cast<const InterpreterExplainQuery *>(interpreter.get());
                        const bool is_executable_analyze = explain_interpreter && explain_interpreter->isExecutableAnalyze();
                        const bool charge_as_select = query_plan
                            || out_ast->as<ASTSelectQuery>()
                            || out_ast->as<ASTSelectWithUnionQuery>()
                            || is_executable_analyze;

                        /// `usedForQuery` dispatches per quota: for `NORMALIZED_QUERY_HASH` keyed
                        /// quotas it charges the per-hash intervals, otherwise the shared session
                        /// intervals. So a single set of calls covers all key types.
                        if (charge_as_select)
                            quota->usedForQuery(normalized_query_hash, QuotaType::QUERY_SELECTS, 1);
                        else if (out_ast->as<ASTInsertQuery>())
                            quota->usedForQuery(normalized_query_hash, QuotaType::QUERY_INSERTS, 1);
                        quota->usedForQuery(normalized_query_hash, QuotaType::QUERIES, 1);
                        quota->usedForQuery(normalized_query_hash, QuotaType::ERRORS, 0, /* check_exceeded = */ true);

                        /// Track per-normalized-query-hash quota limits (works for all key types).
                        quota->usedPerNormalizedHash(normalized_query_hash);
                    }
                }

                /// Invoke HTTP 100-Continue callback after quota checks are completed
                if (http_continue_callback && !internal)
                    http_continue_callback();

                if (interpreter)
                {
                    if (!interpreter->ignoreLimits())
                        limits = StreamLocalLimits::forQueryResult(settings);

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
                    /// If it is a non-internal SELECT query, and active (write) use of the query cache is enabled, then add a processor on
                    /// top of the pipeline which stores the result in the query cache.
                    if (checkCanWriteQueryResultCache(out_ast, context))
                    {
                            auto created_at = std::chrono::system_clock::now();
                            auto expires_at = created_at + std::chrono::seconds(settings[Setting::query_cache_ttl].totalSeconds());

                            QueryResultCache::Key key(
                                out_ast, context->getCurrentDatabase(), *settings_copy, res.pipeline.getSharedHeader(),
                                context->getCurrentQueryId(),
                                context->getUserID(), context->getCurrentRoles(),
                                settings[Setting::query_cache_share_between_users],
                                created_at, expires_at,
                                settings[Setting::query_cache_compress_entries],
                                /* is_subquery = */ false);

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

        if (process_list_entry)
        {
            /// Query was killed before execution
            auto query_status = process_list_entry->getQueryStatus();
            if (query_status->isKilled())
            {
                /// The deadline (max_execution_time) can fire while the query is still pending (e.g. slow to
                /// analyze/plan). Report it as a timeout, not a generic cancellation, so callers see the same
                /// TIMEOUT_EXCEEDED they would get had the deadline fired during execution.
                if (query_status->getCancelReason() == CancelReason::TIMEOUT)
                    query_status->throwIfKilled();
                throw Exception(ErrorCodes::QUERY_WAS_CANCELLED,
                    "Query '{}' is killed in pending state", query_status->getInfo().client_info.current_query_id);
            }
        }

        /// Hold element of process list till end of query execution.
        res.process_list_entries.push_back(process_list_entry);

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

        /// Propagate the normalized query hash so that `NORMALIZED_QUERY_HASH` quotas account the
        /// result/read/execution-time counters against the per-hash intervals of this query pattern.
        pipeline.setNormalizedQueryHash(normalized_query_hash);

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
                log_as_internal,
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
                                    log_as_internal,
                                    implicit_tcl_executor,
                                    // Need to be cached, since will be changed after complete()
                                    pulling_pipeline = pipeline.pulling(),
                                    query_span](const QueryPipelineFinalizedInfo & query_pipeline_finalized_info, std::chrono::system_clock::time_point finish_time) mutable
            {
                logQueryFinishImpl(elem, context, out_ast, query_pipeline_finalized_info, pulling_pipeline, query_span, query_result_cache_usage, internal, log_as_internal, finish_time);

                if (implicit_tcl_executor->transactionRunning())
                {
                    implicit_tcl_executor->commit(context);
                }
            };

            auto exception_callback =
                [start_watch, elem, context, out_ast, internal, log_as_internal, my_quota(quota), normalized_query_hash, implicit_tcl_executor, query_span](bool log_error) mutable
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
                        my_quota->usedForQuery(normalized_query_hash, QuotaType::ERRORS, 1, /* check_exceeded = */ false);
                }

                logQueryException(elem, context, start_watch, out_ast, query_span, internal, log_as_internal, log_error);
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

        logExceptionBeforeStart(query_for_logging, normalized_query_hash, context, out_ast, query_span, start_watch.elapsedMilliseconds(), internal, log_as_internal);

        throw;
    }

    return res;
}


std::pair<std::shared_ptr<QueryFuzzer>, std::unique_lock<std::mutex>> getGlobalASTFuzzer()
{
    static std::mutex mutex;
#if WITH_COVERAGE
    /// Under LLVM coverage builds we use a fixed seed so that the set of AST mutations
    /// (and therefore the set of branches taken inside `QueryFuzzer`) is stable run-to-run.
    /// Without this, coverage of `QueryFuzzer.cpp` and friends flickers between coverage runs.
    static std::shared_ptr<QueryFuzzer> fuzzer = std::make_shared<QueryFuzzer>(pcg64(0xC0FFEEULL));
#else
    static std::shared_ptr<QueryFuzzer> fuzzer = std::make_shared<QueryFuzzer>(randomSeed());
#endif
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

    /// Do not fuzz while an internal replicated-DDL execution is in flight on `context`.
    /// DatabaseReplicatedDDLWorker re-executes a committed DDL entry whose serialized settings still
    /// carry ast_fuzzer_runs, so the fuzzer would fire again on the entry's live, single-shot
    /// ZooKeeperMetadataTransaction. A fuzzed follow-up DDL then either adds ops to the already-executed
    /// txn (ZooKeeperMetadataTransaction::addOp throws "Cannot add ZooKeeper operation because query is
    /// executed") or, because is_replicated_database_internal makes shouldReplicateQuery() route it to a
    /// local commit, reaches DatabaseReplicated::commit* with no txn while the DDL worker is active and
    /// trips the `!ddl_worker->isCurrentlyActive() || txn` assertion. Both are LOGICAL_ERRORs that abort
    /// debug/sanitizer builds. The initiating client query is fuzzed normally; only this redundant
    /// re-fuzz during log replay is skipped.
    if (context->getClientInfo().is_replicated_database_internal || context->getZooKeeperMetadataTransaction())
    {
        ProfileEvents::increment(ProfileEvents::ASTFuzzerSkippedReplicatedDDLInternal);
        return;
    }

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

    /// The fuzzer runs as a query finish callback, after the outer query's pipeline executor
    /// has stopped enforcing limits. Without these checks the outer query keeps spawning fuzzed
    /// queries while ignoring its own deadline, a KILL, or server shutdown, so it lingers in the
    /// processlist and can trip the stress test hung check.
    /// Some fuzzable queries (e.g. SHOW PROCESSLIST) are not inserted into the ProcessList, so
    /// the deadline/KILL check via checkTimeLimitSoft is unavailable; the shutdown metric still
    /// stops the loop in that case.
    QueryStatusPtr process_list_element = context->getProcessListElement();

    ASTPtr base_ast = ast;

    for (size_t i = 0; i < num_runs; ++i)
    {
        if (CurrentMetrics::get(CurrentMetrics::IsServerShuttingDown))
        {
            LOG_TRACE(logger, "Stopping AST fuzzer: the server is shutting down");
            break;
        }

        /// checkTimeLimitSoft returns false without throwing on a KILL or the outer deadline.
        if (process_list_element && !process_list_element->checkTimeLimitSoft())
        {
            LOG_TRACE(logger, "Stopping AST fuzzer: outer query was killed or timed out");
            break;
        }

        ASTPtr fuzzed_ast;
        NameToNameMap fuzzed_query_params;
        {
            auto [fuzzer, lock] = getGlobalASTFuzzer();
            fuzzed_ast = base_ast->clone();
            fuzzer->fuzzMain(fuzzed_ast);
            fuzzed_query_params = fuzzer->getLastQueryParameters();
        }

        /// Skip fuzzed `BACKUP` / `RESTORE` queries. An async `RESTORE`/`BACKUP` returns from
        /// `executeQuery` immediately while `BackupsWorker` keeps the query context alive and its
        /// background workers read it via `Context::createCopy` under the shared `Context::mutex`.
        /// The per-iteration cleanup below would then mutate that escaped context without holding
        /// the mutex, reintroducing the very `merge_tree_transaction` data race this code avoids.
        /// Checked first (before the depth/format/length guards below), and counted, so the skip
        /// is attributable to the query type alone regardless of those other early-continue paths.
        if (fuzzed_ast->as<ASTBackupQuery>())
        {
            ProfileEvents::increment(ProfileEvents::ASTFuzzerSkippedBackupRestore);
            continue;
        }

        /// Skip deeply nested ASTs to avoid stack overflow during formatting or execution.
        try
        {
            fuzzed_ast->checkDepth(500);
        }
        catch (...) // Ok: skip fuzzed ASTs that are too deeply nested
        {
            continue;
        }

        /// Drop any SETTINGS that would override the fuzz-context resource caps below; otherwise a
        /// seed/fuzzed `SETTINGS max_rows_to_read = 0` (etc.) lets the heavy query run unbounded.
        stripFuzzerSafetyLimitSettings(fuzzed_ast);

        /// The fuzzer can produce structurally invalid ASTs (e.g. mismatched children counts)
        /// that cause crashes during formatting. Catch and skip those.
        String fuzzed_query;
        try
        {
            WriteBufferFromOwnString fuzzed_query_buf;
            fuzzed_ast->format(fuzzed_query_buf, IAST::FormatSettings(/*one_line=*/true));
            fuzzed_query = fuzzed_query_buf.str();
        }
        catch (...) // Ok: skip fuzzed ASTs that cannot be formatted
        {
            continue;
        }

        if (fuzzed_query.size() > 10000)
        {
            LOG_TRACE(logger, "Fuzzed query too long ({} chars), skipping", fuzzed_query.size());
            continue;
        }

        ProfileEvents::increment(ProfileEvents::ASTFuzzerQueries);
        LOG_TRACE(logger, "Fuzzed query: {}", fuzzed_query);

        /// Declare contexts outside try block so we can reset transactions on all paths.
        /// MergeTreeTransactionHolder destructor calls rollbackTransaction (noexcept),
        /// which uses getCurrentExceptionCode with bare `throw;` - that only works
        /// inside a catch handler, not during stack unwinding.
        ContextMutablePtr fuzz_session_context;
        ContextMutablePtr fuzz_context;

        auto reset_transactions = [&]()
        {
            if (fuzz_context)
                fuzz_context->setCurrentTransaction(NO_TRANSACTION_PTR);
            if (fuzz_session_context)
                fuzz_session_context->setCurrentTransaction(NO_TRANSACTION_PTR);
        };

        try
        {
            fuzz_session_context = Context::createCopy(context);
            fuzz_session_context->makeSessionContext();
            /// Reset the transaction (if any) on the fuzz session context to isolate
            /// fuzzed queries from the caller's transaction state. The transaction pointer
            /// was copied as a shared_ptr in the copy constructor (see `InterpreterTransactionControlQuery::executeBegin`
            /// which stores it in both session and query contexts).
            /// We clear it on the copy, not on the parent `context`: mutating the caller's
            /// `merge_tree_transaction` races with concurrent readers of the same `Context`
            /// (e.g. `RESTORE ASYNC` background workers calling `Context::createCopy` under
            /// the shared `Context::mutex`), and it also has the surprising side effect of
            /// silently clearing the user's active transaction on the caller session.
            fuzz_session_context->setCurrentTransaction(NO_TRANSACTION_PTR);

            fuzz_context = Context::createCopy(fuzz_session_context);
            fuzz_context->makeQueryContext();
            fuzz_context->resetInputCallbacks();
            fuzz_context->clearTableFunctionResults();
            fuzz_context->setSetting("ast_fuzzer_runs", Field(Float64(0)));
            fuzz_context->setSetting("allow_experimental_parallel_reading_from_replicas", Field(UInt64(0)));

            /// Limit resources for each fuzzed query to prevent runaway execution.
            fuzz_context->setSetting("max_execution_time", Field(UInt64(10)));
            fuzz_context->setSetting("max_memory_usage", Field(UInt64(1024 * 1024 * 1024)));  /// 1 GiB
            fuzz_context->setSetting("max_result_rows", Field(UInt64(1000)));
            fuzz_context->setSetting("max_result_bytes", Field(UInt64(10 * 1024 * 1024)));  /// 10 MiB

            /// The fuzzer rewrites numeric literals to boundary values (1 MiB +/- 1, INT_MAX, ...),
            /// so a seed `numbers(100)` can become `numbers(1048576)` and the resulting INSERT grinds
            /// through ~1M rows of fuzzer-generated columns in the part writer (minutes under
            /// sanitizers). max_execution_time only fires between pipeline tasks, so a single heavy
            /// block can blow past it. Bound the read side instead: stop reading (break, do not throw)
            /// after enough rows to keep exercising the query structure without a runaway data volume.
            fuzz_context->setSetting("max_rows_to_read", Field(UInt64(100000)));
            fuzz_context->setSetting("read_overflow_mode", Field("break"));

            /// max_rows_to_read is only checked after a source emits a chunk, so it bounds the number of
            /// chunks but not the size of the first one. A trivial INSERT ... SELECT into a table that
            /// prefers large blocks copies min_insert_block_size_rows into the SELECT's max_block_size
            /// (InterpreterInsertQuery::applyTrivialInsertSelectOptimization), and the default is
            /// ~1M rows, so a single ~1M-row chunk can still reach the part writer and spend minutes in
            /// one pipeline task before read_overflow_mode = break cancels further reads. The cancel
            /// callback only runs between tasks, so it cannot interrupt that block. Pin both block-forming
            /// settings small so the first emitted chunk is bounded too.
            fuzz_context->setSetting("max_block_size", Field(UInt64(65409)));
            fuzz_context->setSetting("min_insert_block_size_rows", Field(UInt64(65409)));

            fuzz_context->setCurrentQueryId("");
            if (!fuzzed_query_params.empty())
                fuzz_context->setQueryParameters(fuzzed_query_params);

            /// Run the fuzzed query on its own thread group, so that code reading the query context
            /// from the thread (read/write settings, temporary data, distributed plan execution, ...)
            /// sees the fuzz context and the limits pinned above instead of the outer query's.
            ThreadGroupSwitcher thread_group_switcher(
                ThreadGroup::createForQuery(fuzz_context), ThreadName::AST_FUZZER, /*allow_existing_group=*/ true);

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

                    /// A single in-flight fuzzed query (e.g. a heavy INSERT) only checks its own
                    /// time limit between pipeline tasks, so without a cancel callback it ignores the
                    /// outer query's KILL/timeout and server shutdown and can run for minutes, tripping
                    /// the stress test hung check. Poll the same conditions the loop guard uses, plus a
                    /// wall-clock deadline, and cancel the executor (it runs on a separate thread).
                    Stopwatch fuzzed_query_watch;
                    executor.setCancelCallback(
                        [&fuzzed_query_watch, &process_list_element]()
                        {
                            if (CurrentMetrics::get(CurrentMetrics::IsServerShuttingDown))
                                return true;
                            if (process_list_element && !process_list_element->checkTimeLimitSoft())
                                return true;
                            return fuzzed_query_watch.elapsedMilliseconds() > 30000;
                        },
                        /*interactive_timeout_ms=*/100);
                    executor.execute();
                }
            }

            reset_transactions();
            base_ast = fuzzed_ast;
        }
        catch (...)
        {
            reset_transactions();
            LOG_TRACE(logger, "Fuzzed query failed: {}", getCurrentExceptionMessage(/*with_stacktrace=*/false));
            auto [fuzzer, lock] = getGlobalASTFuzzer();
            fuzzer->notifyQueryFailed(fuzzed_ast);
        }
    }
}


/// Helper that runs the failpoints used to test crash-log/stack-trace features.
/// Kept separate (and `noinline`) from `executeQuery` so the symbolizer always
/// reports a frame whose function name contains "executeQuery", regardless of
/// how the compiler lays out the catch handlers in the surrounding function
/// (the symbol attribution for cold paths inside `executeQuery` can otherwise
/// be lost depending on unrelated changes in this translation unit).
[[gnu::noinline, gnu::cold]]
static void executeQueryFailpoints()
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

    fiu_do_on(FailPoints::trigger_sanitizer_error,
    {
        triggerSanitizerError();
    });
}


std::pair<ASTPtr, BlockIO> executeQuery(
    std::string_view query,
    ContextMutablePtr context,
    QueryFlags flags,
    QueryProcessingStage::Enum stage)
{
    if (isCrashed())
        throw Exception(ErrorCodes::ABORTED, "The server is shutting down due to a fatal error");

    ProfileEvents::checkCPUOverload(context->getServerSettings()[ServerSetting::os_cpu_busy_time_threshold],
            static_cast<double>(context->getSettingsRef()[Setting::min_os_cpu_wait_time_ratio_to_throw]),
            static_cast<double>(context->getSettingsRef()[Setting::max_os_cpu_wait_time_ratio_to_throw]),
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
        executeQueryFailpoints();
    }

    const bool is_shared_catalog_internal = context->getClientInfo().is_shared_catalog_internal;
    if (!flags.internal && !is_shared_catalog_internal && ast)
    {
        Float64 ast_fuzzer_runs_value = static_cast<double>(context->getSettingsRef()[Setting::ast_fuzzer_runs]);
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
    const char * begin = nullptr;
    const char * end = nullptr;

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
            static_cast<double>(context->getSettingsRef()[Setting::min_os_cpu_wait_time_ratio_to_throw]),
            static_cast<double>(context->getSettingsRef()[Setting::max_os_cpu_wait_time_ratio_to_throw]),
            /*should_throw*/ true);

    if (istr->available() > max_query_size || http_continue_callback || flags.parse_query_from_initial_buffer)
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        /// Also, if the HTTP 100 Continue response is deferred (which is the case if http_continue_callback is set),
        /// we should not attempt to read anything from the body. We expect the query (without insert data) to be present
        /// in the buffer already because it should have been extracted from the query parameter.
        /// The same applies to streaming inserts whose query is already in the initial buffer and whose body must remain
        /// available for the input format.
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
        catch (const std::exception &) // NOLINT(bugprone-empty-catch)
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
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The value of the `http_response_headers` setting must be a Map");

            if (key_value.safeGet<Tuple>().at(0).getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The keys of the `http_response_headers` setting must be Strings");

            if (key_value.safeGet<Tuple>().at(1).getType() != Field::Types::String)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The values of the `http_response_headers` setting must be Strings");

            String key = key_value.safeGet<Tuple>().at(0).safeGet<String>();
            String value = key_value.safeGet<Tuple>().at(1).safeGet<String>();

            if (std::find_if(key.begin(), key.end(), isControlASCII) != key.end()
                || std::find_if(value.begin(), value.end(), isControlASCII) != value.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "The keys and values of the `http_response_headers` setting cannot contain ASCII control characters");

            if (!result_details.additional_headers.emplace(key, value).second)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "There are duplicate entries in the `http_response_headers` setting");
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

        /// Query with `implicit_transaction` is committed here because:
        /// 1. `onFinish` is invoked after the transaction is committed.
        /// 2. When handling HTTP requests, in `HTTPHandler::processQuery`, there is `query_finish_callback` which is invoked before `onFinish`.
        /// It releases the session and finalizes the output. The client might use the same session to query other queries. Hence, the transaction must be committed before `query_finish_callback`.
        /// Refer: https://github.com/ClickHouse/ClickHouse/issues/80428
        if (implicit_tcl_executor->transactionRunning())
            implicit_tcl_executor->commit(context);

        const bool is_shared_catalog_internal = context->getClientInfo().is_shared_catalog_internal;
        if (!flags.internal && !is_shared_catalog_internal && ast)
        {
            Float64 ast_fuzzer_runs_value = static_cast<double>(context->getSettingsRef()[Setting::ast_fuzzer_runs]);
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

    QueryFinishCallback finish_callback;
    if (query_finish_callback)
    {
        finish_callback = [&]()
        {
            /// Flush the progress (result_rows/result_bytes) before query_finish_callback sends the final HTTP header,
            /// so the X-ClickHouse-Summary header is correct.
            flushQueryProgress(pipeline, pulling_pipeline, context->getProgressCallback(), context->getProcessListElement());
            query_finish_callback();
        };
    }

    finishExecutedQuery(streams, finish_callback);
}

void finishExecutedQuery(BlockIO & io, const QueryFinishCallback & query_finish_callback)
{
    /// Release the query slot now so the client can safely reuse it for its next query, otherwise it would be
    /// released too late by BlockIO. Only the query slot is released here, not the memory reservation: pipeline
    /// threads still hold raw pointers to it until io.onFinish() finalizes the pipeline, so releasing it here
    /// would be a data race.
    io.releaseQuerySlot();

    /// The order is important here:
    /// - first we save finish_time, used for query_log/opentelemetry_span_log.finish_time_us;
    /// - then we call query_finish_callback() - right now its only purpose is to flush the data over HTTP;
    /// - then we call onFinish() that creates the entry in query_log/opentelemetry_span_log.
    /// That way finish_time is the correct finish time of the query regardless of how long query_finish_callback()
    /// takes. If the callback throws, we still run onFinish() and rethrow the callback's exception afterwards, so
    /// onFinish()'s own exceptions propagate normally.
    const auto finish_time = std::chrono::system_clock::now();
    std::exception_ptr callback_exception;
    if (query_finish_callback)
    {
        try
        {
            query_finish_callback();
        }
        catch (...)
        {
            callback_exception = std::current_exception();
        }
    }

    io.onFinish(finish_time);

    if (callback_exception)
        std::rethrow_exception(callback_exception);
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
