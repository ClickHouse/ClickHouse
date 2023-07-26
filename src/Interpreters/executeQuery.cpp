#include <Common/formatReadable.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/SensitiveDataMasker.h>

#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/Cache/QueryCache.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>

#include <QueryPipeline/BlockIO.h>
#include <Processors/Transforms/CountingTransform.h>
#include <Processors/Transforms/getSourceFromASTInsertQuery.h>

#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTRenameQuery.h>
#include <Parsers/ASTAlterQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTWatchQuery.h>
#include <Parsers/ASTTransactionControl.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/Lexer.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryNormalization.h>
#include <Parsers/queryToString.h>
#include <Parsers/formatAST.h>
#include <Parsers/toOneLineQuery.h>

#include <Formats/FormatFactory.h>
#include <Storages/StorageInput.h>

#include <Access/EnabledQuota.h>
#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/InterpreterTransactionControlQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/ProcessorsProfileLog.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/SelectIntersectExceptQueryVisitor.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Common/ProfileEvents.h>

#include <IO/CompressionMethod.h>

#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Executors/CompletedPipelineExecutor.h>
#include <Processors/Sources/WaitForAsyncInsertSource.h>

#include <base/EnumReflection.h>
#include <base/demangle.h>

#include <memory>
#include <random>

#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>

namespace ProfileEvents
{
    extern const Event FailedQuery;
    extern const Event FailedInsertQuery;
    extern const Event FailedSelectQuery;
    extern const Event QueryTimeMicroseconds;
    extern const Event SelectQueryTimeMicroseconds;
    extern const Event InsertQueryTimeMicroseconds;
    extern const Event OtherQueryTimeMicroseconds;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int INTO_OUTFILE_NOT_ALLOWED;
    extern const int QUERY_WAS_CANCELLED;
    extern const int INVALID_TRANSACTION;
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
}


static void checkASTSizeLimits(const IAST & ast, const Settings & settings)
{
    if (settings.max_ast_depth)
        ast.checkDepth(settings.max_ast_depth);
    if (settings.max_ast_elements)
        ast.checkSize(settings.max_ast_elements);
}


/// Log query into text log (not into system table).
static void logQuery(const String & query, ContextPtr context, bool internal, QueryProcessingStage::Enum stage)
{
    if (internal)
    {
        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "(internal) {} (stage: {})", toOneLineQuery(query), QueryProcessingStage::toString(stage));
    }
    else
    {
        const auto & client_info = context->getClientInfo();

        const auto & current_query_id = client_info.current_query_id;
        const auto & initial_query_id = client_info.initial_query_id;
        const auto & current_user = client_info.current_user;

        String comment = context->getSettingsRef().log_comment;
        size_t max_query_size = context->getSettingsRef().max_query_size;

        if (comment.size() > max_query_size)
            comment.resize(max_query_size);

        if (!comment.empty())
            comment = fmt::format(" (comment: {})", comment);

        String transaction_info;
        if (auto txn = context->getCurrentTransaction())
            transaction_info = fmt::format(" (TID: {}, TIDH: {})", txn->tid, txn->tid.getHash());

        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "(from {}{}{}){}{} {} (stage: {})",
            client_info.current_address.toString(),
            (current_user != "default" ? ", user: " + current_user : ""),
            (!initial_query_id.empty() && current_query_id != initial_query_id ? ", initial_query_id: " + initial_query_id : std::string()),
            transaction_info,
            comment,
            toOneLineQuery(query),
            QueryProcessingStage::toString(stage));

        if (client_info.client_trace_context.trace_id != UUID())
        {
            LOG_TRACE(&Poco::Logger::get("executeQuery"),
                "OpenTelemetry traceparent '{}'",
                client_info.client_trace_context.composeTraceparentHeader());
        }
    }
}

/// Call this inside catch block.
static void setExceptionStackTrace(QueryLogElement & elem)
{
    /// Disable memory tracker for stack trace.
    /// Because if exception is "Memory limit (for query) exceed", then we probably can't allocate another one string.

    LockMemoryExceptionInThread lock(VariableContext::Global);

    try
    {
        throw;
    }
    catch (const std::exception & e)
    {
        elem.stack_trace = getExceptionStackTraceString(e);
    }
    catch (...) {}
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

    if (elem.stack_trace.empty() || !log_error)
        message.text = fmt::format("{} (from {}){} (in query: {})", elem.exception,
                        context->getClientInfo().current_address.toString(),
                        comment,
                        toOneLineQuery(elem.query));
    else
        message.text = fmt::format(
            "{} (from {}){} (in query: {}), Stack trace (when copying this message, always include the lines below):\n\n{}",
            elem.exception,
            context->getClientInfo().current_address.toString(),
            comment,
            toOneLineQuery(elem.query),
            elem.stack_trace);

    if (log_error)
        LOG_ERROR(&Poco::Logger::get("executeQuery"), message);
    else
        LOG_INFO(&Poco::Logger::get("executeQuery"), message);
}

static void
addStatusInfoToQueryElement(QueryLogElement & element, const QueryStatusInfo & info, const ASTPtr query_ast, const ContextPtr context_ptr)
{
    const auto time_now = std::chrono::system_clock::now();
    UInt64 elapsed_microseconds = info.elapsed_microseconds;
    element.event_time = timeInSeconds(time_now);
    element.event_time_microseconds = timeInMicroseconds(time_now);
    element.query_duration_ms = elapsed_microseconds / 1000;

    ProfileEvents::increment(ProfileEvents::QueryTimeMicroseconds, elapsed_microseconds);
    if (query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
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
    element.profile_counters = info.profile_counters;

    /// We need to refresh the access info since dependent views might have added extra information, either during
    /// creation of the view (PushingToViews chain) or while executing its internal SELECT
    const auto & access_info = context_ptr->getQueryAccessInfo();
    element.query_databases.insert(access_info.databases.begin(), access_info.databases.end());
    element.query_tables.insert(access_info.tables.begin(), access_info.tables.end());
    element.query_columns.insert(access_info.columns.begin(), access_info.columns.end());
    element.query_partitions.insert(access_info.partitions.begin(), access_info.partitions.end());
    element.query_projections.insert(access_info.projections.begin(), access_info.projections.end());
    element.query_views.insert(access_info.views.begin(), access_info.views.end());

    const auto & factories_info = context_ptr->getQueryFactoriesInfo();
    element.used_aggregate_functions = factories_info.aggregate_functions;
    element.used_aggregate_function_combinators = factories_info.aggregate_function_combinators;
    element.used_database_engines = factories_info.database_engines;
    element.used_data_type_families = factories_info.data_type_families;
    element.used_dictionaries = factories_info.dictionaries;
    element.used_formats = factories_info.formats;
    element.used_functions = factories_info.functions;
    element.used_storages = factories_info.storages;
    element.used_table_functions = factories_info.table_functions;

    element.async_read_counters = context_ptr->getAsyncReadCounters();
}


QueryLogElement logQueryStart(
    const std::chrono::time_point<std::chrono::system_clock> & query_start_time,
    const ContextMutablePtr & context,
    const String & query_for_logging,
    const ASTPtr & query_ast,
    const QueryPipeline & pipeline,
    const std::unique_ptr<IInterpreter> & interpreter,
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
    if (settings.log_formatted_queries)
        elem.formatted_query = queryToString(query_ast);
    elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);
    elem.query_kind = query_ast->getQueryKind();

    elem.client_info = context->getClientInfo();

    if (auto txn = context->getCurrentTransaction())
        elem.tid = txn->tid;

    bool log_queries = settings.log_queries && !internal;

    /// Log into system table start of query execution, if need.
    if (log_queries)
    {
        /// This check is not obvious, but without it 01220_scalar_optimization_in_alter fails.
        if (pipeline.initialized())
        {
            const auto & info = context->getQueryAccessInfo();
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

        if (settings.log_query_settings)
            elem.query_settings = std::make_shared<Settings>(context->getSettingsRef());

        elem.log_comment = settings.log_comment;
        if (elem.log_comment.size() > settings.max_query_size)
            elem.log_comment.resize(settings.max_query_size);

        if (elem.type >= settings.log_queries_min_type && !settings.log_queries_min_query_duration_ms.totalMilliseconds())
        {
            if (auto query_log = context->getQueryLog())
                query_log->add(elem);
        }
    }

    return elem;
}

void logQueryFinish(
    QueryLogElement & elem,
    const ContextMutablePtr & context,
    const ASTPtr & query_ast,
    const QueryPipeline & query_pipeline,
    bool pulling_pipeline,
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span,
    bool internal)
{
    const Settings & settings = context->getSettingsRef();
    auto log_queries = settings.log_queries && !internal;
    auto log_queries_min_type = settings.log_queries_min_type;
    auto log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds();
    auto log_processors_profiles = settings.log_processors_profiles;

    QueryStatusPtr process_list_elem = context->getProcessListElement();
    if (process_list_elem)
    {
        /// Update performance counters before logging to query_log
        CurrentThread::finalizePerformanceCounters();

        QueryStatusInfo info = process_list_elem->getInfo(true, context->getSettingsRef().log_profile_events);
        elem.type = QueryLogElementType::QUERY_FINISH;

        addStatusInfoToQueryElement(elem, info, query_ast, context);

        if (pulling_pipeline)
        {
            query_pipeline.tryGetResultRowsAndBytes(elem.result_rows, elem.result_bytes);
        }
        else /// will be used only for ordinary INSERT queries
        {
            auto progress_out = process_list_elem->getProgressOut();
            elem.result_rows = progress_out.written_rows;
            elem.result_bytes = progress_out.written_bytes;
        }

        auto progress_callback = context->getProgressCallback();
        if (progress_callback)
        {
            Progress p;
            p.incrementPiecewiseAtomically(Progress{ResultProgress{elem.result_rows, elem.result_bytes}});
            progress_callback(p);
        }

        if (elem.read_rows != 0)
        {
            double elapsed_seconds = static_cast<double>(info.elapsed_microseconds) / 1000000.0;
            double rows_per_second = static_cast<double>(elem.read_rows) / elapsed_seconds;
            LOG_DEBUG(
                &Poco::Logger::get("executeQuery"),
                "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
                elem.read_rows,
                ReadableSize(elem.read_bytes),
                elapsed_seconds,
                rows_per_second,
                ReadableSize(elem.read_bytes / elapsed_seconds));
        }

        if (log_queries && elem.type >= log_queries_min_type
            && static_cast<Int64>(elem.query_duration_ms) >= log_queries_min_query_duration_ms)
        {
            if (auto query_log = context->getQueryLog())
                query_log->add(elem);
        }
        if (log_processors_profiles)
        {
            if (auto processors_profile_log = context->getProcessorsProfileLog())
            {
                ProcessorProfileLogElement processor_elem;
                processor_elem.event_time = elem.event_time;
                processor_elem.event_time_microseconds = elem.event_time_microseconds;
                processor_elem.initial_query_id = elem.client_info.initial_query_id;
                processor_elem.query_id = elem.client_info.current_query_id;

                auto get_proc_id = [](const IProcessor & proc) -> UInt64 { return reinterpret_cast<std::uintptr_t>(&proc); };

                for (const auto & processor : query_pipeline.getProcessors())
                {
                    std::vector<UInt64> parents;
                    for (const auto & port : processor->getOutputs())
                    {
                        if (!port.isConnected())
                            continue;
                        const IProcessor & next = port.getInputPort().getProcessor();
                        parents.push_back(get_proc_id(next));
                    }

                    processor_elem.id = get_proc_id(*processor);
                    processor_elem.parent_ids = std::move(parents);

                    processor_elem.plan_step = reinterpret_cast<std::uintptr_t>(processor->getQueryPlanStep());
                    processor_elem.plan_group = processor->getQueryPlanStepGroup();

                    processor_elem.processor_name = processor->getName();

                    /// NOTE: convert this to UInt64
                    processor_elem.elapsed_us = static_cast<UInt32>(processor->getElapsedUs());
                    processor_elem.input_wait_elapsed_us = static_cast<UInt32>(processor->getInputWaitElapsedUs());
                    processor_elem.output_wait_elapsed_us = static_cast<UInt32>(processor->getOutputWaitElapsedUs());

                    auto stats = processor->getProcessorDataStats();
                    processor_elem.input_rows = stats.input_rows;
                    processor_elem.input_bytes = stats.input_bytes;
                    processor_elem.output_rows = stats.output_rows;
                    processor_elem.output_bytes = stats.output_bytes;

                    processors_profile_log->add(processor_elem);
                }
            }
        }
    }

    if (query_span)
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
        query_span->finish();
    }
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
    auto log_queries = settings.log_queries && !internal;
    auto log_queries_min_type = settings.log_queries_min_type;
    auto log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds();

    elem.type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;
    elem.exception_code = getCurrentExceptionCode();
    auto exception_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false);
    elem.exception = std::move(exception_message.text);
    elem.exception_format_string = exception_message.format_string;

    QueryStatusPtr process_list_elem = context->getProcessListElement();

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();
    const auto time_now = std::chrono::system_clock::now();
    elem.event_time = timeInSeconds(time_now);
    elem.event_time_microseconds = timeInMicroseconds(time_now);

    if (process_list_elem)
    {
        QueryStatusInfo info = process_list_elem->getInfo(true, settings.log_profile_events, false);
        addStatusInfoToQueryElement(elem, info, query_ast, context);
    }
    else
    {
        elem.query_duration_ms = start_watch.elapsedMilliseconds();
    }

    if (settings.calculate_text_stack_trace && log_error)
        setExceptionStackTrace(elem);
    logException(context, elem, log_error);

    /// In case of exception we log internal queries also
    if (log_queries && elem.type >= log_queries_min_type && static_cast<Int64>(elem.query_duration_ms) >= log_queries_min_query_duration_ms)
    {
        if (auto query_log = context->getQueryLog())
            query_log->add(elem);
    }

    ProfileEvents::increment(ProfileEvents::FailedQuery);
    if (query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
        ProfileEvents::increment(ProfileEvents::FailedSelectQuery);
    else if (query_ast->as<ASTInsertQuery>())
        ProfileEvents::increment(ProfileEvents::FailedInsertQuery);

    if (query_span)
    {
        query_span->addAttribute("db.statement", elem.query);
        query_span->addAttribute("clickhouse.query_id", elem.client_info.current_query_id);
        query_span->addAttribute("clickhouse.exception", elem.exception);
        query_span->addAttribute("clickhouse.exception_code", elem.exception_code);
        query_span->finish();
    }
}

void logExceptionBeforeStart(
    const String & query_for_logging,
    ContextPtr context,
    ASTPtr ast,
    const std::shared_ptr<OpenTelemetry::SpanHolder> & query_span,
    UInt64 elapsed_millliseconds)
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
    elem.query_duration_ms = elapsed_millliseconds;

    elem.current_database = context->getCurrentDatabase();
    elem.query = query_for_logging;
    elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);

    // Log query_kind if ast is valid
    if (ast)
    {
        elem.query_kind = ast->getQueryKind();
        if (settings.log_formatted_queries)
            elem.formatted_query = queryToString(ast);
    }

    // We don't calculate databases, tables and columns when the query isn't able to start

    elem.exception_code = getCurrentExceptionCode();
    auto exception_message = getCurrentExceptionMessageAndPattern(/* with_stacktrace */ false);
    elem.exception = std::move(exception_message.text);
    elem.exception_format_string = exception_message.format_string;

    elem.client_info = context->getClientInfo();

    elem.log_comment = settings.log_comment;
    if (elem.log_comment.size() > settings.max_query_size)
        elem.log_comment.resize(settings.max_query_size);

    if (auto txn = context->getCurrentTransaction())
        elem.tid = txn->tid;

    if (settings.calculate_text_stack_trace)
        setExceptionStackTrace(elem);
    logException(context, elem);

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    if (settings.log_queries && elem.type >= settings.log_queries_min_type && !settings.log_queries_min_query_duration_ms.totalMilliseconds())
        if (auto query_log = context->getQueryLog())
            query_log->add(elem);

    if (query_span)
    {
        query_span->addAttribute("clickhouse.exception_code", elem.exception_code);
        query_span->addAttribute("clickhouse.exception", elem.exception);
        query_span->addAttribute("db.statement", elem.query);
        query_span->addAttribute("clickhouse.query_id", elem.client_info.current_query_id);
        query_span->finish();
    }

    ProfileEvents::increment(ProfileEvents::FailedQuery);

    if (ast)
    {
        if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
        {
            ProfileEvents::increment(ProfileEvents::FailedSelectQuery);
        }
        else if (ast->as<ASTInsertQuery>())
        {
            ProfileEvents::increment(ProfileEvents::FailedInsertQuery);
        }
    }
}

static void setQuerySpecificSettings(ASTPtr & ast, ContextMutablePtr context)
{
    if (auto * ast_insert_into = ast->as<ASTInsertQuery>())
    {
        if (ast_insert_into->watch)
            context->setSetting("output_format_enable_streaming", 1);
    }
}

static std::tuple<ASTPtr, BlockIO> executeQueryImpl(
    const char * begin,
    const char * end,
    ContextMutablePtr context,
    bool internal,
    QueryProcessingStage::Enum stage,
    ReadBuffer * istr)
{
    /// query_span is a special span, when this function exits, it's lifetime is not ended, but ends when the query finishes.
    /// Some internal queries might call this function recursively by setting 'internal' parameter to 'true',
    /// to make sure SpanHolders in current stack ends in correct order, we disable this span for these internal queries
    ///
    /// This does not have impact on the final span logs, because these internal queries are issued by external queries,
    /// we still have enough span logs for the execution of external queries.
    std::shared_ptr<OpenTelemetry::SpanHolder> query_span = internal ? nullptr : std::make_shared<OpenTelemetry::SpanHolder>("query");
    if (query_span && query_span->trace_id != UUID{})
        LOG_TRACE(&Poco::Logger::get("executeQuery"), "Query span trace_id for opentelemetry log: {}", query_span->trace_id);

    auto query_start_time = std::chrono::system_clock::now();

    /// Used to set the watch in QueryStatus and the output formats. It is not based on query_start_time as that might be based on
    /// the value passed by the client
    Stopwatch start_watch{CLOCK_MONOTONIC};

    const auto & client_info = context->getClientInfo();

    if (!internal)
    {
        // If it's not an internal query and we don't see an initial_query_start_time yet, initialize it
        // to current time. Internal queries are those executed without an independent client context,
        // thus should not set initial_query_start_time, because it might introduce data race. It's also
        // possible to have unset initial_query_start_time for non-internal and non-initial queries. For
        // example, the query is from an initiator that is running an old version of clickhouse.
        // On the other hand, if it's initialized then take it as the start of the query
        if (client_info.initial_query_start_time == 0)
        {
            context->setInitialQueryStartTime(query_start_time);
        }
        else
        {
            query_start_time = std::chrono::time_point<std::chrono::system_clock>(
                std::chrono::microseconds{client_info.initial_query_start_time_microseconds});
        }
    }

    assert(internal || CurrentThread::get().getQueryContext());
    assert(internal || CurrentThread::get().getQueryContext()->getCurrentQueryId() == CurrentThread::getQueryId());

    const Settings & settings = context->getSettingsRef();

    size_t max_query_size = settings.max_query_size;
    /// Don't limit the size of internal queries or distributed subquery.
    if (internal || client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        max_query_size = 0;

    ASTPtr ast;
    String query;
    String query_for_logging;
    size_t log_queries_cut_to_length = context->getSettingsRef().log_queries_cut_to_length;

    /// Parse the query from string.
    try
    {
        if (settings.dialect == Dialect::kusto && !internal)
        {
            ParserKQLStatement parser(end, settings.allow_settings_after_format_in_insert);

            /// TODO: parser should fail early when max_query_size limit is reached.
            ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);
        }
        else if (settings.dialect == Dialect::prql && !internal)
        {
            ParserPRQLQuery parser(max_query_size, settings.max_parser_depth);
            ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);
        }
        else
        {
            ParserQuery parser(end, settings.allow_settings_after_format_in_insert);
            /// TODO: parser should fail early when max_query_size limit is reached.
            ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);
        }

        const char * query_end = end;
        if (const auto * insert_query = ast->as<ASTInsertQuery>(); insert_query && insert_query->data)
            query_end = insert_query->data;

        bool is_create_parameterized_view = false;
        if (const auto * create_query = ast->as<ASTCreateQuery>())
            is_create_parameterized_view = create_query->isParameterizedView();

        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        if (!is_create_parameterized_view && context->hasQueryParameters())
        {
            ReplaceQueryParameterVisitor visitor(context->getQueryParameters());
            visitor.visit(ast);
            query = serializeAST(*ast);
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
        if (ast->hasSecretParts())
        {
            /// IAST::formatForLogging() wipes secret parts in AST and then calls wipeSensitiveDataAndCutToLength().
            query_for_logging = ast->formatForLogging(log_queries_cut_to_length);
        }
        else
        {
            query_for_logging = wipeSensitiveDataAndCutToLength(query, log_queries_cut_to_length);
        }
    }
    catch (...)
    {
        /// Anyway log the query.
        if (query.empty())
            query.assign(begin, std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));

        query_for_logging = wipeSensitiveDataAndCutToLength(query, log_queries_cut_to_length);
        logQuery(query_for_logging, context, internal, stage);

        if (!internal)
            logExceptionBeforeStart(query_for_logging, context, ast, query_span, start_watch.elapsedMilliseconds());
        throw;
    }

    /// Avoid early destruction of process_list_entry if it was not saved to `res` yet (in case of exception)
    ProcessList::EntryPtr process_list_entry;
    BlockIO res;
    auto implicit_txn_control = std::make_shared<bool>(false);
    String query_database;
    String query_table;

    auto execute_implicit_tcl_query = [implicit_txn_control](const ContextMutablePtr & query_context, ASTTransactionControl::QueryType tcl_type)
    {
        /// Unset the flag on COMMIT and ROLLBACK
        SCOPE_EXIT({ if (tcl_type != ASTTransactionControl::BEGIN) *implicit_txn_control = false; });

        ASTPtr tcl_ast = std::make_shared<ASTTransactionControl>(tcl_type);
        InterpreterTransactionControlQuery tc(tcl_ast, query_context);
        tc.execute();

        /// Set the flag after successful BIGIN
        if (tcl_type == ASTTransactionControl::BEGIN)
            *implicit_txn_control = true;
    };

    try
    {
        if (auto txn = context->getCurrentTransaction())
        {
            chassert(txn->getState() != MergeTreeTransaction::COMMITTING);
            chassert(txn->getState() != MergeTreeTransaction::COMMITTED);
            if (txn->getState() == MergeTreeTransaction::ROLLED_BACK && !ast->as<ASTTransactionControl>() && !ast->as<ASTExplainQuery>())
                throw Exception(
                    ErrorCodes::INVALID_TRANSACTION,
                    "Cannot execute query because current transaction failed. Expecting ROLLBACK statement");
        }

        /// Interpret SETTINGS clauses as early as possible (before invoking the corresponding interpreter),
        /// to allow settings to take effect.
        InterpreterSetQuery::applySettingsFromQuery(ast, context);

        if (auto * insert_query = ast->as<ASTInsertQuery>())
            insert_query->tail = istr;

        setQuerySpecificSettings(ast, context);

        /// There is an option of probabilistic logging of queries.
        /// If it is used - do the random sampling and "collapse" the settings.
        /// It allows to consistently log queries with all the subqueries in distributed query processing
        /// (subqueries on remote nodes will receive these "collapsed" settings)
        if (!internal && settings.log_queries && settings.log_queries_probability < 1.0)
        {
            std::bernoulli_distribution should_write_log{settings.log_queries_probability};

            context->setSetting("log_queries", should_write_log(thread_local_rng));
            context->setSetting("log_queries_probability", 1.0);
        }

        if (const auto * query_with_table_output = dynamic_cast<const ASTQueryWithTableAndOutput *>(ast.get()))
        {
            query_database = query_with_table_output->getDatabase();
            query_table = query_with_table_output->getTable();
        }

        logQuery(query_for_logging, context, internal, stage);

        /// Propagate WITH statement to children ASTSelect.
        if (settings.enable_global_with_statement)
        {
            ApplyWithGlobalVisitor().visit(ast);
        }

        {
            SelectIntersectExceptQueryVisitor::Data data{settings.intersect_default_mode, settings.except_default_mode};
            SelectIntersectExceptQueryVisitor{data}.visit(ast);
        }

        {
            /// Normalize SelectWithUnionQuery
            NormalizeSelectWithUnionQueryVisitor::Data data{settings.union_default_mode};
            NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);
        }

        /// Check the limits.
        checkASTSizeLimits(*ast, settings);

        /// Put query to process list. But don't put SHOW PROCESSLIST query itself.
        if (!internal && !ast->as<ASTShowProcesslistQuery>())
        {
            /// processlist also has query masked now, to avoid secrets leaks though SHOW PROCESSLIST by other users.
            process_list_entry = context->getProcessList().insert(query_for_logging, ast.get(), context, start_watch.getStart());
            context->setProcessListElement(process_list_entry->getQueryStatus());
        }

        /// Load external tables if they were provided
        context->initializeExternalTablesIfSet();

        auto * insert_query = ast->as<ASTInsertQuery>();
        bool async_insert_enabled = settings.async_insert;

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
            if (istr)
            {
                ASTPtr input_function;
                insert_query->tryFindInputFunction(input_function);
                if (input_function)
                {
                    StoragePtr storage = context->executeTableFunction(input_function, insert_query->select->as<ASTSelectQuery>());
                    auto & input_storage = dynamic_cast<StorageInput &>(*storage);
                    auto input_metadata_snapshot = input_storage.getInMemoryMetadataPtr();
                    auto pipe = getSourceFromASTInsertQuery(
                        ast, true, input_metadata_snapshot->getSampleBlock(), context, input_function);
                    input_storage.setPipe(std::move(pipe));
                }
            }
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
        auto * queue = context->getAsynchronousInsertQueue();
        auto * logger = &Poco::Logger::get("executeQuery");

        if (insert_query && async_insert_enabled)
        {
            String reason;

            if (!queue)
                reason = "asynchronous insert queue is not configured";
            else if (insert_query->select)
                reason = "insert query has select";
            else if (!insert_query->hasInlinedData())
                reason = "insert query doesn't have inlined data";
            else
                async_insert = true;

            if (!async_insert)
                LOG_DEBUG(logger, "Setting async_insert=1, but INSERT query will be executed synchronously (reason: {})", reason);
        }

        bool quota_checked = false;
        std::unique_ptr<ReadBuffer> insert_data_buffer_holder;

        if (async_insert)
        {
            if (context->getCurrentTransaction() && settings.throw_on_unsupported_query_inside_transaction)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async inserts inside transactions are not supported");
            if (settings.implicit_transaction && settings.throw_on_unsupported_query_inside_transaction)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async inserts with 'implicit_transaction' are not supported");

            quota = context->getQuota();
            if (quota)
            {
                quota_checked = true;
                quota->used(QuotaType::QUERY_INSERTS, 1);
                quota->used(QuotaType::QUERIES, 1);
                quota->checkExceeded(QuotaType::ERRORS);
            }

            auto result = queue->push(ast, context);

            if (result.status == AsynchronousInsertQueue::PushResult::OK)
            {
                if (settings.wait_for_async_insert)
                {
                    auto timeout = settings.wait_for_async_insert_timeout.totalMilliseconds();
                    auto source = std::make_shared<WaitForAsyncInsertSource>(std::move(result.future), timeout);
                    res.pipeline = QueryPipeline(Pipe(std::move(source)));
                }

                const auto & table_id = insert_query->table_id;
                if (!table_id.empty())
                    context->setInsertionTable(table_id);
            }
            else if (result.status == AsynchronousInsertQueue::PushResult::TOO_MUCH_DATA)
            {
                async_insert = false;
                insert_data_buffer_holder = std::move(result.insert_data_buffer);

                if (insert_query->data)
                {
                    /// Reset inlined data because it will be
                    /// available from tail read buffer.
                    insert_query->end = insert_query->data;
                    insert_query->data = nullptr;
                }

                insert_query->tail = insert_data_buffer_holder.get();
                LOG_DEBUG(logger, "Setting async_insert=1, but INSERT query will be executed synchronously because it has too much data");
            }
        }

        QueryCachePtr query_cache = context->getQueryCache();
        const bool can_use_query_cache = query_cache != nullptr && settings.use_query_cache && !internal && (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>());
        bool write_into_query_cache = false;

        if (!async_insert)
        {
            /// If it is a non-internal SELECT, and passive/read use of the query cache is enabled, and the cache knows the query, then set
            /// a pipeline with a source populated by the query cache.
            auto get_result_from_query_cache = [&]()
            {
                if (can_use_query_cache && settings.enable_reads_from_query_cache)
                {
                    QueryCache::Key key(ast, context->getUserName());
                    QueryCache::Reader reader = query_cache->createReader(key);
                    if (reader.hasCacheEntryForKey())
                    {
                        QueryPipeline pipeline;
                        pipeline.readFromQueryCache(reader.getSource(), reader.getSourceTotals(), reader.getSourceExtremes());
                        res.pipeline = std::move(pipeline);
                        return true;
                    }
                }
                return false;
            };

            if (!get_result_from_query_cache())
            {
                /// We need to start the (implicit) transaction before getting the interpreter as this will get links to the latest snapshots
                if (!context->getCurrentTransaction() && settings.implicit_transaction && !ast->as<ASTTransactionControl>())
                {
                    try
                    {
                        if (context->isGlobalContext())
                            throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot create transactions");

                        execute_implicit_tcl_query(context, ASTTransactionControl::BEGIN);
                    }
                    catch (Exception & e)
                    {
                        e.addMessage("while starting a transaction with 'implicit_transaction'");
                        throw;
                    }
                }

                interpreter = InterpreterFactory::get(ast, context, SelectQueryOptions(stage).setInternal(internal));

                const auto & query_settings = context->getSettingsRef();
                if (context->getCurrentTransaction() && query_settings.throw_on_unsupported_query_inside_transaction)
                {
                    if (!interpreter->supportsTransactions())
                        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported for this type of query ({})", ast->getID());

                }

                if (!interpreter->ignoreQuota() && !quota_checked)
                {
                    quota = context->getQuota();
                    if (quota)
                    {
                        if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
                        {
                            quota->used(QuotaType::QUERY_SELECTS, 1);
                        }
                        else if (ast->as<ASTInsertQuery>())
                        {
                            quota->used(QuotaType::QUERY_INSERTS, 1);
                        }
                        quota->used(QuotaType::QUERIES, 1);
                        quota->checkExceeded(QuotaType::ERRORS);
                    }
                }

                if (!interpreter->ignoreLimits())
                {
                    limits.mode = LimitsMode::LIMITS_CURRENT;
                    limits.size_limits = SizeLimits(settings.max_result_rows, settings.max_result_bytes, settings.result_overflow_mode);
                }

                if (auto * insert_interpreter = typeid_cast<InterpreterInsertQuery *>(&*interpreter))
                {
                    /// Save insertion table (not table function). TODO: support remote() table function.
                    auto table_id = insert_interpreter->getDatabaseTable();
                    if (!table_id.empty())
                        context->setInsertionTable(std::move(table_id));

                    if (insert_data_buffer_holder)
                        insert_interpreter->addBuffer(std::move(insert_data_buffer_holder));
                }

                {
                    std::unique_ptr<OpenTelemetry::SpanHolder> span;
                    if (OpenTelemetry::CurrentContext().isTraceEnabled())
                    {
                        auto * raw_interpreter_ptr = interpreter.get();
                        String class_name(demangle(typeid(*raw_interpreter_ptr).name()));
                        span = std::make_unique<OpenTelemetry::SpanHolder>(class_name + "::execute()");
                    }

                    res = interpreter->execute();

                    /// If it is a non-internal SELECT query, and active/write use of the query cache is enabled, then add a processor on
                    /// top of the pipeline which stores the result in the query cache.
                    if (can_use_query_cache && settings.enable_writes_to_query_cache
                        && (!astContainsNonDeterministicFunctions(ast, context) || settings.query_cache_store_results_of_queries_with_nondeterministic_functions))
                    {
                        QueryCache::Key key(
                            ast, res.pipeline.getHeader(),
                            context->getUserName(), settings.query_cache_share_between_users,
                            std::chrono::system_clock::now() + std::chrono::seconds(settings.query_cache_ttl),
                            settings.query_cache_compress_entries);

                        const size_t num_query_runs = query_cache->recordQueryRun(key);
                        if (num_query_runs > settings.query_cache_min_query_runs)
                        {
                            auto query_cache_writer = std::make_shared<QueryCache::Writer>(query_cache->createWriter(
                                             key,
                                             std::chrono::milliseconds(settings.query_cache_min_query_duration.totalMilliseconds()),
                                             settings.query_cache_squash_partial_results,
                                             settings.max_block_size,
                                             settings.query_cache_max_size_in_bytes,
                                             settings.query_cache_max_entries));
                            res.pipeline.writeResultIntoQueryCache(query_cache_writer);
                            write_into_query_cache = true;
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
        res.process_list_entry = process_list_entry;

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
                ast,
                pipeline,
                interpreter,
                internal,
                query_database,
                query_table,
                async_insert);
            /// Also make possible for caller to log successful query finish and exception during execution.
            auto finish_callback = [elem,
                                    context,
                                    ast,
                                    write_into_query_cache,
                                    internal,
                                    implicit_txn_control,
                                    execute_implicit_tcl_query,
                                    pulling_pipeline = pipeline.pulling(),
                                    query_span](QueryPipeline & query_pipeline) mutable
            {
                if (write_into_query_cache)
                    /// Trigger the actual write of the buffered query result into the query cache. This is done explicitly to prevent
                    /// partial/garbage results in case of exceptions during query execution.
                    query_pipeline.finalizeWriteInQueryCache();

                logQueryFinish(elem, context, ast, query_pipeline, pulling_pipeline, query_span, internal);

                if (*implicit_txn_control)
                    execute_implicit_tcl_query(context, ASTTransactionControl::COMMIT);
            };

            auto exception_callback =
                [start_watch, elem, context, ast, internal, my_quota(quota), implicit_txn_control, execute_implicit_tcl_query, query_span](
                    bool log_error) mutable
            {
                if (*implicit_txn_control)
                    execute_implicit_tcl_query(context, ASTTransactionControl::ROLLBACK);
                else if (auto txn = context->getCurrentTransaction())
                    txn->onException();

                if (my_quota)
                    my_quota->used(QuotaType::ERRORS, 1, /* check_exceeded = */ false);

                logQueryException(elem, context, start_watch, ast, query_span, internal, log_error);
            };

            res.finish_callback = std::move(finish_callback);
            res.exception_callback = std::move(exception_callback);
        }
    }
    catch (...)
    {
        if (*implicit_txn_control)
            execute_implicit_tcl_query(context, ASTTransactionControl::ROLLBACK);
        else if (auto txn = context->getCurrentTransaction())
            txn->onException();

        if (!internal)
            logExceptionBeforeStart(query_for_logging, context, ast, query_span, start_watch.elapsedMilliseconds());

        throw;
    }

    return std::make_tuple(ast, std::move(res));
}


BlockIO executeQuery(
    const String & query,
    ContextMutablePtr context,
    bool internal,
    QueryProcessingStage::Enum stage)
{
    ASTPtr ast;
    BlockIO streams;
    std::tie(ast, streams) = executeQueryImpl(query.data(), query.data() + query.size(), context, internal, stage, nullptr);

    if (const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
    {
        String format_name = ast_query_with_output->format
                ? getIdentifierName(ast_query_with_output->format)
                : context->getDefaultFormat();

        if (format_name == "Null")
            streams.null_format = true;
    }

    return streams;
}

BlockIO executeQuery(
    bool allow_processors,
    const String & query,
    ContextMutablePtr context,
    bool internal,
    QueryProcessingStage::Enum stage)
{
    if (!allow_processors)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Flag allow_processors is deprecated for executeQuery");

    return executeQuery(query, context, internal, stage);
}


void executeQuery(
    ReadBuffer & istr,
    WriteBuffer & ostr,
    bool allow_into_outfile,
    ContextMutablePtr context,
    SetResultDetailsFunc set_result_details,
    const std::optional<FormatSettings> & output_format_settings)
{
    PODArray<char> parse_buf;
    const char * begin;
    const char * end;

    istr.nextIfAtEnd();

    size_t max_query_size = context->getSettingsRef().max_query_size;

    if (istr.buffer().end() - istr.position() > static_cast<ssize_t>(max_query_size))
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        begin = istr.position();
        end = istr.buffer().end();
        istr.position() += end - begin;
    }
    else
    {
        /// FIXME: this is an extra copy not required for async insertion.

        /// If not - copy enough data into 'parse_buf'.
        WriteBufferFromVector<PODArray<char>> out(parse_buf);
        LimitReadBuffer limit(istr, max_query_size + 1, /* trow_exception */ false, /* exact_limit */ {});
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
        if (set_result_details == nullptr)
            /// Either the result_details have been set in the flow below or the caller of this function does not provide this callback
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

    std::tie(ast, streams) = executeQueryImpl(begin, end, context, false, QueryProcessingStage::Complete, &istr);
    auto & pipeline = streams.pipeline;

    std::unique_ptr<WriteBuffer> compressed_buffer;
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

            WriteBuffer * out_buf = &ostr;
            if (ast_query_with_output && ast_query_with_output->out_file)
            {
                if (!allow_into_outfile)
                    throw Exception(ErrorCodes::INTO_OUTFILE_NOT_ALLOWED, "INTO OUTFILE is not allowed");

                const auto & out_file = typeid_cast<const ASTLiteral &>(*ast_query_with_output->out_file).value.safeGet<std::string>();

                std::string compression_method;
                if (ast_query_with_output->compression)
                {
                    const auto & compression_method_node = ast_query_with_output->compression->as<ASTLiteral &>();
                    compression_method = compression_method_node.value.safeGet<std::string>();
                }

                compressed_buffer = wrapWriteBufferWithCompressionMethod(
                    std::make_unique<WriteBufferFromFile>(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT),
                    chooseCompressionMethod(out_file, compression_method),
                    /* compression level = */ 3
                );
            }

            String format_name = ast_query_with_output && (ast_query_with_output->format != nullptr)
                                    ? getIdentifierName(ast_query_with_output->format)
                                    : context->getDefaultFormat();

            auto out = FormatFactory::instance().getOutputFormatParallelIfPossible(
                format_name,
                compressed_buffer ? *compressed_buffer : *out_buf,
                materializeBlock(pipeline.getHeader()),
                context,
                output_format_settings);

            out->setAutoFlush();

            /// Save previous progress callback if any. TODO Do it more conveniently.
            auto previous_progress_callback = context->getProgressCallback();

            /// NOTE Progress callback takes shared ownership of 'out'.
            pipeline.setProgressCallback([out, previous_progress_callback] (const Progress & progress)
            {
                if (previous_progress_callback)
                    previous_progress_callback(progress);
                out->onProgress(progress);
            });

            result_details.content_type = out->getContentType();
            result_details.format = format_name;

            pipeline.complete(std::move(out));
        }
        else
        {
            pipeline.setProgressCallback(context->getProgressCallback());
        }

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
    }
    catch (...)
    {
        streams.onException();
        throw;
    }

    streams.onFinish();
}

void executeTrivialBlockIO(BlockIO & streams, ContextPtr context)
{
    try
    {
        if (!streams.pipeline.initialized())
            return;

        if (!streams.pipeline.completed())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Query pipeline requires output, but no output buffer provided, it's a bug");

        streams.pipeline.setProgressCallback(context->getProgressCallback());
        CompletedPipelineExecutor executor(streams.pipeline);
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
