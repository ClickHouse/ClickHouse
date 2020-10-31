#include <Interpreters/QueryProcess.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <DataStreams/CountingBlockOutputStream.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/queryNormalization.h>
#include <Parsers/queryToString.h>
#include <Access/EnabledQuota.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ProfileEvents.h>

namespace ProfileEvents
{
    extern const Event QueryMaskingRulesMatch;
    extern const Event FailedQuery;
    extern const Event FailedInsertQuery;
    extern const Event FailedSelectQuery;
    extern const Event QueryTimeMicroseconds;
    extern const Event SelectQueryTimeMicroseconds;
    extern const Event InsertQueryTimeMicroseconds;
}

namespace
{

using namespace DB;

String joinLines(const String & query)
{
    /// Care should be taken. We don't join lines inside non-whitespace tokens (e.g. multiline string literals)
    ///  and we don't join line after comment (because it can be single-line comment).
    /// All other whitespaces replaced to a single whitespace.

    String res;
    const char * begin = query.data();
    const char * end = begin + query.size();

    Lexer lexer(begin, end);
    Token token = lexer.nextToken();
    for (; !token.isEnd(); token = lexer.nextToken())
    {
        if (token.type == TokenType::Whitespace)
        {
            res += ' ';
        }
        else if (token.type == TokenType::Comment)
        {
            res.append(token.begin, token.end);
            if (token.end < end && *token.end == '\n')
                res += '\n';
        }
        else
            res.append(token.begin, token.end);
    }

    return res;
}

/// Call this inside catch block.
void setExceptionStackTrace(QueryLogElement & elem)
{
    /// Disable memory tracker for stack trace.
    /// Because if exception is "Memory limit (for query) exceed", then we probably can't allocate another one string.
    MemoryTracker::BlockerInThread temporarily_disable_memory_tracker(VariableContext::Global);

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
void logException(Context & context, QueryLogElement & elem)
{
    String comment;
    if (!elem.log_comment.empty())
        comment = fmt::format(" (comment: {})", elem.log_comment);

    if (elem.stack_trace.empty())
        LOG_ERROR(&Poco::Logger::get("executeQuery"), "{} (from {}){} (in query: {})",
            elem.exception, context.getClientInfo().current_address.toString(), comment, joinLines(elem.query));
    else
        LOG_ERROR(&Poco::Logger::get("executeQuery"), "{} (from {}){} (in query: {})"
            ", Stack trace (when copying this message, always include the lines below):\n\n{}",
            elem.exception, context.getClientInfo().current_address.toString(), comment, joinLines(elem.query), elem.stack_trace);
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}

inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

}

namespace DB
{

QueryProcess::QueryProcess(const ASTPtr & ast_, bool internal_, Context & context_)
    : QueryProcess(prepareQueryForLogging(queryToString(ast_), context_), ast_, internal_, prepareContext(context_))
{
    logQuery(query_for_logging, context, internal);
}
QueryProcess::QueryProcess(const String & query_for_logging_, const ASTPtr & ast_, bool internal_, Context & context_)
    : context(context_)
    , settings(context.getSettingsRef())
    , query_for_logging(query_for_logging_)
    , ast(ast_)
    , process_list_entry(context.getProcessList().insert(query_for_logging, ast.get(), context))
    , parent_process_list_element(context.getProcessListElement())
    , internal(internal_)
    , log_queries(settings.log_queries && !internal)
    , current_time(std::chrono::system_clock::now())
    , query_log_element(makeQueryLogElement())
{
    /// FIXME: misses setProcessListElement() for other bits:
    /// - QueryPipline
    /// - BlockInputStream
    /// - CountingBlockOutputStream
    context.setProcessListElement(&process_list_entry->get());

    /// NOTE: There is no need in ThreadStatus/QueryScope adjustment because:
    /// - executeQuery() creates separate ThreadStatus/QueryScope by itself.
    /// - PullingAsyncPipelineExecutor:
    ///   - ThreadStatus will be created in ThreadFromGlobalPool
    ///   - QueryScope will be configured properly with CurrentThread::attachTo()

    if (const auto * query_with_table_output = dynamic_cast<const ASTQueryWithTableAndOutput *>(ast.get()))
    {
        query_database = query_with_table_output->database;
        query_table = query_with_table_output->table;
    }
}

QueryProcess::~QueryProcess()
{
    if (parent_process_list_element)
        context.setProcessListElement(parent_process_list_element);
}

void QueryProcess::queryExceptionBeforeStart()
{
    onExceptionBeforeStart(query_for_logging, context, current_time, ast);
}

void QueryProcess::queryStart(bool use_processors, IInterpreter * interpreter)
{
    if (!log_queries)
        return;

    query_log_element.type = QueryLogElementType::QUERY_START;

    // construct event_time and event_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    const auto start_time = std::chrono::system_clock::now();
    query_log_element.event_time = time_in_seconds(start_time);
    query_log_element.event_time_microseconds = time_in_microseconds(start_time);

    if (use_processors)
    {
        const auto & info = context.getQueryAccessInfo();
        query_log_element.query_databases = info.databases;
        query_log_element.query_tables = info.tables;
        query_log_element.query_columns = info.columns;
    }

    /// FIXME: not full query_log entry (for subqueries)
    if (interpreter)
        interpreter->extendQueryLogElem(query_log_element, ast, context, query_database, query_table);

    if (settings.log_query_settings)
        query_log_element.query_settings = std::make_shared<Settings>(context.getSettingsRef());

    query_log_element.log_comment = settings.log_comment;
    if (query_log_element.log_comment.size() > settings.max_query_size)
        query_log_element.log_comment.resize(settings.max_query_size);

    if (query_log_element.type >= settings.log_queries_min_type && !settings.log_queries_min_query_duration_ms.totalMilliseconds())
    {
        if (auto query_log = context.getQueryLog())
            query_log->add(query_log_element);
    }
}

void QueryProcess::queryFinish(IBlockInputStream * stream_in, IBlockOutputStream * stream_out, QueryPipeline * query_pipeline)
{
    QueryStatus & query_status = queryStatus();

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    QueryStatusInfo info = query_status.getInfo(true, context.getSettingsRef().log_profile_events);

    double elapsed_seconds = info.elapsed_seconds;

    query_log_element.type = QueryLogElementType::QUERY_FINISH;

    // construct event_time and event_time_microseconds using the same time point
    // so that the two times will always be equal up to a precision of a second.
    const auto finish_time = std::chrono::system_clock::now();
    query_log_element.event_time = time_in_seconds(finish_time);
    query_log_element.event_time_microseconds = time_in_microseconds(finish_time);
    addQueryStatusInfoToQueryLogElement(info, ast);

    auto progress_callback = context.getProgressCallback();

    if (progress_callback)
        progress_callback(Progress(WriteProgress(info.written_rows, info.written_bytes)));

    if (stream_in)
    {
        const BlockStreamProfileInfo & stream_in_info = stream_in->getProfileInfo();

        /// NOTE: INSERT SELECT query contains zero metrics
        query_log_element.result_rows = stream_in_info.rows;
        query_log_element.result_bytes = stream_in_info.bytes;
    }
    else if (stream_out) /// will be used only for ordinary INSERT queries
    {
        if (const auto * counting_stream = dynamic_cast<const CountingBlockOutputStream *>(stream_out))
        {
            /// NOTE: Redundancy.
            /// The same values could be extracted from
            /// query_status.progress_out.query_settings = query_status.progress_in
            query_log_element.result_rows = counting_stream->getProgress().read_rows;
            query_log_element.result_bytes = counting_stream->getProgress().read_bytes;
        }
    }
    else if (query_pipeline)
    {
        if (const auto * output_format = query_pipeline->getOutputFormat())
        {
            query_log_element.result_rows = output_format->getResultRows();
            query_log_element.result_bytes = output_format->getResultBytes();
        }
    }

    if (query_log_element.read_rows != 0)
    {
        LOG_INFO(&Poco::Logger::get("executeQuery"), "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
            query_log_element.read_rows, ReadableSize(query_log_element.read_bytes), elapsed_seconds,
            static_cast<size_t>(query_log_element.read_rows / elapsed_seconds),
            ReadableSize(query_log_element.read_bytes / elapsed_seconds));
    }

    query_log_element.thread_ids = std::move(info.thread_ids);
    query_log_element.profile_counters = std::move(info.profile_counters);

    const auto & factories_info = context.getQueryFactoriesInfo();
    query_log_element.used_aggregate_functions = factories_info.aggregate_functions;
    query_log_element.used_aggregate_function_combinators = factories_info.aggregate_function_combinators;
    query_log_element.used_database_engines = factories_info.database_engines;
    query_log_element.used_data_type_families = factories_info.data_type_families;
    query_log_element.used_dictionaries = factories_info.dictionaries;
    query_log_element.used_formats = factories_info.formats;
    query_log_element.used_functions = factories_info.functions;
    query_log_element.used_storages = factories_info.storages;
    query_log_element.used_table_functions = factories_info.table_functions;

    const auto log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds();
    if (log_queries && query_log_element.type >= settings.log_queries_min_type && Int64(query_log_element.query_duration_ms) >= log_queries_min_query_duration_ms)
    {
        if (auto query_log = context.getQueryLog())
            query_log->add(query_log_element);
    }

    if (auto opentelemetry_span_log = context.getOpenTelemetrySpanLog();
        context.query_trace_context.trace_id
            && opentelemetry_span_log)
    {
        OpenTelemetrySpanLogElement span;
        span.trace_id = context.query_trace_context.trace_id;
        span.span_id = context.query_trace_context.span_id;
        span.parent_span_id = context.getClientInfo().client_trace_context.span_id;
        span.operation_name = "query";
        span.start_time_us = query_log_element.query_start_time_microseconds;
        span.finish_time_us = time_in_microseconds(finish_time);

        /// Keep values synchronized to type enum in QueryLogElement::createBlock.
        span.attribute_names.push_back("clickhouse.query_status");
        span.attribute_values.push_back("QueryFinish");

        span.attribute_names.push_back("db.statement");
        span.attribute_values.push_back(query_log_element.query);

        span.attribute_names.push_back("clickhouse.query_id");
        span.attribute_values.push_back(query_log_element.client_info.current_query_id);
        if (!context.query_trace_context.tracestate.empty())
        {
            span.attribute_names.push_back("clickhouse.tracestate");
            span.attribute_values.push_back(
                context.query_trace_context.tracestate);
        }

        opentelemetry_span_log->add(span);
    }
}

void QueryProcess::queryException()
{
    if (auto quota = context.getQuota())
        quota->used(Quota::ERRORS, 1, /* check_exceeded = */ false);

    query_log_element.type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;

    // event_time and event_time_microseconds are being constructed from the same time point
    // to ensure that both the times will be equal up to the precision of a second.
    const auto time_now = std::chrono::system_clock::now();

    query_log_element.event_time = time_in_seconds(time_now);
    query_log_element.event_time_microseconds = time_in_microseconds(time_now);
    query_log_element.query_duration_ms = 1000 * (query_log_element.event_time - query_log_element.query_start_time);
    query_log_element.exception_code = getCurrentExceptionCode();
    query_log_element.exception = getCurrentExceptionMessage(false);

    QueryStatus * process_list_query_log_element = context.getProcessListElement();
    const Settings & current_settings = context.getSettingsRef();

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    if (process_list_query_log_element)
    {
        QueryStatusInfo info = process_list_query_log_element->getInfo(true, current_settings.log_profile_events, false);
        addQueryStatusInfoToQueryLogElement(info, ast);
    }

    if (current_settings.calculate_text_stack_trace)
        setExceptionStackTrace(query_log_element);
    logException(context, query_log_element);

    const auto log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds();
    /// In case of exception we log internal queries also
    if (log_queries && query_log_element.type >= settings.log_queries_min_type && Int64(query_log_element.query_duration_ms) >= log_queries_min_query_duration_ms)
    {
        if (auto query_log = context.getQueryLog())
            query_log->add(query_log_element);
    }

    ProfileEvents::increment(ProfileEvents::FailedQuery);
    if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
    {
        ProfileEvents::increment(ProfileEvents::FailedSelectQuery);
    }
    else if (ast->as<ASTInsertQuery>())
    {
        ProfileEvents::increment(ProfileEvents::FailedInsertQuery);
    }
}

void QueryProcess::addQueryStatusInfoToQueryLogElement(const QueryStatusInfo &info, const ASTPtr query_ast)
{
    DB::UInt64 query_time = info.elapsed_seconds * 1000000;
    ProfileEvents::increment(ProfileEvents::QueryTimeMicroseconds, query_time);
    if (query_ast->as<ASTSelectQuery>() || query_ast->as<ASTSelectWithUnionQuery>())
    {
        ProfileEvents::increment(ProfileEvents::SelectQueryTimeMicroseconds, query_time);
    }
    else if (query_ast->as<ASTInsertQuery>())
    {
        ProfileEvents::increment(ProfileEvents::InsertQueryTimeMicroseconds, query_time);
    }

    query_log_element.query_duration_ms = info.elapsed_seconds * 1000;

    query_log_element.read_rows = info.read_rows;
    query_log_element.read_bytes = info.read_bytes;

    query_log_element.written_rows = info.written_rows;
    query_log_element.written_bytes = info.written_bytes;

    query_log_element.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;

    query_log_element.thread_ids = std::move(info.thread_ids);
    query_log_element.profile_counters = std::move(info.profile_counters);
}

QueryLogElement QueryProcess::makeQueryLogElement()
{
    QueryLogElement elem;

    elem.event_time = time_in_seconds(current_time);
    elem.event_time_microseconds = time_in_microseconds(current_time);
    elem.query_start_time = time_in_seconds(current_time);
    elem.query_start_time_microseconds = time_in_microseconds(current_time);

    elem.current_database = context.getCurrentDatabase();
    elem.query = query_for_logging;
    elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);

    elem.client_info = context.getClientInfo();

    return elem;
}

/// Prepares query for logging:
/// - wipe sensitive data
/// - trim to log_queries_cut_to_length
String QueryProcess::prepareQueryForLogging(const String & query, const Context & context)
{
    String res = query;

    // wiping sensitive data before cropping query by log_queries_cut_to_length,
    // otherwise something like credit card without last digit can go to log
    if (auto * masker = SensitiveDataMasker::getInstance())
    {
        auto matches = masker->wipeSensitiveData(res);
        if (matches > 0)
        {
            ProfileEvents::increment(ProfileEvents::QueryMaskingRulesMatch, matches);
        }
    }

    res = res.substr(0, context.getSettingsRef().log_queries_cut_to_length);

    return res;
}

void QueryProcess::logQuery(const String & query_for_logging, const Context & context, bool internal)
{
    if (internal)
    {
        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "(internal) {}", joinLines(query_for_logging));
    }
    else
    {
        const auto & client_info = context.getClientInfo();

        const auto & current_query_id = client_info.current_query_id;
        const auto & initial_query_id = client_info.initial_query_id;
        const auto & current_user = client_info.current_user;

        String comment = context.getSettingsRef().log_comment;
        size_t max_query_size = context.getSettingsRef().max_query_size;

        if (comment.size() > max_query_size)
            comment.resize(max_query_size);

        if (!comment.empty())
            comment = fmt::format(" (comment: {})", comment);

        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "(from {}{}{}, using {} parser){} {}",
            client_info.current_address.toString(),
            (current_user != "default" ? ", user: " + current_user : ""),
            (!initial_query_id.empty() && current_query_id != initial_query_id ? ", initial_query_id: " + initial_query_id : std::string()),
            (context.getSettingsRef().use_antlr_parser ? "experimental" : "production"),
            comment,
            joinLines(query_for_logging));

        if (client_info.client_trace_context.trace_id)
        {
            LOG_TRACE(&Poco::Logger::get("executeQuery"),
                "OpenTelemetry traceparent '{}'",
                client_info.client_trace_context.composeTraceparentHeader());
        }
    }
}

void QueryProcess::onExceptionBeforeStart(const String & query_for_logging, Context & context,
    const std::chrono::time_point<std::chrono::system_clock> & current_time, const ASTPtr & ast)
{
    /// Exception before the query execution.
    if (auto quota = context.getQuota())
        quota->used(Quota::ERRORS, 1, /* check_exceeded = */ false);

    const Settings & settings = context.getSettingsRef();

    /// Log the start of query execution into the table if necessary.
    QueryLogElement elem;

    elem.type = QueryLogElementType::EXCEPTION_BEFORE_START;

    // all callers to onExceptionBeforeStart method construct the timespec for event_time and
    // event_time_microseconds from the same time point. So, it can be assumed that both of these
    // times are equal up to the precision of a second.
    UInt64 current_time_us = time_in_microseconds(current_time);
    elem.event_time = current_time_us / 1000000;
    elem.event_time_microseconds = current_time_us;
    elem.query_start_time = current_time_us / 1000000;
    elem.query_start_time_microseconds = current_time_us;

    elem.current_database = context.getCurrentDatabase();
    elem.query = query_for_logging;
    elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);

    // We don't calculate query_kind, databases, tables and columns when the query isn't able to start

    elem.exception_code = getCurrentExceptionCode();
    elem.exception = getCurrentExceptionMessage(false);

    elem.client_info = context.getClientInfo();

    elem.log_comment = settings.log_comment;
    if (elem.log_comment.size() > settings.max_query_size)
        elem.log_comment.resize(settings.max_query_size);

    if (settings.calculate_text_stack_trace)
        setExceptionStackTrace(elem);
    logException(context, elem);

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    if (settings.log_queries && elem.type >= settings.log_queries_min_type && !settings.log_queries_min_query_duration_ms.totalMilliseconds())
        if (auto query_log = context.getQueryLog())
            query_log->add(elem);

    if (auto opentelemetry_span_log = context.getOpenTelemetrySpanLog();
        context.query_trace_context.trace_id
            && opentelemetry_span_log)
    {
        OpenTelemetrySpanLogElement span;
        span.trace_id = context.query_trace_context.trace_id;
        span.span_id = context.query_trace_context.span_id;
        span.parent_span_id = context.getClientInfo().client_trace_context.span_id;
        span.operation_name = "query";
        span.start_time_us = current_time_us;
        span.finish_time_us = current_time_us;

        /// Keep values synchronized to type enum in QueryLogElement::createBlock.
        span.attribute_names.push_back("clickhouse.query_status");
        span.attribute_values.push_back("ExceptionBeforeStart");

        span.attribute_names.push_back("db.statement");
        span.attribute_values.push_back(elem.query);

        span.attribute_names.push_back("clickhouse.query_id");
        span.attribute_values.push_back(elem.client_info.current_query_id);

        if (!context.query_trace_context.tracestate.empty())
        {
            span.attribute_names.push_back("clickhouse.tracestate");
            span.attribute_values.push_back(
                context.query_trace_context.tracestate);
        }

        opentelemetry_span_log->add(span);
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

Context & QueryProcess::prepareContext(Context & context)
{
    /// Generate new random query_id.
    context.setCurrentQueryId("");
    return context;
}

}
