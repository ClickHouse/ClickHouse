#include <Common/formatReadable.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadProfileEvents.h>

#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/CountingBlockOutputStream.h>

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
#include <Parsers/Lexer.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/queryNormalization.h>
#include <Parsers/queryToString.h>

#include <Formats/FormatFactory.h>
#include <Storages/StorageInput.h>

#include <Access/EnabledQuota.h>
#include <Interpreters/ApplyWithGlobalVisitor.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Interpreters/OpenTelemetrySpanLog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/SelectQueryOptions.h>
#include <Interpreters/executeQuery.h>
#include <Common/ProfileEvents.h>

#include <Common/SensitiveDataMasker.h>

#include <Processors/Transforms/LimitsCheckingTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Formats/IOutputFormat.h>


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

namespace DB
{

namespace ErrorCodes
{
    extern const int INTO_OUTFILE_NOT_ALLOWED;
    extern const int QUERY_WAS_CANCELLED;
}


static void checkASTSizeLimits(const IAST & ast, const Settings & settings)
{
    if (settings.max_ast_depth)
        ast.checkDepth(settings.max_ast_depth);
    if (settings.max_ast_elements)
        ast.checkSize(settings.max_ast_elements);
}


static String joinLines(const String & query)
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


static String prepareQueryForLogging(const String & query, ContextPtr context)
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

    res = res.substr(0, context->getSettingsRef().log_queries_cut_to_length);

    return res;
}


/// Log query into text log (not into system table).
static void logQuery(const String & query, ContextPtr context, bool internal)
{
    if (internal)
    {
        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "(internal) {}", joinLines(query));
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

        LOG_DEBUG(&Poco::Logger::get("executeQuery"), "(from {}{}{}){} {}",
            client_info.current_address.toString(),
            (current_user != "default" ? ", user: " + current_user : ""),
            (!initial_query_id.empty() && current_query_id != initial_query_id ? ", initial_query_id: " + initial_query_id : std::string()),
            comment,
            joinLines(query));

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
static void logException(ContextPtr context, QueryLogElement & elem)
{
    String comment;
    if (!elem.log_comment.empty())
        comment = fmt::format(" (comment: {})", elem.log_comment);

    if (elem.stack_trace.empty())
        LOG_ERROR(
            &Poco::Logger::get("executeQuery"),
            "{} (from {}){} (in query: {})",
            elem.exception,
            context->getClientInfo().current_address.toString(),
            comment,
            joinLines(elem.query));
    else
        LOG_ERROR(
            &Poco::Logger::get("executeQuery"),
            "{} (from {}){} (in query: {})"
            ", Stack trace (when copying this message, always include the lines below):\n\n{}",
            elem.exception,
            context->getClientInfo().current_address.toString(),
            comment,
            joinLines(elem.query),
            elem.stack_trace);
}

inline UInt64 time_in_microseconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::microseconds>(timepoint.time_since_epoch()).count();
}


inline UInt64 time_in_seconds(std::chrono::time_point<std::chrono::system_clock> timepoint)
{
    return std::chrono::duration_cast<std::chrono::seconds>(timepoint.time_since_epoch()).count();
}

static void onExceptionBeforeStart(const String & query_for_logging, ContextPtr context, UInt64 current_time_us, ASTPtr ast)
{
    /// Exception before the query execution.
    if (auto quota = context->getQuota())
        quota->used(Quota::ERRORS, 1, /* check_exceeded = */ false);

    const Settings & settings = context->getSettingsRef();

    /// Log the start of query execution into the table if necessary.
    QueryLogElement elem;

    elem.type = QueryLogElementType::EXCEPTION_BEFORE_START;

    // all callers to onExceptionBeforeStart method construct the timespec for event_time and
    // event_time_microseconds from the same time point. So, it can be assumed that both of these
    // times are equal up to the precision of a second.
    elem.event_time = current_time_us / 1000000;
    elem.event_time_microseconds = current_time_us;
    elem.query_start_time = current_time_us / 1000000;
    elem.query_start_time_microseconds = current_time_us;

    elem.current_database = context->getCurrentDatabase();
    elem.query = query_for_logging;
    elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);

    // We don't calculate query_kind, databases, tables and columns when the query isn't able to start

    elem.exception_code = getCurrentExceptionCode();
    elem.exception = getCurrentExceptionMessage(false);

    elem.client_info = context->getClientInfo();

    elem.log_comment = settings.log_comment;
    if (elem.log_comment.size() > settings.max_query_size)
        elem.log_comment.resize(settings.max_query_size);

    if (settings.calculate_text_stack_trace)
        setExceptionStackTrace(elem);
    logException(context, elem);

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    if (settings.log_queries && elem.type >= settings.log_queries_min_type && !settings.log_queries_min_query_duration_ms.totalMilliseconds())
        if (auto query_log = context->getQueryLog())
            query_log->add(elem);

    if (auto opentelemetry_span_log = context->getOpenTelemetrySpanLog();
        context->query_trace_context.trace_id != UUID()
            && opentelemetry_span_log)
    {
        OpenTelemetrySpanLogElement span;
        span.trace_id = context->query_trace_context.trace_id;
        span.span_id = context->query_trace_context.span_id;
        span.parent_span_id = context->getClientInfo().client_trace_context.span_id;
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

        if (!context->query_trace_context.tracestate.empty())
        {
            span.attribute_names.push_back("clickhouse.tracestate");
            span.attribute_values.push_back(
                context->query_trace_context.tracestate);
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

static void setQuerySpecificSettings(ASTPtr & ast, ContextMutablePtr context)
{
    if (auto * ast_insert_into = dynamic_cast<ASTInsertQuery *>(ast.get()))
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
    bool has_query_tail,
    ReadBuffer * istr)
{
    const auto current_time = std::chrono::system_clock::now();

    auto & client_info = context->getClientInfo();

    // If it's not an internal query and we don't see an initial_query_start_time yet, initialize it
    // to current time. Internal queries are those executed without an independent client context,
    // thus should not set initial_query_start_time, because it might introduce data race. It's also
    // possible to have unset initial_query_start_time for non-internal and non-initial queries. For
    // example, the query is from an initiator that is running an old version of clickhouse.
    if (!internal && client_info.initial_query_start_time == 0)
    {
        client_info.initial_query_start_time = time_in_seconds(current_time);
        client_info.initial_query_start_time_microseconds = time_in_microseconds(current_time);
    }

#if !defined(ARCADIA_BUILD)
    assert(internal || CurrentThread::get().getQueryContext());
    assert(internal || CurrentThread::get().getQueryContext()->getCurrentQueryId() == CurrentThread::getQueryId());
#endif

    const Settings & settings = context->getSettingsRef();

    ASTPtr ast;
    const char * query_end;

    /// Don't limit the size of internal queries.
    size_t max_query_size = 0;
    if (!internal) max_query_size = settings.max_query_size;

    String query_database;
    String query_table;
    try
    {
        ParserQuery parser(end);

        /// TODO: parser should fail early when max_query_size limit is reached.
        ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);

        /// Interpret SETTINGS clauses as early as possible (before invoking the corresponding interpreter),
        /// to allow settings to take effect.
        if (const auto * select_query = ast->as<ASTSelectQuery>())
        {
            if (auto new_settings = select_query->settings())
                InterpreterSetQuery(new_settings, context).executeForCurrentContext();
        }
        else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
        {
            if (!select_with_union_query->list_of_selects->children.empty())
            {
                // We might have an arbitrarily complex UNION tree, so just give
                // up if the last first-order child is not a plain SELECT.
                // It is flattened later, when we process UNION ALL/DISTINCT.
                const auto * last_select = select_with_union_query->list_of_selects->children.back()->as<ASTSelectQuery>();
                if (last_select && last_select->settings())
                {
                    InterpreterSetQuery(last_select->settings(), context).executeForCurrentContext();
                }
            }
        }
        else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
        {
            if (query_with_output->settings_ast)
                InterpreterSetQuery(query_with_output->settings_ast, context).executeForCurrentContext();
        }

        if (const auto * query_with_table_output = dynamic_cast<const ASTQueryWithTableAndOutput *>(ast.get()))
        {
            query_database = query_with_table_output->database;
            query_table = query_with_table_output->table;
        }

        auto * insert_query = ast->as<ASTInsertQuery>();

        if (insert_query && insert_query->settings_ast)
            InterpreterSetQuery(insert_query->settings_ast, context).executeForCurrentContext();

        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
            insert_query->has_tail = has_query_tail;
        }
        else
        {
            query_end = end;
        }
    }
    catch (...)
    {
        /// Anyway log the query.
        String query = String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));

        auto query_for_logging = prepareQueryForLogging(query, context);
        logQuery(query_for_logging, context, internal);

        if (!internal)
        {
            onExceptionBeforeStart(query_for_logging, context, time_in_microseconds(current_time), ast);
        }

        throw;
    }

    setQuerySpecificSettings(ast, context);

    /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
    String query(begin, query_end);
    BlockIO res;

    String query_for_logging;

    try
    {
        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        if (context->hasQueryParameters())
        {
            ReplaceQueryParameterVisitor visitor(context->getQueryParameters());
            visitor.visit(ast);
            query = serializeAST(*ast);
        }

        /// MUST goes before any modification (except for prepared statements,
        /// since it substitute parameters and w/o them query does not contains
        /// parameters), to keep query as-is in query_log and server log.
        query_for_logging = prepareQueryForLogging(query, context);
        logQuery(query_for_logging, context, internal);

        /// Propagate WITH statement to children ASTSelect.
        if (settings.enable_global_with_statement)
        {
            ApplyWithGlobalVisitor().visit(ast);
        }

        /// Normalize SelectWithUnionQuery
        NormalizeSelectWithUnionQueryVisitor::Data data{context->getSettingsRef().union_default_mode};
        NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);

        /// Check the limits.
        checkASTSizeLimits(*ast, settings);

        /// Put query to process list. But don't put SHOW PROCESSLIST query itself.
        ProcessList::EntryPtr process_list_entry;
        if (!internal && !ast->as<ASTShowProcesslistQuery>())
        {
            /// processlist also has query masked now, to avoid secrets leaks though SHOW PROCESSLIST by other users.
            process_list_entry = context->getProcessList().insert(query_for_logging, ast.get(), context);
            context->setProcessListElement(&process_list_entry->get());
        }

        /// Load external tables if they were provided
        context->initializeExternalTablesIfSet();

        auto * insert_query = ast->as<ASTInsertQuery>();
        if (insert_query && insert_query->select)
        {
            /// Prepare Input storage before executing interpreter if we already got a buffer with data.
            if (istr)
            {
                ASTPtr input_function;
                insert_query->tryFindInputFunction(input_function);
                if (input_function)
                {
                    StoragePtr storage = context->executeTableFunction(input_function);
                    auto & input_storage = dynamic_cast<StorageInput &>(*storage);
                    auto input_metadata_snapshot = input_storage.getInMemoryMetadataPtr();
                    BlockInputStreamPtr input_stream = std::make_shared<InputStreamFromASTInsertQuery>(
                        ast, istr, input_metadata_snapshot->getSampleBlock(), context, input_function);
                    input_storage.setInputStream(input_stream);
                }
            }
        }
        else
            /// reset Input callbacks if query is not INSERT SELECT
            context->resetInputCallbacks();

        auto interpreter = InterpreterFactory::get(ast, context, SelectQueryOptions(stage).setInternal(internal));

        std::shared_ptr<const EnabledQuota> quota;
        if (!interpreter->ignoreQuota())
        {
            quota = context->getQuota();
            if (quota)
            {
                if (ast->as<ASTSelectQuery>() || ast->as<ASTSelectWithUnionQuery>())
                {
                    quota->used(Quota::QUERY_SELECTS, 1);
                }
                else if (ast->as<ASTInsertQuery>())
                {
                    quota->used(Quota::QUERY_INSERTS, 1);
                }
                quota->used(Quota::QUERIES, 1);
                quota->checkExceeded(Quota::ERRORS);
            }
        }

        StreamLocalLimits limits;
        if (!interpreter->ignoreLimits())
        {
            limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
            limits.size_limits = SizeLimits(settings.max_result_rows, settings.max_result_bytes, settings.result_overflow_mode);
        }

        {
            OpenTelemetrySpanHolder span("IInterpreter::execute()");
            res = interpreter->execute();
        }

        QueryPipeline & pipeline = res.pipeline;
        bool use_processors = pipeline.initialized();

        if (const auto * insert_interpreter = typeid_cast<const InterpreterInsertQuery *>(&*interpreter))
        {
            /// Save insertion table (not table function). TODO: support remote() table function.
            auto table_id = insert_interpreter->getDatabaseTable();
            if (!table_id.empty())
                context->setInsertionTable(std::move(table_id));
        }

        if (process_list_entry)
        {
            /// Query was killed before execution
            if ((*process_list_entry)->isKilled())
                throw Exception("Query '" + (*process_list_entry)->getInfo().client_info.current_query_id + "' is killed in pending state",
                    ErrorCodes::QUERY_WAS_CANCELLED);
            else if (!use_processors)
                (*process_list_entry)->setQueryStreams(res);
        }

        /// Hold element of process list till end of query execution.
        res.process_list_entry = process_list_entry;

        if (use_processors)
        {
            /// Limits on the result, the quota on the result, and also callback for progress.
            /// Limits apply only to the final result.
            pipeline.setProgressCallback(context->getProgressCallback());
            pipeline.setProcessListElement(context->getProcessListElement());
            if (stage == QueryProcessingStage::Complete && !pipeline.isCompleted())
            {
                pipeline.resize(1);
                pipeline.addSimpleTransform([&](const Block & header)
                {
                    auto transform = std::make_shared<LimitsCheckingTransform>(header, limits);
                    transform->setQuota(quota);
                    return transform;
                });
            }
        }
        else
        {
            /// Limits on the result, the quota on the result, and also callback for progress.
            /// Limits apply only to the final result.
            if (res.in)
            {
                res.in->setProgressCallback(context->getProgressCallback());
                res.in->setProcessListElement(context->getProcessListElement());
                if (stage == QueryProcessingStage::Complete)
                {
                    if (!interpreter->ignoreQuota())
                        res.in->setQuota(quota);
                    if (!interpreter->ignoreLimits())
                        res.in->setLimits(limits);
                }
            }

            if (res.out)
            {
                if (auto * stream = dynamic_cast<CountingBlockOutputStream *>(res.out.get()))
                {
                    stream->setProcessListElement(context->getProcessListElement());
                }
            }
        }

        /// Everything related to query log.
        {
            QueryLogElement elem;

            elem.type = QueryLogElementType::QUERY_START; //-V1048

            elem.event_time = time_in_seconds(current_time);
            elem.event_time_microseconds = time_in_microseconds(current_time);
            elem.query_start_time = time_in_seconds(current_time);
            elem.query_start_time_microseconds = time_in_microseconds(current_time);

            elem.current_database = context->getCurrentDatabase();
            elem.query = query_for_logging;
            elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);

            elem.client_info = client_info;

            bool log_queries = settings.log_queries && !internal;

            /// Log into system table start of query execution, if need.
            if (log_queries)
            {
                if (use_processors)
                {
                    const auto & info = context->getQueryAccessInfo();
                    elem.query_databases = info.databases;
                    elem.query_tables = info.tables;
                    elem.query_columns = info.columns;
                    elem.query_projections = info.projections;
                }

                interpreter->extendQueryLogElem(elem, ast, context, query_database, query_table);

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

            /// Common code for finish and exception callbacks
            auto status_info_to_query_log = [](QueryLogElement &element, const QueryStatusInfo &info, const ASTPtr query_ast) mutable
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

                element.query_duration_ms = info.elapsed_seconds * 1000;

                element.read_rows = info.read_rows;
                element.read_bytes = info.read_bytes;

                element.written_rows = info.written_rows;
                element.written_bytes = info.written_bytes;

                element.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;

                element.thread_ids = std::move(info.thread_ids);
                element.profile_counters = std::move(info.profile_counters);
            };

            /// Also make possible for caller to log successful query finish and exception during execution.
            auto finish_callback = [elem, context, ast,
                 log_queries,
                 log_queries_min_type = settings.log_queries_min_type,
                 log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds(),
                 status_info_to_query_log
            ]
                (IBlockInputStream * stream_in, IBlockOutputStream * stream_out, QueryPipeline * query_pipeline) mutable
            {
                QueryStatus * process_list_elem = context->getProcessListElement();

                if (!process_list_elem)
                    return;

                /// Update performance counters before logging to query_log
                CurrentThread::finalizePerformanceCounters();

                QueryStatusInfo info = process_list_elem->getInfo(true, context->getSettingsRef().log_profile_events);

                double elapsed_seconds = info.elapsed_seconds;

                elem.type = QueryLogElementType::QUERY_FINISH;

                // construct event_time and event_time_microseconds using the same time point
                // so that the two times will always be equal up to a precision of a second.
                const auto finish_time = std::chrono::system_clock::now();
                elem.event_time = time_in_seconds(finish_time);
                elem.event_time_microseconds = time_in_microseconds(finish_time);
                status_info_to_query_log(elem, info, ast);

                auto progress_callback = context->getProgressCallback();

                if (progress_callback)
                    progress_callback(Progress(WriteProgress(info.written_rows, info.written_bytes)));

                if (stream_in)
                {
                    const BlockStreamProfileInfo & stream_in_info = stream_in->getProfileInfo();

                    /// NOTE: INSERT SELECT query contains zero metrics
                    elem.result_rows = stream_in_info.rows;
                    elem.result_bytes = stream_in_info.bytes;
                }
                else if (stream_out) /// will be used only for ordinary INSERT queries
                {
                    if (const auto * counting_stream = dynamic_cast<const CountingBlockOutputStream *>(stream_out))
                    {
                        /// NOTE: Redundancy. The same values could be extracted from process_list_elem->progress_out.query_settings = process_list_elem->progress_in
                        elem.result_rows = counting_stream->getProgress().read_rows;
                        elem.result_bytes = counting_stream->getProgress().read_bytes;
                    }
                }
                else if (query_pipeline)
                {
                    if (const auto * output_format = query_pipeline->getOutputFormat())
                    {
                        elem.result_rows = output_format->getResultRows();
                        elem.result_bytes = output_format->getResultBytes();
                    }
                }

                if (elem.read_rows != 0)
                {
                    LOG_INFO(&Poco::Logger::get("executeQuery"), "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
                        elem.read_rows, ReadableSize(elem.read_bytes), elapsed_seconds,
                        static_cast<size_t>(elem.read_rows / elapsed_seconds),
                        ReadableSize(elem.read_bytes / elapsed_seconds));
                }

                elem.thread_ids = std::move(info.thread_ids);
                elem.profile_counters = std::move(info.profile_counters);

                const auto & factories_info = context->getQueryFactoriesInfo();
                elem.used_aggregate_functions = factories_info.aggregate_functions;
                elem.used_aggregate_function_combinators = factories_info.aggregate_function_combinators;
                elem.used_database_engines = factories_info.database_engines;
                elem.used_data_type_families = factories_info.data_type_families;
                elem.used_dictionaries = factories_info.dictionaries;
                elem.used_formats = factories_info.formats;
                elem.used_functions = factories_info.functions;
                elem.used_storages = factories_info.storages;
                elem.used_table_functions = factories_info.table_functions;

                if (log_queries && elem.type >= log_queries_min_type && Int64(elem.query_duration_ms) >= log_queries_min_query_duration_ms)
                {
                    if (auto query_log = context->getQueryLog())
                        query_log->add(elem);
                }

                if (auto opentelemetry_span_log = context->getOpenTelemetrySpanLog();
                    context->query_trace_context.trace_id != UUID()
                        && opentelemetry_span_log)
                {
                    OpenTelemetrySpanLogElement span;
                    span.trace_id = context->query_trace_context.trace_id;
                    span.span_id = context->query_trace_context.span_id;
                    span.parent_span_id = context->getClientInfo().client_trace_context.span_id;
                    span.operation_name = "query";
                    span.start_time_us = elem.query_start_time_microseconds;
                    span.finish_time_us = time_in_microseconds(finish_time);

                    /// Keep values synchronized to type enum in QueryLogElement::createBlock.
                    span.attribute_names.push_back("clickhouse.query_status");
                    span.attribute_values.push_back("QueryFinish");

                    span.attribute_names.push_back("db.statement");
                    span.attribute_values.push_back(elem.query);

                    span.attribute_names.push_back("clickhouse.query_id");
                    span.attribute_values.push_back(elem.client_info.current_query_id);
                    if (!context->query_trace_context.tracestate.empty())
                    {
                        span.attribute_names.push_back("clickhouse.tracestate");
                        span.attribute_values.push_back(
                            context->query_trace_context.tracestate);
                    }

                    opentelemetry_span_log->add(span);
                }
            };

            auto exception_callback = [elem, context, ast,
                 log_queries,
                 log_queries_min_type = settings.log_queries_min_type,
                 log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds(),
                 quota(quota), status_info_to_query_log] () mutable
            {
                if (quota)
                    quota->used(Quota::ERRORS, 1, /* check_exceeded = */ false);

                elem.type = QueryLogElementType::EXCEPTION_WHILE_PROCESSING;

                // event_time and event_time_microseconds are being constructed from the same time point
                // to ensure that both the times will be equal up to the precision of a second.
                const auto time_now = std::chrono::system_clock::now();

                elem.event_time = time_in_seconds(time_now);
                elem.event_time_microseconds = time_in_microseconds(time_now);
                elem.query_duration_ms = 1000 * (elem.event_time - elem.query_start_time);
                elem.exception_code = getCurrentExceptionCode();
                elem.exception = getCurrentExceptionMessage(false);

                QueryStatus * process_list_elem = context->getProcessListElement();
                const Settings & current_settings = context->getSettingsRef();

                /// Update performance counters before logging to query_log
                CurrentThread::finalizePerformanceCounters();

                if (process_list_elem)
                {
                    QueryStatusInfo info = process_list_elem->getInfo(true, current_settings.log_profile_events, false);
                    status_info_to_query_log(elem, info, ast);
                }

                if (current_settings.calculate_text_stack_trace)
                    setExceptionStackTrace(elem);
                logException(context, elem);

                /// In case of exception we log internal queries also
                if (log_queries && elem.type >= log_queries_min_type && Int64(elem.query_duration_ms) >= log_queries_min_query_duration_ms)
                {
                    if (auto query_log = context->getQueryLog())
                        query_log->add(elem);
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

            };

            res.finish_callback = std::move(finish_callback);
            res.exception_callback = std::move(exception_callback);

            if (!internal && res.in)
            {
                WriteBufferFromOwnString msg_buf;
                res.in->dumpTree(msg_buf);
                LOG_DEBUG(&Poco::Logger::get("executeQuery"), "Query pipeline:\n{}", msg_buf.str());
            }
        }
    }
    catch (...)
    {
        if (!internal)
        {
            if (query_for_logging.empty())
                query_for_logging = prepareQueryForLogging(query, context);

            onExceptionBeforeStart(query_for_logging, context, time_in_microseconds(current_time), ast);
        }

        throw;
    }

    return std::make_tuple(ast, std::move(res));
}


BlockIO executeQuery(
    const String & query,
    ContextMutablePtr context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data)
{
    ASTPtr ast;
    BlockIO streams;
    std::tie(ast, streams) = executeQueryImpl(query.data(), query.data() + query.size(), context,
        internal, stage, !may_have_embedded_data, nullptr);

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
    const String & query,
    ContextMutablePtr context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data,
    bool allow_processors)
{
    BlockIO res = executeQuery(query, context, internal, stage, may_have_embedded_data);

    if (!allow_processors && res.pipeline.initialized())
        res.in = res.getInputStream();

    return res;
}


void executeQuery(
    ReadBuffer & istr,
    WriteBuffer & ostr,
    bool allow_into_outfile,
    ContextMutablePtr context,
    std::function<void(const String &, const String &, const String &, const String &)> set_result_details,
    const std::optional<FormatSettings> & output_format_settings)
{
    PODArray<char> parse_buf;
    const char * begin;
    const char * end;

    /// If 'istr' is empty now, fetch next data into buffer.
    if (!istr.hasPendingData())
        istr.next();

    size_t max_query_size = context->getSettingsRef().max_query_size;

    bool may_have_tail;
    if (istr.buffer().end() - istr.position() > static_cast<ssize_t>(max_query_size))
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        begin = istr.position();
        end = istr.buffer().end();
        istr.position() += end - begin;
        /// Actually we don't know will query has additional data or not.
        /// But we can't check istr.eof(), because begin and end pointers will become invalid
        may_have_tail = true;
    }
    else
    {
        /// If not - copy enough data into 'parse_buf'.
        WriteBufferFromVector<PODArray<char>> out(parse_buf);
        LimitReadBuffer limit(istr, max_query_size + 1, false);
        copyData(limit, out);
        out.finalize();

        begin = parse_buf.data();
        end = begin + parse_buf.size();
        /// Can check stream for eof, because we have copied data
        may_have_tail = !istr.eof();
    }

    ASTPtr ast;
    BlockIO streams;

    std::tie(ast, streams) = executeQueryImpl(begin, end, context, false, QueryProcessingStage::Complete, may_have_tail, &istr);

    auto & pipeline = streams.pipeline;

    try
    {
        if (streams.out)
        {
            InputStreamFromASTInsertQuery in(ast, &istr, streams.out->getHeader(), context, nullptr);
            copyData(in, *streams.out);
        }
        else if (streams.in)
        {
            const auto * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

            WriteBuffer * out_buf = &ostr;
            std::optional<WriteBufferFromFile> out_file_buf;
            if (ast_query_with_output && ast_query_with_output->out_file)
            {
                if (!allow_into_outfile)
                    throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

                const auto & out_file = ast_query_with_output->out_file->as<ASTLiteral &>().value.safeGet<std::string>();
                out_file_buf.emplace(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
                out_buf = &*out_file_buf;
            }

            String format_name = ast_query_with_output && (ast_query_with_output->format != nullptr)
                ? getIdentifierName(ast_query_with_output->format)
                : context->getDefaultFormat();

            auto out = FormatFactory::instance().getOutputStreamParallelIfPossible(format_name, *out_buf, streams.in->getHeader(), context, {}, output_format_settings);

            /// Save previous progress callback if any. TODO Do it more conveniently.
            auto previous_progress_callback = context->getProgressCallback();

            /// NOTE Progress callback takes shared ownership of 'out'.
            streams.in->setProgressCallback([out, previous_progress_callback] (const Progress & progress)
            {
                if (previous_progress_callback)
                    previous_progress_callback(progress);
                out->onProgress(progress);
            });

            if (set_result_details)
                set_result_details(
                    context->getClientInfo().current_query_id, out->getContentType(), format_name, DateLUT::instance().getTimeZone());

            copyData(*streams.in, *out, [](){ return false; }, [&out](const Block &) { out->flush(); });
        }
        else if (pipeline.initialized())
        {
            const ASTQueryWithOutput * ast_query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get());

            WriteBuffer * out_buf = &ostr;
            std::optional<WriteBufferFromFile> out_file_buf;
            if (ast_query_with_output && ast_query_with_output->out_file)
            {
                if (!allow_into_outfile)
                    throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

                const auto & out_file = typeid_cast<const ASTLiteral &>(*ast_query_with_output->out_file).value.safeGet<std::string>();
                out_file_buf.emplace(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
                out_buf = &*out_file_buf;
            }

            String format_name = ast_query_with_output && (ast_query_with_output->format != nullptr)
                                 ? getIdentifierName(ast_query_with_output->format)
                                 : context->getDefaultFormat();

            if (!pipeline.isCompleted())
            {
                pipeline.addSimpleTransform([](const Block & header)
                {
                    return std::make_shared<MaterializingTransform>(header);
                });

                auto out = FormatFactory::instance().getOutputFormatParallelIfPossible(format_name, *out_buf, pipeline.getHeader(), context, {}, output_format_settings);
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

                if (set_result_details)
                    set_result_details(
                        context->getClientInfo().current_query_id, out->getContentType(), format_name, DateLUT::instance().getTimeZone());

                pipeline.setOutputFormat(std::move(out));
            }
            else
            {
                pipeline.setProgressCallback(context->getProgressCallback());
            }

            {
                auto executor = pipeline.execute();
                executor->execute(pipeline.getNumThreads());
            }
        }
    }
    catch (...)
    {
        streams.onException();
        throw;
    }

    streams.onFinish();
}

}
