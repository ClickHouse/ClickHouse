#include <Common/formatReadable.h>
#include <Common/PODArray.h>
#include <Common/typeid_cast.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/MemoryTrackerBlockerInThread.h>

#include <Interpreters/AsynchronousInsertQueue.h>
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
#include <Common/ProfileEvents.h>

#include <Common/SensitiveDataMasker.h>
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


namespace ProfileEvents
{
    extern const Event QueryMaskingRulesMatch;
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
            toOneLineQuery(elem.query));
    else
        LOG_ERROR(
            &Poco::Logger::get("executeQuery"),
            "{} (from {}){} (in query: {})"
            ", Stack trace (when copying this message, always include the lines below):\n\n{}",
            elem.exception,
            context->getClientInfo().current_address.toString(),
            comment,
            toOneLineQuery(elem.query),
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
        quota->used(QuotaType::ERRORS, 1, /* check_exceeded = */ false);

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

    // Try log query_kind if ast is valid
    if (ast)
    {
        elem.query_kind = magic_enum::enum_name(ast->getQueryKind());
        if (settings.log_formatted_queries)
            elem.formatted_query = queryToString(ast);
    }

    // We don't calculate databases, tables and columns when the query isn't able to start

    elem.exception_code = getCurrentExceptionCode();
    elem.exception = getCurrentExceptionMessage(false);

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
        span.finish_time_us = time_in_microseconds(std::chrono::system_clock::now());
        span.attributes.reserve(6);
        span.attributes.push_back(Tuple{"clickhouse.query_status", "ExceptionBeforeStart"});
        span.attributes.push_back(Tuple{"db.statement", elem.query});
        span.attributes.push_back(Tuple{"clickhouse.query_id", elem.client_info.current_query_id});
        span.attributes.push_back(Tuple{"clickhouse.exception", elem.exception});
        span.attributes.push_back(Tuple{"clickhouse.exception_code", toString(elem.exception_code)});
        if (!context->query_trace_context.tracestate.empty())
        {
            span.attributes.push_back(Tuple{"clickhouse.tracestate", context->query_trace_context.tracestate});
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
    if (auto * ast_insert_into = ast->as<ASTInsertQuery>())
    {
        if (ast_insert_into->watch)
            context->setSetting("output_format_enable_streaming", 1);
    }
}

static void applySettingsFromSelectWithUnion(const ASTSelectWithUnionQuery & select_with_union, ContextMutablePtr context)
{
    const ASTs & children = select_with_union.list_of_selects->children;
    if (children.empty())
        return;

    // We might have an arbitrarily complex UNION tree, so just give
    // up if the last first-order child is not a plain SELECT.
    // It is flattened later, when we process UNION ALL/DISTINCT.
    const auto * last_select = children.back()->as<ASTSelectQuery>();
    if (last_select && last_select->settings())
    {
        InterpreterSetQuery(last_select->settings(), context).executeForCurrentContext();
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

    assert(internal || CurrentThread::get().getQueryContext());
    assert(internal || CurrentThread::get().getQueryContext()->getCurrentQueryId() == CurrentThread::getQueryId());

    const Settings & settings = context->getSettingsRef();

    ASTPtr ast;
    const char * query_end;

    size_t max_query_size = settings.max_query_size;
    /// Don't limit the size of internal queries or distributed subquery.
    if (internal || client_info.query_kind == ClientInfo::QueryKind::SECONDARY_QUERY)
        max_query_size = 0;

    String query_database;
    String query_table;
    try
    {
        ParserQuery parser(end, settings.allow_settings_after_format_in_insert);

        /// TODO: parser should fail early when max_query_size limit is reached.
        ast = parseQuery(parser, begin, end, "", max_query_size, settings.max_parser_depth);

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
        if (const auto * select_query = ast->as<ASTSelectQuery>())
        {
            if (auto new_settings = select_query->settings())
                InterpreterSetQuery(new_settings, context).executeForCurrentContext();
        }
        else if (const auto * select_with_union_query = ast->as<ASTSelectWithUnionQuery>())
        {
            applySettingsFromSelectWithUnion(*select_with_union_query, context);
        }
        else if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(ast.get()))
        {
            if (query_with_output->settings_ast)
                InterpreterSetQuery(query_with_output->settings_ast, context).executeForCurrentContext();
        }

        if (auto * create_query = ast->as<ASTCreateQuery>())
        {
            if (create_query->select)
            {
                applySettingsFromSelectWithUnion(create_query->select->as<ASTSelectWithUnionQuery &>(), context);
            }
        }

        auto * insert_query = ast->as<ASTInsertQuery>();

        if (insert_query && insert_query->settings_ast)
            InterpreterSetQuery(insert_query->settings_ast, context).executeForCurrentContext();

        if (insert_query)
        {
            if (insert_query->data)
                query_end = insert_query->data;
            else
                query_end = end;

            insert_query->tail = istr;
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
        logQuery(query_for_logging, context, internal, stage);

        if (!internal)
        {
            onExceptionBeforeStart(query_for_logging, context, time_in_microseconds(current_time), ast);
        }

        throw;
    }

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

    /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
    String query(begin, query_end);
    BlockIO res;

    String query_for_logging;
    std::shared_ptr<InterpreterTransactionControlQuery> implicit_txn_control{};

    try
    {
        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        if (context->hasQueryParameters())
        {
            ReplaceQueryParameterVisitor visitor(context->getQueryParameters());
            visitor.visit(ast);
            query = serializeAST(*ast);
        }

        if (const auto * query_with_table_output = dynamic_cast<const ASTQueryWithTableAndOutput *>(ast.get()))
        {
            query_database = query_with_table_output->getDatabase();
            query_table = query_with_table_output->getTable();
        }

        /// MUST go before any modification (except for prepared statements,
        /// since it substitute parameters and without them query does not contain
        /// parameters), to keep query as-is in query_log and server log.
        query_for_logging = prepareQueryForLogging(query, context);
        logQuery(query_for_logging, context, internal, stage);

        /// Propagate WITH statement to children ASTSelect.
        if (settings.enable_global_with_statement)
        {
            ApplyWithGlobalVisitor().visit(ast);
        }

        {
            SelectIntersectExceptQueryVisitor::Data data;
            SelectIntersectExceptQueryVisitor{data}.visit(ast);
        }

        {
            /// Normalize SelectWithUnionQuery
            NormalizeSelectWithUnionQueryVisitor::Data data{context->getSettingsRef().union_default_mode};
            NormalizeSelectWithUnionQueryVisitor{data}.visit(ast);
        }

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

        /// Resolve database before trying to use async insert feature - to properly hash the query.
        if (insert_query)
        {
            if (insert_query->table_id)
                insert_query->table_id = context->resolveStorageID(insert_query->table_id);
            else if (auto table = insert_query->getTable(); !table.empty())
                insert_query->table_id = context->resolveStorageID(StorageID{insert_query->getDatabase(), table});
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
                    StoragePtr storage = context->executeTableFunction(input_function);
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

        auto * queue = context->getAsynchronousInsertQueue();
        const bool async_insert = queue
            && insert_query && !insert_query->select
            && insert_query->hasInlinedData() && settings.async_insert;

        if (async_insert)
        {
            quota = context->getQuota();
            if (quota)
            {
                quota->used(QuotaType::QUERY_INSERTS, 1);
                quota->used(QuotaType::QUERIES, 1);
                quota->checkExceeded(QuotaType::ERRORS);
            }

            queue->push(ast, context);

            if (settings.wait_for_async_insert)
            {
                auto timeout = settings.wait_for_async_insert_timeout.totalMilliseconds();
                auto query_id = context->getCurrentQueryId();
                auto source = std::make_shared<WaitForAsyncInsertSource>(query_id, timeout, *queue);
                res.pipeline = QueryPipeline(Pipe(std::move(source)));
            }

            const auto & table_id = insert_query->table_id;
            if (!table_id.empty())
                context->setInsertionTable(table_id);

            if (context->getCurrentTransaction() && settings.throw_on_unsupported_query_inside_transaction)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async inserts inside transactions are not supported");
            if (settings.implicit_transaction && settings.throw_on_unsupported_query_inside_transaction)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Async inserts with 'implicit_transaction' are not supported");
        }
        else
        {
            /// We need to start the (implicit) transaction before getting the interpreter as this will get links to the latest snapshots
            if (!context->getCurrentTransaction() && settings.implicit_transaction && !ast->as<ASTTransactionControl>())
            {
                try
                {
                    if (context->isGlobalContext())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot create transactions");

                    /// If there is no session (which is the default for the HTTP Handler), set up one just for this as it is necessary
                    /// to control the transaction lifetime
                    if (!context->hasSessionContext())
                        context->makeSessionContext();

                    auto tc = std::make_shared<InterpreterTransactionControlQuery>(ast, context);
                    tc->executeBegin(context->getSessionContext());
                    implicit_txn_control = std::move(tc);
                }
                catch (Exception & e)
                {
                    e.addMessage("while starting a transaction with 'implicit_transaction'");
                    throw;
                }
            }

            interpreter = InterpreterFactory::get(ast, context, SelectQueryOptions(stage).setInternal(internal));

            if (context->getCurrentTransaction() && !interpreter->supportsTransactions() &&
                context->getSettingsRef().throw_on_unsupported_query_inside_transaction)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported for this type of query ({})", ast->getID());

            if (!interpreter->ignoreQuota())
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
                limits.mode = LimitsMode::LIMITS_CURRENT; //-V1048
                limits.size_limits = SizeLimits(settings.max_result_rows, settings.max_result_bytes, settings.result_overflow_mode);
            }

            if (const auto * insert_interpreter = typeid_cast<const InterpreterInsertQuery *>(&*interpreter))
            {
                /// Save insertion table (not table function). TODO: support remote() table function.
                auto table_id = insert_interpreter->getDatabaseTable();
                if (!table_id.empty())
                    context->setInsertionTable(std::move(table_id));
            }

            {
                std::unique_ptr<OpenTelemetrySpanHolder> span;
                if (context->query_trace_context.trace_id != UUID())
                {
                    auto * raw_interpreter_ptr = interpreter.get();
                    std::string class_name(demangle(typeid(*raw_interpreter_ptr).name()));
                    span = std::make_unique<OpenTelemetrySpanHolder>(class_name + "::execute()");
                }
                res = interpreter->execute();
            }
        }

        if (process_list_entry)
        {
            /// Query was killed before execution
            if ((*process_list_entry)->isKilled())
                throw Exception("Query '" + (*process_list_entry)->getInfo().client_info.current_query_id + "' is killed in pending state",
                    ErrorCodes::QUERY_WAS_CANCELLED);
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
            QueryLogElement elem;

            elem.type = QueryLogElementType::QUERY_START; //-V1048

            elem.event_time = time_in_seconds(current_time);
            elem.event_time_microseconds = time_in_microseconds(current_time);
            elem.query_start_time = time_in_seconds(current_time);
            elem.query_start_time_microseconds = time_in_microseconds(current_time);

            elem.current_database = context->getCurrentDatabase();
            elem.query = query_for_logging;
            if (settings.log_formatted_queries)
                elem.formatted_query = queryToString(ast);
            elem.normalized_query_hash = normalizedQueryHash<false>(query_for_logging);

            elem.client_info = client_info;

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
                    elem.query_projections = info.projections;
                    elem.query_views = info.views;
                }

                if (async_insert)
                    InterpreterInsertQuery::extendQueryLogElemImpl(elem, context);
                else if (interpreter)
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
            auto status_info_to_query_log = [](QueryLogElement & element, const QueryStatusInfo & info, const ASTPtr query_ast, const ContextPtr context_ptr) mutable
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
                else
                {
                    ProfileEvents::increment(ProfileEvents::OtherQueryTimeMicroseconds, query_time);
                }

                element.query_duration_ms = info.elapsed_seconds * 1000;

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
            };

            /// Also make possible for caller to log successful query finish and exception during execution.
            auto finish_callback = [elem,
                                    context,
                                    ast,
                                    log_queries,
                                    log_queries_min_type = settings.log_queries_min_type,
                                    log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds(),
                                    log_processors_profiles = settings.log_processors_profiles,
                                    status_info_to_query_log,
                                    implicit_txn_control,
                                    pulling_pipeline = pipeline.pulling()](QueryPipeline & query_pipeline) mutable
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
                status_info_to_query_log(elem, info, ast, context);

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
                    Progress p(WriteProgress{info.written_rows, info.written_bytes});
                    p.incrementPiecewiseAtomically(Progress{ResultProgress{elem.result_rows, elem.result_bytes}});
                    progress_callback(p);
                }

                if (elem.read_rows != 0)
                {
                    LOG_INFO(&Poco::Logger::get("executeQuery"), "Read {} rows, {} in {} sec., {} rows/sec., {}/sec.",
                        elem.read_rows, ReadableSize(elem.read_bytes), elapsed_seconds,
                        static_cast<size_t>(elem.read_rows / elapsed_seconds),
                        ReadableSize(elem.read_bytes / elapsed_seconds));
                }

                if (log_queries && elem.type >= log_queries_min_type && static_cast<Int64>(elem.query_duration_ms) >= log_queries_min_query_duration_ms)
                {
                    if (auto query_log = context->getQueryLog())
                        query_log->add(elem);
                }
                if (log_processors_profiles)
                {
                    if (auto processors_profile_log = context->getProcessorsProfileLog())
                    {
                        ProcessorProfileLogElement processor_elem;
                        processor_elem.event_time = time_in_seconds(finish_time);
                        processor_elem.event_time_microseconds = time_in_microseconds(finish_time);
                        processor_elem.query_id = elem.client_info.current_query_id;

                        auto get_proc_id = [](const IProcessor & proc) -> UInt64
                        {
                            return reinterpret_cast<std::uintptr_t>(&proc);
                        };

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

                            processor_elem.processor_name = processor->getName();

                            processor_elem.elapsed_us = processor->getElapsedUs();
                            processor_elem.input_wait_elapsed_us = processor->getInputWaitElapsedUs();
                            processor_elem.output_wait_elapsed_us = processor->getOutputWaitElapsedUs();

                            processors_profile_log->add(processor_elem);
                        }
                    }
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

                    span.attributes.reserve(4);
                    span.attributes.push_back(Tuple{"clickhouse.query_status", "QueryFinish"});
                    span.attributes.push_back(Tuple{"db.statement", elem.query});
                    span.attributes.push_back(Tuple{"clickhouse.query_id", elem.client_info.current_query_id});
                    if (!context->query_trace_context.tracestate.empty())
                    {
                    span.attributes.push_back(Tuple{"clickhouse.tracestate", context->query_trace_context.tracestate});
                    }

                    opentelemetry_span_log->add(span);
                }

                if (implicit_txn_control)
                {
                    try
                    {
                        implicit_txn_control->executeCommit(context->getSessionContext());
                        implicit_txn_control.reset();
                    }
                    catch (const Exception &)
                    {
                        /// An exception might happen when trying to commit the transaction. For example we might get an immediate exception
                        /// because ZK is down and wait_changes_become_visible_after_commit_mode == WAIT_UNKNOWN
                        implicit_txn_control.reset();
                        throw;
                    }
                }
            };

            auto exception_callback = [elem,
                                       context,
                                       ast,
                                       log_queries,
                                       log_queries_min_type = settings.log_queries_min_type,
                                       log_queries_min_query_duration_ms = settings.log_queries_min_query_duration_ms.totalMilliseconds(),
                                       quota(quota),
                                       status_info_to_query_log,
                                       implicit_txn_control]() mutable
            {
                if (implicit_txn_control)
                {
                    implicit_txn_control->executeRollback(context->getSessionContext());
                    implicit_txn_control.reset();
                }
                else if (auto txn = context->getCurrentTransaction())
                    txn->onException();

                if (quota)
                    quota->used(QuotaType::ERRORS, 1, /* check_exceeded = */ false);

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
                    status_info_to_query_log(elem, info, ast, context);
                }

                if (current_settings.calculate_text_stack_trace)
                    setExceptionStackTrace(elem);
                logException(context, elem);

                /// In case of exception we log internal queries also
                if (log_queries && elem.type >= log_queries_min_type && static_cast<Int64>(elem.query_duration_ms) >= log_queries_min_query_duration_ms)
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
        }
    }
    catch (...)
    {
        if (implicit_txn_control)
        {
            implicit_txn_control->executeRollback(context->getSessionContext());
            implicit_txn_control.reset();
        }
        else if (auto txn = context->getCurrentTransaction())
        {
            txn->onException();
        }

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
        LimitReadBuffer limit(istr, max_query_size + 1, false);
        copyData(limit, out);
        out.finalize();

        begin = parse_buf.data();
        end = begin + parse_buf.size();
    }

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
                    throw Exception("INTO OUTFILE is not allowed", ErrorCodes::INTO_OUTFILE_NOT_ALLOWED);

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
                {},
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

            if (set_result_details)
                set_result_details(
                    context->getClientInfo().current_query_id, out->getContentType(), format_name, DateLUT::instance().getTimeZone());

            pipeline.complete(std::move(out));
        }
        else
        {
            pipeline.setProgressCallback(context->getProgressCallback());
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
