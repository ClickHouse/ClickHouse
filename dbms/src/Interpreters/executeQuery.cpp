#include <Common/formatReadable.h>
#include <Common/typeid_cast.h>

#include <IO/ConcatReadBuffer.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromVector.h>
#include <IO/LimitReadBuffer.h>
#include <IO/copyData.h>

#include <DataStreams/BlockIO.h>
#include <DataStreams/copyData.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/InputStreamFromASTInsertQuery.h>
#include <DataStreams/CountingBlockOutputStream.h>

#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTShowProcesslistQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/queryToString.h>

#include <Interpreters/Quota.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/QueryLog.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/executeQuery.h>
#include "DNSCacheUpdater.h"


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int QUERY_IS_TOO_LARGE;
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


/// NOTE This is wrong in case of single-line comments and in case of multiline string literals.
static String joinLines(const String & query)
{
    String res = query;
    std::replace(res.begin(), res.end(), '\n', ' ');
    return res;
}


/// Log query into text log (not into system table).
static void logQuery(const String & query, const Context & context, bool internal)
{
    if (internal)
    {
        LOG_DEBUG(&Logger::get("executeQuery"), "(internal) " << joinLines(query));
    }
    else
    {
        const auto & current_query_id = context.getClientInfo().current_query_id;
        const auto & initial_query_id = context.getClientInfo().initial_query_id;
        const auto & current_user = context.getClientInfo().current_user;

        LOG_DEBUG(&Logger::get("executeQuery"), "(from " << context.getClientInfo().current_address.toString()
            << (current_user != "default" ? ", user: " + context.getClientInfo().current_user : "")
            << (!initial_query_id.empty() && current_query_id != initial_query_id ? ", initial_query_id: " + initial_query_id : std::string())
            << ") "
            << joinLines(query));
    }
}


/// Call this inside catch block.
static void setExceptionStackTrace(QueryLogElement & elem)
{
    try
    {
        throw;
    }
    catch (const Exception & e)
    {
        elem.stack_trace = e.getStackTrace().toString();
    }
    catch (...) {}
}


/// Log exception (with query info) into text log (not into system table).
static void logException(Context & context, QueryLogElement & elem)
{
    LOG_ERROR(&Logger::get("executeQuery"), elem.exception
        << " (from " << context.getClientInfo().current_address.toString() << ")"
        << " (in query: " << joinLines(elem.query) << ")"
        << (!elem.stack_trace.empty() ? ", Stack trace:\n\n" + elem.stack_trace : ""));
}


static void onExceptionBeforeStart(const String & query, Context & context, time_t current_time)
{
    /// Exception before the query execution.
    context.getQuota().addError();

    const Settings & settings = context.getSettingsRef();

    /// Log the start of query execution into the table if necessary.
    QueryLogElement elem;

    elem.type = QueryLogElement::EXCEPTION_BEFORE_START;

    elem.event_time = current_time;
    elem.query_start_time = current_time;

    elem.query = query.substr(0, settings.log_queries_cut_to_length);
    elem.exception = getCurrentExceptionMessage(false);

    elem.client_info = context.getClientInfo();

    if (settings.calculate_text_stack_trace)
        setExceptionStackTrace(elem);
    logException(context, elem);

    /// Update performance counters before logging to query_log
    CurrentThread::finalizePerformanceCounters();

    if (settings.log_queries)
        if (auto query_log = context.getQueryLog())
            query_log->add(elem);
}


static std::tuple<ASTPtr, BlockIO> executeQueryImpl(
    const char * begin,
    const char * end,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool has_query_tail)
{
    time_t current_time = time(nullptr);

    context.setQueryContext(context);
    CurrentThread::attachQueryContext(context);

    const Settings & settings = context.getSettingsRef();

    ParserQuery parser(end, settings.enable_debug_queries);
    ASTPtr ast;
    const char * query_end;

    /// Don't limit the size of internal queries.
    size_t max_query_size = 0;
    if (!internal)
        max_query_size = settings.max_query_size;

    try
    {
        /// TODO Parser should fail early when max_query_size limit is reached.
        ast = parseQuery(parser, begin, end, "", max_query_size);

        auto * insert_query = ast->as<ASTInsertQuery>();

        if (insert_query && insert_query->settings_ast)
            InterpreterSetQuery(insert_query->settings_ast, context).executeForCurrentContext();

        if (insert_query && insert_query->data)
        {
            query_end = insert_query->data;
            insert_query->has_tail = has_query_tail;
        }
        else
            query_end = end;
    }
    catch (...)
    {
        /// Anyway log the query.
        String query = String(begin, begin + std::min(end - begin, static_cast<ptrdiff_t>(max_query_size)));
        logQuery(query.substr(0, settings.log_queries_cut_to_length), context, internal);

        if (!internal)
            onExceptionBeforeStart(query, context, current_time);

        throw;
    }

    /// Copy query into string. It will be written to log and presented in processlist. If an INSERT query, string will not include data to insertion.
    String query(begin, query_end);
    BlockIO res;

    try
    {
        logQuery(query.substr(0, settings.log_queries_cut_to_length), context, internal);

        /// Check the limits.
        checkASTSizeLimits(*ast, settings);

        QuotaForIntervals & quota = context.getQuota();

        quota.addQuery();    /// NOTE Seems that when new time interval has come, first query is not accounted in number of queries.
        quota.checkExceeded(current_time);

        /// Put query to process list. But don't put SHOW PROCESSLIST query itself.
        ProcessList::EntryPtr process_list_entry;
        if (!internal && !ast->as<ASTShowProcesslistQuery>())
        {
            process_list_entry = context.getProcessList().insert(query, ast.get(), context);
            context.setProcessListElement(&process_list_entry->get());
        }

        /// Load external tables if they were provided
        context.initializeExternalTablesIfSet();

        auto interpreter = InterpreterFactory::get(ast, context, stage);
        res = interpreter->execute();
        if (auto * insert_interpreter = typeid_cast<const InterpreterInsertQuery *>(&*interpreter))
            context.setInsertionTable(insert_interpreter->getDatabaseTable());

        if (process_list_entry)
        {
            /// Query was killed before execution
            if ((*process_list_entry)->isKilled())
                throw Exception("Query '" + (*process_list_entry)->getInfo().client_info.current_query_id + "' is killed in pending state",
                    ErrorCodes::QUERY_WAS_CANCELLED);
            else
                (*process_list_entry)->setQueryStreams(res);
        }

        /// Hold element of process list till end of query execution.
        res.process_list_entry = process_list_entry;

        if (res.in)
        {
            res.in->setProgressCallback(context.getProgressCallback());
            res.in->setProcessListElement(context.getProcessListElement());

            /// Limits on the result, the quota on the result, and also callback for progress.
            /// Limits apply only to the final result.
            if (stage == QueryProcessingStage::Complete)
            {
                IBlockInputStream::LocalLimits limits;
                limits.mode = IBlockInputStream::LIMITS_CURRENT;
                limits.size_limits = SizeLimits(settings.max_result_rows, settings.max_result_bytes, settings.result_overflow_mode);

                res.in->setLimits(limits);
                res.in->setQuota(quota);
            }
        }

        if (res.out)
        {
            if (auto stream = dynamic_cast<CountingBlockOutputStream *>(res.out.get()))
            {
                stream->setProcessListElement(context.getProcessListElement());
            }
        }

        /// Everything related to query log.
        {
            QueryLogElement elem;

            elem.type = QueryLogElement::QUERY_START;

            elem.event_time = current_time;
            elem.query_start_time = current_time;

            elem.query = query.substr(0, settings.log_queries_cut_to_length);

            elem.client_info = context.getClientInfo();

            bool log_queries = settings.log_queries && !internal;

            /// Log into system table start of query execution, if need.
            if (log_queries)
            {
                if (settings.log_query_settings)
                    elem.query_settings = std::make_shared<Settings>(context.getSettingsRef());

                if (auto query_log = context.getQueryLog())
                    query_log->add(elem);
            }

            /// Also make possible for caller to log successful query finish and exception during execution.
            res.finish_callback = [elem, &context, log_queries] (IBlockInputStream * stream_in, IBlockOutputStream * stream_out) mutable
            {
                QueryStatus * process_list_elem = context.getProcessListElement();

                if (!process_list_elem)
                    return;

                /// Update performance counters before logging to query_log
                CurrentThread::finalizePerformanceCounters();

                QueryStatusInfo info = process_list_elem->getInfo(true, context.getSettingsRef().log_profile_events);

                double elapsed_seconds = info.elapsed_seconds;

                elem.type = QueryLogElement::QUERY_FINISH;

                elem.event_time = time(nullptr);
                elem.query_duration_ms = elapsed_seconds * 1000;

                elem.read_rows = info.read_rows;
                elem.read_bytes = info.read_bytes;

                elem.written_rows = info.written_rows;
                elem.written_bytes = info.written_bytes;

                elem.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;

                if (stream_in)
                {
                    const BlockStreamProfileInfo & stream_in_info = stream_in->getProfileInfo();

                    /// NOTE: INSERT SELECT query contains zero metrics
                    elem.result_rows = stream_in_info.rows;
                    elem.result_bytes = stream_in_info.bytes;
                }
                else if (stream_out) /// will be used only for ordinary INSERT queries
                {
                    if (auto counting_stream = dynamic_cast<const CountingBlockOutputStream *>(stream_out))
                    {
                        /// NOTE: Redundancy. The same values could be extracted from process_list_elem->progress_out.query_settings = process_list_elem->progress_in
                        elem.result_rows = counting_stream->getProgress().rows;
                        elem.result_bytes = counting_stream->getProgress().bytes;
                    }
                }

                if (elem.read_rows != 0)
                {
                    LOG_INFO(&Logger::get("executeQuery"), std::fixed << std::setprecision(3)
                        << "Read " << elem.read_rows << " rows, "
                        << formatReadableSizeWithBinarySuffix(elem.read_bytes) << " in " << elapsed_seconds << " sec., "
                        << static_cast<size_t>(elem.read_rows / elapsed_seconds) << " rows/sec., "
                        << formatReadableSizeWithBinarySuffix(elem.read_bytes / elapsed_seconds) << "/sec.");
                }

                elem.thread_numbers = std::move(info.thread_numbers);
                elem.profile_counters = std::move(info.profile_counters);

                if (log_queries)
                {
                    if (auto query_log = context.getQueryLog())
                        query_log->add(elem);
                }
            };

            res.exception_callback = [elem, &context, log_queries] () mutable
            {
                context.getQuota().addError();

                elem.type = QueryLogElement::EXCEPTION_WHILE_PROCESSING;

                elem.event_time = time(nullptr);
                elem.query_duration_ms = 1000 * (elem.event_time - elem.query_start_time);
                elem.exception = getCurrentExceptionMessage(false);

                QueryStatus * process_list_elem = context.getProcessListElement();
                const Settings & current_settings = context.getSettingsRef();

                /// Update performance counters before logging to query_log
                CurrentThread::finalizePerformanceCounters();

                if (process_list_elem)
                {
                    QueryStatusInfo info = process_list_elem->getInfo(true, current_settings.log_profile_events, false);

                    elem.query_duration_ms = info.elapsed_seconds * 1000;

                    elem.read_rows = info.read_rows;
                    elem.read_bytes = info.read_bytes;

                    elem.memory_usage = info.peak_memory_usage > 0 ? info.peak_memory_usage : 0;

                    elem.thread_numbers = std::move(info.thread_numbers);
                    elem.profile_counters = std::move(info.profile_counters);
                }

                if (current_settings.calculate_text_stack_trace)
                    setExceptionStackTrace(elem);
                logException(context, elem);

                /// In case of exception we log internal queries also
                if (log_queries)
                {
                    if (auto query_log = context.getQueryLog())
                        query_log->add(elem);
                }
            };

            if (!internal && res.in)
            {
                std::stringstream log_str;
                log_str << "Query pipeline:\n";
                res.in->dumpTree(log_str);
                LOG_DEBUG(&Logger::get("executeQuery"), log_str.str());
            }
        }
    }
    catch (...)
    {
        if (!internal)
            onExceptionBeforeStart(query, context, current_time);

        DNSCacheUpdater::incrementNetworkErrorEventsIfNeeded();

        throw;
    }

    return std::make_tuple(ast, res);
}


BlockIO executeQuery(
    const String & query,
    Context & context,
    bool internal,
    QueryProcessingStage::Enum stage,
    bool may_have_embedded_data)
{
    BlockIO streams;
    std::tie(std::ignore, streams) = executeQueryImpl(query.data(), query.data() + query.size(), context, internal, stage, !may_have_embedded_data);
    return streams;
}


void executeQuery(
    ReadBuffer & istr,
    WriteBuffer & ostr,
    bool allow_into_outfile,
    Context & context,
    std::function<void(const String &)> set_content_type,
    std::function<void(const String &)> set_query_id)
{
    PODArray<char> parse_buf;
    const char * begin;
    const char * end;

    /// If 'istr' is empty now, fetch next data into buffer.
    if (istr.buffer().size() == 0)
        istr.next();

    size_t max_query_size = context.getSettingsRef().max_query_size;

    bool may_have_tail;
    if (istr.buffer().end() - istr.position() > static_cast<ssize_t>(max_query_size))
    {
        /// If remaining buffer space in 'istr' is enough to parse query up to 'max_query_size' bytes, then parse inplace.
        begin = istr.position();
        end = istr.buffer().end();
        istr.position() += end - begin;
        /// Actually we don't know will query has additional data or not.
        /// But we can't check istr.eof(), because begin and end pointers will became invalid
        may_have_tail = true;
    }
    else
    {
        /// If not - copy enough data into 'parse_buf'.
        WriteBufferFromVector<PODArray<char>> out(parse_buf);
        LimitReadBuffer limit(istr, max_query_size + 1, false);
        copyData(limit, out);
        out.finish();

        begin = parse_buf.data();
        end = begin + parse_buf.size();
        /// Can check stream for eof, because we have copied data
        may_have_tail = !istr.eof();
    }

    ASTPtr ast;
    BlockIO streams;

    std::tie(ast, streams) = executeQueryImpl(begin, end, context, false, QueryProcessingStage::Complete, may_have_tail);

    try
    {
        if (streams.out)
        {
            InputStreamFromASTInsertQuery in(ast, &istr, streams.out->getHeader(), context);
            copyData(in, *streams.out);
        }

        if (streams.in)
        {
            /// FIXME: try to prettify this cast using `as<>()`
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
                ? *getIdentifierName(ast_query_with_output->format)
                : context.getDefaultFormat();

            if (ast_query_with_output && ast_query_with_output->settings_ast)
                InterpreterSetQuery(ast_query_with_output->settings_ast, context).executeForCurrentContext();

            BlockOutputStreamPtr out = context.getOutputFormat(format_name, *out_buf, streams.in->getHeader());

            /// Save previous progress callback if any. TODO Do it more conveniently.
            auto previous_progress_callback = context.getProgressCallback();

            /// NOTE Progress callback takes shared ownership of 'out'.
            streams.in->setProgressCallback([out, previous_progress_callback] (const Progress & progress)
            {
                if (previous_progress_callback)
                    previous_progress_callback(progress);
                out->onProgress(progress);
            });

            if (set_content_type)
                set_content_type(out->getContentType());

            if (set_query_id)
                set_query_id(context.getClientInfo().current_query_id);

            copyData(*streams.in, *out);
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
