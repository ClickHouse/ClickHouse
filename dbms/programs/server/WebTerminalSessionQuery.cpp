#include "WebTerminalSessionQuery.h"

#include <Parsers/formatAST.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Interpreters/executeQuery.h>
#include <DataStreams/NullBlockInputStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <Common/setThreadName.h>
#include <ext/scope_guard.h>
#include <IO/Operators.h>
#include <Common/config_version.h>
#include <IO/copyData.h>
#include <DataStreams/copyData.h>
#include <Formats/FormatFactory.h>
#include <IO/NullWriteBuffer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int QUERY_WAS_CANCELLED;
}

static auto parseQuery(const String & parse_query, const Context & context)
{
    const char * query_begin = parse_query.data();
    const char * query_end = query_begin + parse_query.size();

    const auto & settings = context.getSettingsRef();
    ParserQuery query_parser(query_end, settings.enable_debug_queries);
    const auto & parsed_query = parseQuery(query_parser, query_begin, query_end, "", settings.max_query_size, settings.max_parser_depth);
    return std::make_pair(parsed_query, serializeAST(*parsed_query));
}

static std::unique_ptr<CurrentThread::QueryScope> makeQueryScope(std::unique_ptr<Context> & context)
{
    /// TODO: Log queue
    return std::make_unique<CurrentThread::QueryScope>(*context);
}

static std::unique_ptr<Context> makeQueryContext(const Context & context, const String & query_id, Progress & progress)
{
    auto query_context = std::make_unique<Context>(context);
    query_context->setCurrentQueryId(query_id);
    query_context->setProgressCallback([&progress] (const Progress & value) { progress.incrementPiecewiseAtomically(value); });
    return query_context;
}

WebTerminalSessionQuery::WebTerminalSessionQuery(
    Context & context_, const String & execute_query, const String & query_id, bool has_vertical_output_suffix_)
    : context(context_), query_context(makeQueryContext(context, query_id, progress)), query_scope(makeQueryScope(query_context))
{
    std::tie(parsed_query, serialized_query) = parseQuery(execute_query, context);

    out = std::make_shared<RingMemoryReadWriteBuffer>(10, DBMS_DEFAULT_BUFFER_SIZE);
    streams = executeQuery(serialized_query, *query_context, false, QueryProcessingStage::Complete, true);

    if (!streams.pipeline.initialized())
        executeQueryWithStreams(streams.in, has_vertical_output_suffix_ ? "Vertical" : "PrettyCompact");
    else
        executeQueryWithProcessors(streams.pipeline, has_vertical_output_suffix_ ? "Vertical" : "PrettyCompact");
}

void WebTerminalSessionQuery::getEchoQuery(const WriteBufferPtr & write_buffer)
{
    std::stringstream ss;
    formatAST(*parsed_query, ss);

    *write_buffer << "{" << DB::double_quote << "type" << ":" << DB::double_quote << "started_query"
        << "," << DB::double_quote << "running_query_id" << ":" << DB::double_quote << query_context->getCurrentQueryId()
        << "," << DB::double_quote << "echo_formatted_query" << ":";

    writeJSONString(ss.str(), *write_buffer, FormatSettings{});
    *write_buffer << "}";
}

bool WebTerminalSessionQuery::getOutput(const WriteBufferPtr & write_buffer, const SendHeaders & set_headers)
{
    set_headers(progress.fetchAndResetPiecewiseAtomically(), total_format_rows.fetch_and(0));
    const auto & read_buffer = out->tryGetReadBuffer(DBMS_DEFAULT_BUFFER_SIZE * 2, 500);
    copyData(*read_buffer, *write_buffer);
    return finished.load(std::memory_order_seq_cst) && !read_buffer->count();
}

void WebTerminalSessionQuery::executeQueryWithStreams(BlockInputStreamPtr & input, const String & output_format)
{
    executeQueryImpl([&, output_format]() -> void
    {
        if (input)
        {
            const auto & header = input->getHeader();
            const auto & output = query_context->getOutputFormat(output_format, *out, header);
            copyData(*input, *output,
                [&]() { return cancelled.load(std::memory_order_seq_cst); },
                [&](const Block & data) { total_format_rows += data.rows(); }
            );
        }
    });
}

void WebTerminalSessionQuery::executeQueryWithProcessors(QueryPipeline & query_pipeline, const String & output_format)
{
    const auto & header = query_pipeline.getHeader();
    FormatFactory::WriteCallback write_callback = [&](const Columns &, size_t) { ++total_format_rows; };

    if (output_format == "PrettyCompact")
        write_callback = [&](const Columns &, size_t row) { total_format_rows += row; };

    query_pipeline.setOutput(
        FormatFactory::instance().getOutputFormat(
            output_format, *out, header,
            *query_context, write_callback
    ));

    executor = query_pipeline.execute();

    executeQueryImpl([&]() -> void
    {
        size_t num_threads = query_context->getSettingsRef().max_threads;
        num_threads = std::min(num_threads, query_pipeline.getNumThreads());

        executor->execute(num_threads);
    });
}

template<typename Function>
void WebTerminalSessionQuery::executeQueryImpl(const Function & function)
{
    background_pool.scheduleOrThrowOnError([this, function, thread_group = CurrentThread::getGroup()]()
    {
        CurrentMetrics::Increment metric_increment{CurrentMetrics::QueryThread};

        try
        {
            setThreadName("TerminalExec");

            if (thread_group)
                CurrentThread::attachToIfDetached(thread_group);

            function();

            out->finalize();
            streams.onFinish();
            finished = true;
        }
        catch (...)
        {
            const auto & exception_code = getCurrentExceptionCode();
            const auto & exception_message = getCurrentExceptionMessage(false, false, true);

            if (!cancelled.load(std::memory_order_seq_cst) || exception_code != ErrorCodes::QUERY_WAS_CANCELLED)
                *out << "\nReceived exception from server (version " << VERSION_STRING << "): \n"
                     << "Code: " << exception_code << ". " << exception_message << "\n";

            out->finalize();
            streams.onException();
            finished = true;
        }
    });
}

void WebTerminalSessionQuery::cancelQuery()
{
    cancelled = true;

    if (executor)
        executor->cancel();
}

WebTerminalSessionQuery::~WebTerminalSessionQuery()
{
    try
    {
        if (!finished)
        {
            /// Destroy query, this query may be executing.
            cancelled = true;

            if (executor)
                executor->cancel();

            NullWriteBuffer null_buffer;

            while (true)
            {
                /// Write not pulled results to NullWriteBuffer
                const auto & read_buffer = out->tryGetReadBuffer(DBMS_DEFAULT_BUFFER_SIZE * 2, 500);
                copyData(*read_buffer, null_buffer);

                if (finished.load(std::memory_order_seq_cst) && !read_buffer->count())
                    break;
            }

            background_pool.wait();
        }
    }
    catch(...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);;
    }
}

}
