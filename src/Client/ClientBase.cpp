#include <Client/ClientBase.h>

#include <iostream>
#include <iomanip>
#include <string_view>
#include <filesystem>

#include <base/argsToConfig.h>
#include <base/DateLUT.h>
#include <base/LocalDate.h>
#include <base/LineReader.h>
#include <base/scope_guard_safe.h>
#include "Columns/ColumnString.h"
#include "Columns/ColumnsNumber.h"
#include "Core/Block.h"
#include "Core/Protocol.h"

#include <Common/config_version.h>
#include <Common/UTF8Helpers.h>
#include <Common/TerminalSize.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/Config/configReadClient.h>
#include <Common/NetException.h>
#include <Storages/ColumnsDescription.h>

#include <Client/ClientBaseHelpers.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <QueryPipeline/QueryPipeline.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/CompressionMethod.h>
#include <Client/InternalTextLogs.h>

namespace fs = std::filesystem;
using namespace std::literals;


namespace DB
{

static const NameSet exit_strings
{
    "exit", "quit", "logout", "учше", "йгше", "дщпщге",
    "exit;", "quit;", "logout;", "учшеж", "йгшеж", "дщпщгеж",
    "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"
};

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DEADLOCK_AVOIDED;
    extern const int CLIENT_OUTPUT_FORMAT_SPECIFIED;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int NO_DATA_TO_INSERT;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int CANNOT_SET_SIGNAL_HANDLER;
    extern const int UNRECOGNIZED_ARGUMENTS;
}

}

namespace ProfileEvents
{
    extern const Event UserTimeMicroseconds;
    extern const Event SystemTimeMicroseconds;
}

namespace DB
{


std::atomic_flag exit_on_signal = ATOMIC_FLAG_INIT;

class QueryInterruptHandler : private boost::noncopyable
{
public:
    QueryInterruptHandler() { exit_on_signal.clear(); }

    ~QueryInterruptHandler() { exit_on_signal.test_and_set(); }

    static bool cancelled() { return exit_on_signal.test(); }
};

/// This signal handler is set only for sigint.
void interruptSignalHandler(int signum)
{
    if (exit_on_signal.test_and_set())
        _exit(signum);
}


ClientBase::~ClientBase() = default;
ClientBase::ClientBase() = default;


void ClientBase::setupSignalHandler()
{
     exit_on_signal.test_and_set();

     struct sigaction new_act;
     memset(&new_act, 0, sizeof(new_act));

     new_act.sa_handler = interruptSignalHandler;
     new_act.sa_flags = 0;

#if defined(OS_DARWIN)
    sigemptyset(&new_act.sa_mask);
#else
     if (sigemptyset(&new_act.sa_mask))
        throw Exception(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler.");
#endif

     if (sigaction(SIGINT, &new_act, nullptr))
        throw Exception(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler.");
}


ASTPtr ClientBase::parseQuery(const char *& pos, const char * end, bool allow_multi_statements) const
{
    ParserQuery parser(end);
    ASTPtr res;

    const auto & settings = global_context->getSettingsRef();
    size_t max_length = 0;

    if (!allow_multi_statements)
        max_length = settings.max_query_size;

    if (is_interactive || ignore_error)
    {
        String message;
        res = tryParseQuery(parser, pos, end, message, true, "", allow_multi_statements, max_length, settings.max_parser_depth);

        if (!res)
        {
            std::cerr << std::endl << message << std::endl << std::endl;
            return nullptr;
        }
    }
    else
    {
        res = parseQueryAndMovePosition(parser, pos, end, "", allow_multi_statements, max_length, settings.max_parser_depth);
    }

    if (is_interactive)
    {
        std::cout << std::endl;
        WriteBufferFromOStream res_buf(std::cout, 4096);
        formatAST(*res, res_buf);
        res_buf.next();
        std::cout << std::endl << std::endl;
    }

    return res;
}


/// Consumes trailing semicolons and tries to consume the same-line trailing comment.
void ClientBase::adjustQueryEnd(const char *& this_query_end, const char * all_queries_end, int max_parser_depth)
{
    // We have to skip the trailing semicolon that might be left
    // after VALUES parsing or just after a normal semicolon-terminated query.
    Tokens after_query_tokens(this_query_end, all_queries_end);
    IParser::Pos after_query_iterator(after_query_tokens, max_parser_depth);
    while (after_query_iterator.isValid() && after_query_iterator->type == TokenType::Semicolon)
    {
        this_query_end = after_query_iterator->end;
        ++after_query_iterator;
    }

    // Now we have to do some extra work to add the trailing
    // same-line comment to the query, but preserve the leading
    // comments of the next query. The trailing comment is important
    // because the test hints are usually written this way, e.g.:
    // select nonexistent_column; -- { serverError 12345 }.
    // The token iterator skips comments and whitespace, so we have
    // to find the newline in the string manually. If it's earlier
    // than the next significant token, it means that the text before
    // newline is some trailing whitespace or comment, and we should
    // add it to our query. There are also several special cases
    // that are described below.
    const auto * newline = find_first_symbols<'\n'>(this_query_end, all_queries_end);
    const char * next_query_begin = after_query_iterator->begin;

    // We include the entire line if the next query starts after
    // it. This is a generic case of trailing in-line comment.
    // The "equals" condition is for case of end of input (they both equal
    // all_queries_end);
    if (newline <= next_query_begin)
    {
        assert(newline >= this_query_end);
        this_query_end = newline;
    }
    else
    {
        // Many queries on one line, can't do anything. By the way, this
        // syntax is probably going to work as expected:
        // select nonexistent /* { serverError 12345 } */; select 1
    }
}


/// Convert external tables to ExternalTableData and send them using the connection.
void ClientBase::sendExternalTables(ASTPtr parsed_query)
{
    const auto * select = parsed_query->as<ASTSelectWithUnionQuery>();
    if (!select && !external_tables.empty())
        throw Exception("External tables could be sent only with select query", ErrorCodes::BAD_ARGUMENTS);

    std::vector<ExternalTableDataPtr> data;
    for (auto & table : external_tables)
        data.emplace_back(table.getData(global_context));

    if (send_external_tables)
        connection->sendExternalTablesData(data);
}


void ClientBase::onData(Block & block, ASTPtr parsed_query)
{
    if (!block)
        return;

    processed_rows += block.rows();

    /// Even if all blocks are empty, we still need to initialize the output stream to write empty resultset.
    initBlockOutputStream(block, parsed_query);

    /// The header block containing zero rows was used to initialize
    /// output_format, do not output it.
    /// Also do not output too much data if we're fuzzing.
    if (block.rows() == 0 || (query_fuzzer_runs != 0 && processed_rows >= 100))
        return;

    /// If results are written INTO OUTFILE, we can avoid clearing progress to avoid flicker.
    if (need_render_progress && (stdout_is_a_tty || is_interactive) && !select_into_file)
        progress_indication.clearProgressOutput();

    output_format->write(materializeBlock(block));
    written_first_block = true;

    /// Received data block is immediately displayed to the user.
    output_format->flush();

    /// Restore progress bar after data block.
    if (need_render_progress && (stdout_is_a_tty || is_interactive))
    {
        if (select_into_file)
            std::cerr << "\r";
        progress_indication.writeProgress();
    }
}


void ClientBase::onLogData(Block & block)
{
    initLogsOutputStream();
    progress_indication.clearProgressOutput();
    logs_out_stream->writeLogs(block);
    logs_out_stream->flush();
}


void ClientBase::onTotals(Block & block, ASTPtr parsed_query)
{
    initBlockOutputStream(block, parsed_query);
    output_format->setTotals(block);
}


void ClientBase::onExtremes(Block & block, ASTPtr parsed_query)
{
    initBlockOutputStream(block, parsed_query);
    output_format->setExtremes(block);
}


void ClientBase::onReceiveExceptionFromServer(std::unique_ptr<Exception> && e)
{
    have_error = true;
    server_exception = std::move(e);
    resetOutput();
}


void ClientBase::onProfileInfo(const ProfileInfo & profile_info)
{
    if (profile_info.hasAppliedLimit() && output_format)
        output_format->setRowsBeforeLimit(profile_info.getRowsBeforeLimit());
}


void ClientBase::initBlockOutputStream(const Block & block, ASTPtr parsed_query)
{
    if (!output_format)
    {
        /// Ignore all results when fuzzing as they can be huge.
        if (query_fuzzer_runs)
        {
            output_format = std::make_shared<NullOutputFormat>(block);
            return;
        }

        WriteBuffer * out_buf = nullptr;
        String pager = config().getString("pager", "");
        if (!pager.empty())
        {
            signal(SIGPIPE, SIG_IGN);

            ShellCommand::Config config(pager);
            config.pipe_stdin_only = true;
            pager_cmd = ShellCommand::execute(config);
            out_buf = &pager_cmd->in;
        }
        else
        {
            out_buf = &std_out;
        }

        String current_format = format;

        select_into_file = false;

        /// The query can specify output format or output file.
        if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get()))
        {
            if (query_with_output->out_file)
            {
                select_into_file = true;

                const auto & out_file_node = query_with_output->out_file->as<ASTLiteral &>();
                const auto & out_file = out_file_node.value.safeGet<std::string>();

                std::string compression_method;
                if (query_with_output->compression)
                {
                    const auto & compression_method_node = query_with_output->compression->as<ASTLiteral &>();
                    compression_method = compression_method_node.value.safeGet<std::string>();
                }

                out_file_buf = wrapWriteBufferWithCompressionMethod(
                    std::make_unique<WriteBufferFromFile>(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT),
                    chooseCompressionMethod(out_file, compression_method),
                    /* compression level = */ 3
                );

                // We are writing to file, so default format is the same as in non-interactive mode.
                if (is_interactive && is_default_format)
                    current_format = "TabSeparated";
            }
            if (query_with_output->format != nullptr)
            {
                if (has_vertical_output_suffix)
                    throw Exception("Output format already specified", ErrorCodes::CLIENT_OUTPUT_FORMAT_SPECIFIED);
                const auto & id = query_with_output->format->as<ASTIdentifier &>();
                current_format = id.name();
            }
        }

        if (has_vertical_output_suffix)
            current_format = "Vertical";

        /// It is not clear how to write progress intermixed with data with parallel formatting.
        /// It may increase code complexity significantly.
        if (!need_render_progress || select_into_file)
            output_format = global_context->getOutputFormatParallelIfPossible(
                current_format, out_file_buf ? *out_file_buf : *out_buf, block);
        else
            output_format = global_context->getOutputFormat(
                current_format, out_file_buf ? *out_file_buf : *out_buf, block);

        output_format->doWritePrefix();
    }
}


void ClientBase::initLogsOutputStream()
{
    if (!logs_out_stream)
    {
        WriteBuffer * wb = out_logs_buf.get();

        if (!out_logs_buf)
        {
            if (server_logs_file.empty())
            {
                /// Use stderr by default
                out_logs_buf = std::make_unique<WriteBufferFromFileDescriptor>(STDERR_FILENO);
                wb = out_logs_buf.get();
            }
            else if (server_logs_file == "-")
            {
                /// Use stdout if --server_logs_file=- specified
                wb = &std_out;
            }
            else
            {
                out_logs_buf
                    = std::make_unique<WriteBufferFromFile>(server_logs_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
                wb = out_logs_buf.get();
            }
        }

        logs_out_stream = std::make_unique<InternalTextLogs>(*wb, stdout_is_a_tty);
    }
}


void ClientBase::processTextAsSingleQuery(const String & full_query)
{
    /// Some parts of a query (result output and formatting) are executed
    /// client-side. Thus we need to parse the query.
    const char * begin = full_query.data();
    auto parsed_query = parseQuery(begin, begin + full_query.size(), false);

    if (!parsed_query)
        return;

    String query_to_execute;

    // An INSERT query may have the data that follow query text. Remove the
    /// Send part of query without data, because data will be sent separately.
    auto * insert = parsed_query->as<ASTInsertQuery>();
    if (insert && insert->data)
        query_to_execute = full_query.substr(0, insert->data - full_query.data());
    else
        query_to_execute = full_query;

    try
    {
        processParsedSingleQuery(full_query, query_to_execute, parsed_query);
    }
    catch (Exception & e)
    {
        if (!is_interactive)
            e.addMessage("(in query: {})", full_query);
        throw;
    }

    if (have_error)
        processError(full_query);
}


void ClientBase::processOrdinaryQuery(const String & query_to_execute, ASTPtr parsed_query)
{
    /// Rewrite query only when we have query parameters.
    /// Note that if query is rewritten, comments in query are lost.
    /// But the user often wants to see comments in server logs, query log, processlist, etc.
    auto query = query_to_execute;
    if (!query_parameters.empty())
    {
        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        ReplaceQueryParameterVisitor visitor(query_parameters);
        visitor.visit(parsed_query);

        /// Get new query after substitutions. Note that it cannot be done for INSERT query with embedded data.
        query = serializeAST(*parsed_query);
    }

    int retries_left = 10;
    while (retries_left)
    {
        try
        {
            connection->sendQuery(
                connection_parameters.timeouts,
                query,
                global_context->getCurrentQueryId(),
                query_processing_stage,
                &global_context->getSettingsRef(),
                &global_context->getClientInfo(),
                true);

            if (send_external_tables)
                sendExternalTables(parsed_query);

            receiveResult(parsed_query);

            break;
        }
        catch (const Exception & e)
        {
            /// Retry when the server said "Client should retry" and no rows
            /// has been received yet.
            if (processed_rows == 0 && e.code() == ErrorCodes::DEADLOCK_AVOIDED && --retries_left)
            {
                std::cerr << "Got a transient error from the server, will"
                        << " retry (" << retries_left << " retries left)";
            }
            else
            {
                throw;
            }
        }
    }
    assert(retries_left > 0);
}


/// Receives and processes packets coming from server.
/// Also checks if query execution should be cancelled.
void ClientBase::receiveResult(ASTPtr parsed_query)
{
    bool cancelled = false;
    QueryInterruptHandler query_interrupt_handler;

    // TODO: get the poll_interval from commandline.
    const auto receive_timeout = connection_parameters.timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1000000; /// in microseconds
    constexpr size_t min_poll_interval = 5000; /// in microseconds
    const size_t poll_interval
        = std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));

    bool break_on_timeout = connection->getConnectionType() != IServerConnection::Type::LOCAL;
    while (true)
    {
        Stopwatch receive_watch(CLOCK_MONOTONIC_COARSE);

        while (true)
        {
            /// Has the Ctrl+C been pressed and thus the query should be cancelled?
            /// If this is the case, inform the server about it and receive the remaining packets
            /// to avoid losing sync.
            if (!cancelled)
            {
                auto cancel_query = [&] {
                    connection->sendCancel();
                    if (is_interactive)
                    {
                        progress_indication.clearProgressOutput();
                        std::cout << "Cancelling query." << std::endl;

                    }
                    cancelled = true;
                };

                /// handler received sigint
                if (query_interrupt_handler.cancelled())
                {
                    cancel_query();
                }
                else
                {
                    double elapsed = receive_watch.elapsedSeconds();
                    if (break_on_timeout && elapsed > receive_timeout.totalSeconds())
                    {
                        std::cout << "Timeout exceeded while receiving data from server."
                                    << " Waited for " << static_cast<size_t>(elapsed) << " seconds,"
                                    << " timeout is " << receive_timeout.totalSeconds() << " seconds." << std::endl;

                        cancel_query();
                    }
                }
            }

            /// Poll for changes after a cancellation check, otherwise it never reached
            /// because of progress updates from server.

            if (connection->poll(poll_interval))
                break;
        }

        if (!receiveAndProcessPacket(parsed_query, cancelled))
            break;
    }

    if (cancelled && is_interactive)
        std::cout << "Query was cancelled." << std::endl;
}


/// Receive a part of the result, or progress info or an exception and process it.
/// Returns true if one should continue receiving packets.
/// Output of result is suppressed if query was cancelled.
bool ClientBase::receiveAndProcessPacket(ASTPtr parsed_query, bool cancelled)
{
    Packet packet = connection->receivePacket();

    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
            return true;

        case Protocol::Server::Data:
            if (!cancelled)
                onData(packet.block, parsed_query);
            return true;

        case Protocol::Server::Progress:
            onProgress(packet.progress);
            return true;

        case Protocol::Server::ProfileInfo:
            onProfileInfo(packet.profile_info);
            return true;

        case Protocol::Server::Totals:
            if (!cancelled)
                onTotals(packet.block, parsed_query);
            return true;

        case Protocol::Server::Extremes:
            if (!cancelled)
                onExtremes(packet.block, parsed_query);
            return true;

        case Protocol::Server::Exception:
            onReceiveExceptionFromServer(std::move(packet.exception));
            return false;

        case Protocol::Server::Log:
            onLogData(packet.block);
            return true;

        case Protocol::Server::EndOfStream:
            onEndOfStream();
            return false;

        case Protocol::Server::ProfileEvents:
            onProfileEvents(packet.block);
            return true;

        default:
            throw Exception(
                ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
    }
}


void ClientBase::onProgress(const Progress & value)
{
    if (!progress_indication.updateProgress(value))
    {
        // Just a keep-alive update.
        return;
    }

    if (output_format)
        output_format->onProgress(value);

    if (need_render_progress)
        progress_indication.writeProgress();
}


void ClientBase::onEndOfStream()
{
    progress_indication.clearProgressOutput();

    if (output_format)
        output_format->doWriteSuffix();

    resetOutput();

    if (is_interactive && !written_first_block)
    {
        progress_indication.clearProgressOutput();
        std::cout << "Ok." << std::endl;
    }
}


void ClientBase::onProfileEvents(Block & block)
{
    const auto rows = block.rows();
    if (rows == 0)
        return;

    if (progress_indication.print_hardware_utilization)
    {
        const auto & array_thread_id = typeid_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
        const auto & names = typeid_cast<const ColumnString &>(*block.getByName("name").column);
        const auto & host_names = typeid_cast<const ColumnString &>(*block.getByName("host_name").column);
        const auto & array_values = typeid_cast<const ColumnUInt64 &>(*block.getByName("value").column).getData();

        const auto * user_time_name = ProfileEvents::getName(ProfileEvents::UserTimeMicroseconds);
        const auto * system_time_name = ProfileEvents::getName(ProfileEvents::SystemTimeMicroseconds);

        HostToThreadTimesMap thread_times;
        for (size_t i = 0; i < rows; ++i)
        {
            auto thread_id = array_thread_id[i];
            auto host_name = host_names.getDataAt(i).toString();
            if (thread_id != 0)
                progress_indication.addThreadIdToList(host_name, thread_id);
            auto event_name = names.getDataAt(i);
            auto value = array_values[i];
            if (event_name == user_time_name)
            {
                thread_times[host_name][thread_id].user_ms = value;
            }
            else if (event_name == system_time_name)
            {
                thread_times[host_name][thread_id].system_ms = value;
            }
            else if (event_name == MemoryTracker::USAGE_EVENT_NAME)
            {
                thread_times[host_name][thread_id].memory_usage = value;
            }
        }
        progress_indication.updateThreadEventData(thread_times);
    }

    if (profile_events.print)
    {
        if (profile_events.watch.elapsedMilliseconds() >= profile_events.delay_ms)
        {
            initLogsOutputStream();
            progress_indication.clearProgressOutput();
            logs_out_stream->writeProfileEvents(block);
            logs_out_stream->flush();

            profile_events.watch.restart();
            profile_events.last_block = {};
        }
        else
        {
            profile_events.last_block = block;
        }
    }
}


/// Flush all buffers.
void ClientBase::resetOutput()
{
    output_format.reset();
    logs_out_stream.reset();

    if (pager_cmd)
    {
        pager_cmd->in.close();
        pager_cmd->wait();
    }
    pager_cmd = nullptr;

    if (out_file_buf)
    {
        out_file_buf->next();
        out_file_buf.reset();
    }

    if (out_logs_buf)
    {
        out_logs_buf->next();
        out_logs_buf.reset();
    }

    std_out.next();
}


/// Receive the block that serves as an example of the structure of table where data will be inserted.
bool ClientBase::receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query)
{
    while (true)
    {
        Packet packet = connection->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::Data:
                out = packet.block;
                return true;

            case Protocol::Server::Exception:
                onReceiveExceptionFromServer(std::move(packet.exception));
                return false;

            case Protocol::Server::Log:
                onLogData(packet.block);
                break;

            case Protocol::Server::TableColumns:
                columns_description = ColumnsDescription::parse(packet.multistring_message[1]);
                return receiveSampleBlock(out, columns_description, parsed_query);

            default:
                throw NetException(
                    "Unexpected packet from server (expected Data, Exception or Log, got "
                        + String(Protocol::Server::toString(packet.type)) + ")",
                    ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
        }
    }
}


void ClientBase::processInsertQuery(const String & query_to_execute, ASTPtr parsed_query)
{
    /// Process the query that requires transferring data blocks to the server.
    const auto parsed_insert_query = parsed_query->as<ASTInsertQuery &>();
    if ((!parsed_insert_query.data && !parsed_insert_query.infile) && (is_interactive || (!stdin_is_a_tty && std_in.eof())))
        throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);

    connection->sendQuery(
        connection_parameters.timeouts,
        query_to_execute,
        global_context->getCurrentQueryId(),
        query_processing_stage,
        &global_context->getSettingsRef(),
        &global_context->getClientInfo(),
        true);

    if (send_external_tables)
        sendExternalTables(parsed_query);

    /// Receive description of table structure.
    Block sample;
    ColumnsDescription columns_description;
    if (receiveSampleBlock(sample, columns_description, parsed_query))
    {
        /// If structure was received (thus, server has not thrown an exception),
        /// send our data with that structure.
        sendData(sample, columns_description, parsed_query);
        receiveEndOfQuery();
    }
}


void ClientBase::sendData(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query)
{
    /// If INSERT data must be sent.
    auto * parsed_insert_query = parsed_query->as<ASTInsertQuery>();
    if (!parsed_insert_query)
        return;

    if (need_render_progress)
    {
        /// Set total_bytes_to_read for current fd.
        FileProgress file_progress(0, std_in.size());
        progress_indication.updateProgress(Progress(file_progress));

        /// Set callback to be called on file progress.
        progress_indication.setFileProgressCallback(global_context, true);
    }

    /// If data fetched from file (maybe compressed file)
    if (parsed_insert_query->infile)
    {
        /// Get name of this file (path to file)
        const auto & in_file_node = parsed_insert_query->infile->as<ASTLiteral &>();
        const auto in_file = in_file_node.value.safeGet<std::string>();

        std::string compression_method;
        /// Compression method can be specified in query
        if (parsed_insert_query->compression)
        {
            const auto & compression_method_node = parsed_insert_query->compression->as<ASTLiteral &>();
            compression_method = compression_method_node.value.safeGet<std::string>();
        }

        /// Otherwise, it will be detected from file name automatically (by chooseCompressionMethod)
        /// Buffer for reading from file is created and wrapped with appropriate compression method
        auto in_buffer = wrapReadBufferWithCompressionMethod(std::make_unique<ReadBufferFromFile>(in_file), chooseCompressionMethod(in_file, compression_method));

        try
        {
            sendDataFrom(*in_buffer, sample, columns_description, parsed_query);
        }
        catch (Exception & e)
        {
            e.addMessage("data for INSERT was parsed from file");
            throw;
        }
    }
    else if (parsed_insert_query->data)
    {
        /// Send data contained in the query.
        ReadBufferFromMemory data_in(parsed_insert_query->data, parsed_insert_query->end - parsed_insert_query->data);
        try
        {
            sendDataFrom(data_in, sample, columns_description, parsed_query);
        }
        catch (Exception & e)
        {
            /// The following query will use data from input
            //      "INSERT INTO data FORMAT TSV\n " < data.csv
            //  And may be pretty hard to debug, so add information about data source to make it easier.
            e.addMessage("data for INSERT was parsed from query");
            throw;
        }
        // Remember where the data ended. We use this info later to determine
        // where the next query begins.
        parsed_insert_query->end = parsed_insert_query->data + data_in.count();
    }
    else if (!is_interactive)
    {
        if (need_render_progress)
        {
            /// Add callback to track reading from fd.
            std_in.setProgressCallback(global_context);
        }

        /// Send data read from stdin.
        try
        {
            sendDataFrom(std_in, sample, columns_description, parsed_query);
        }
        catch (Exception & e)
        {
            e.addMessage("data for INSERT was parsed from stdin");
            throw;
        }
    }
    else
        throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);
}


void ClientBase::sendDataFrom(ReadBuffer & buf, Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query)
{
    String current_format = insert_format;

    /// Data format can be specified in the INSERT query.
    if (const auto * insert = parsed_query->as<ASTInsertQuery>())
    {
        if (!insert->format.empty())
            current_format = insert->format;
    }

    auto source = global_context->getInputFormat(current_format, buf, sample, insert_format_max_block_size);
    Pipe pipe(source);

    if (columns_description.hasDefaults())
    {
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns_description, *source, global_context);
        });
    }

    QueryPipeline pipeline(std::move(pipe));
    PullingAsyncPipelineExecutor executor(pipeline);

    Block block;
    while (executor.pull(block))
    {
        /// Check if server send Log packet
        receiveLogs(parsed_query);

        /// Check if server send Exception packet
        auto packet_type = connection->checkPacket(0);
        if (packet_type && *packet_type == Protocol::Server::Exception)
        {
            /**
             * We're exiting with error, so it makes sense to kill the
             * input stream without waiting for it to complete.
             */
            executor.cancel();
            return;
        }

        if (block)
        {
            connection->sendData(block, /* name */"", /* scalar */false);
            processed_rows += block.rows();
        }
    }

    connection->sendData({}, "", false);
}


/// Process Log packets, used when inserting data by blocks
void ClientBase::receiveLogs(ASTPtr parsed_query)
{
    auto packet_type = connection->checkPacket(0);

    while (packet_type && *packet_type == Protocol::Server::Log)
    {
        receiveAndProcessPacket(parsed_query, false);
        packet_type = connection->checkPacket(0);
    }
}


/// Process Log packets, exit when receive Exception or EndOfStream
bool ClientBase::receiveEndOfQuery()
{
    while (true)
    {
        Packet packet = connection->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::EndOfStream:
                onEndOfStream();
                return true;

            case Protocol::Server::Exception:
                onReceiveExceptionFromServer(std::move(packet.exception));
                return false;

            case Protocol::Server::Log:
                onLogData(packet.block);
                break;

            case Protocol::Server::Progress:
                onProgress(packet.progress);
                return true;

            default:
                throw NetException(
                    "Unexpected packet from server (expected Exception, EndOfStream or Log, got "
                        + String(Protocol::Server::toString(packet.type)) + ")",
                    ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
        }
    }
}


void ClientBase::processParsedSingleQuery(const String & full_query, const String & query_to_execute,
        ASTPtr parsed_query, std::optional<bool> echo_query_, bool report_error)
{
    resetOutput();
    have_error = false;
    client_exception.reset();
    server_exception.reset();

    if (echo_query_ && *echo_query_)
    {
        writeString(full_query, std_out);
        writeChar('\n', std_out);
        std_out.next();
    }

    if (is_interactive)
    {
        global_context->setCurrentQueryId("");
        // Generate a new query_id
        for (const auto & query_id_format : query_id_formats)
        {
            writeString(query_id_format.first, std_out);
            writeString(fmt::format(query_id_format.second, fmt::arg("query_id", global_context->getCurrentQueryId())), std_out);
            writeChar('\n', std_out);
            std_out.next();
        }
    }

    processed_rows = 0;
    written_first_block = false;
    progress_indication.resetProgress();
    profile_events.watch.restart();

    {
        /// Temporarily apply query settings to context.
        std::optional<Settings> old_settings;
        SCOPE_EXIT_SAFE({
            if (old_settings)
                global_context->setSettings(*old_settings);
        });

        auto apply_query_settings = [&](const IAST & settings_ast)
        {
            if (!old_settings)
                old_settings.emplace(global_context->getSettingsRef());
            global_context->applySettingsChanges(settings_ast.as<ASTSetQuery>()->changes);
        };

        const auto * insert = parsed_query->as<ASTInsertQuery>();
        if (insert && insert->settings_ast)
            apply_query_settings(*insert->settings_ast);

        /// FIXME: try to prettify this cast using `as<>()`
        const auto * with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get());
        if (with_output && with_output->settings_ast)
            apply_query_settings(*with_output->settings_ast);

        if (!connection->checkConnected())
            connect();

        ASTPtr input_function;
        if (insert && insert->select)
            insert->tryFindInputFunction(input_function);

        /// INSERT query for which data transfer is needed (not an INSERT SELECT or input()) is processed separately.
        if (insert && (!insert->select || input_function) && !insert->watch)
        {
            if (input_function && insert->format.empty())
                throw Exception("FORMAT must be specified for function input()", ErrorCodes::INVALID_USAGE_OF_INPUT);

            processInsertQuery(query_to_execute, parsed_query);
        }
        else
            processOrdinaryQuery(query_to_execute, parsed_query);
    }

    /// Do not change context (current DB, settings) in case of an exception.
    if (!have_error)
    {
        if (const auto * set_query = parsed_query->as<ASTSetQuery>())
        {
            /// Save all changes in settings to avoid losing them if the connection is lost.
            for (const auto & change : set_query->changes)
            {
                if (change.name == "profile")
                    current_profile = change.value.safeGet<String>();
                else
                    global_context->applySettingChange(change);
            }
        }
        if (const auto * use_query = parsed_query->as<ASTUseQuery>())
        {
            const String & new_database = use_query->database;
            /// If the client initiates the reconnection, it takes the settings from the config.
            config().setString("database", new_database);
            /// If the connection initiates the reconnection, it uses its variable.
            connection->setDefaultDatabase(new_database);
        }
    }

    /// Always print last block (if it was not printed already)
    if (profile_events.last_block)
    {
        initLogsOutputStream();
        progress_indication.clearProgressOutput();
        logs_out_stream->writeProfileEvents(profile_events.last_block);
        logs_out_stream->flush();
    }

    if (is_interactive)
    {
        std::cout << std::endl << processed_rows << " rows in set. Elapsed: " << progress_indication.elapsedSeconds() << " sec. ";
        progress_indication.writeFinalProgress();
        std::cout << std::endl << std::endl;
    }
    else if (print_time_to_stderr)
    {
        std::cerr << progress_indication.elapsedSeconds() << "\n";
    }

    if (have_error && report_error)
        processError(full_query);
}


MultiQueryProcessingStage ClientBase::analyzeMultiQueryText(
    const char *& this_query_begin, const char *& this_query_end, const char * all_queries_end,
    String & query_to_execute, ASTPtr & parsed_query, const String & all_queries_text,
    std::optional<Exception> & current_exception)
{
    if (this_query_begin >= all_queries_end)
        return MultiQueryProcessingStage::QUERIES_END;

    // Remove leading empty newlines and other whitespace, because they
    // are annoying to filter in query log. This is mostly relevant for
    // the tests.
    while (this_query_begin < all_queries_end && isWhitespaceASCII(*this_query_begin))
        ++this_query_begin;

    if (this_query_begin >= all_queries_end)
        return MultiQueryProcessingStage::QUERIES_END;

    // If there are only comments left until the end of file, we just
    // stop. The parser can't handle this situation because it always
    // expects that there is some query that it can parse.
    // We can get into this situation because the parser also doesn't
    // skip the trailing comments after parsing a query. This is because
    // they may as well be the leading comments for the next query,
    // and it makes more sense to treat them as such.
    {
        Tokens tokens(this_query_begin, all_queries_end);
        IParser::Pos token_iterator(tokens, global_context->getSettingsRef().max_parser_depth);
        if (!token_iterator.isValid())
            return MultiQueryProcessingStage::QUERIES_END;
    }

    this_query_end = this_query_begin;
    try
    {
        parsed_query = parseQuery(this_query_end, all_queries_end, true);
    }
    catch (Exception & e)
    {
        current_exception.emplace(e);
        return MultiQueryProcessingStage::PARSING_EXCEPTION;
    }

    if (!parsed_query)
    {
        if (ignore_error)
        {
            Tokens tokens(this_query_begin, all_queries_end);
            IParser::Pos token_iterator(tokens, global_context->getSettingsRef().max_parser_depth);
            while (token_iterator->type != TokenType::Semicolon && token_iterator.isValid())
                ++token_iterator;
            this_query_begin = token_iterator->end;

            return MultiQueryProcessingStage::CONTINUE_PARSING;
        }

        return MultiQueryProcessingStage::PARSING_FAILED;
    }

    // INSERT queries may have the inserted data in the query text
    // that follow the query itself, e.g. "insert into t format CSV 1;2".
    // They need special handling. First of all, here we find where the
    // inserted data ends. In multy-query mode, it is delimited by a
    // newline.
    // The VALUES format needs even more handling -- we also allow the
    // data to be delimited by semicolon. This case is handled later by
    // the format parser itself.
    // We can't do multiline INSERTs with inline data, because most
    // row input formats (e.g. TSV) can't tell when the input stops,
    // unlike VALUES.
    auto * insert_ast = parsed_query->as<ASTInsertQuery>();
    if (insert_ast && insert_ast->data)
    {
        this_query_end = find_first_symbols<'\n'>(insert_ast->data, all_queries_end);
        insert_ast->end = this_query_end;
        query_to_execute = all_queries_text.substr(this_query_begin - all_queries_text.data(), insert_ast->data - this_query_begin);
    }
    else
    {
        query_to_execute = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);
    }

    // Try to include the trailing comment with test hints. It is just
    // a guess for now, because we don't yet know where the query ends
    // if it is an INSERT query with inline data. We will do it again
    // after we have processed the query. But even this guess is
    // beneficial so that we see proper trailing comments in "echo" and
    // server log.
    adjustQueryEnd(this_query_end, all_queries_end, global_context->getSettingsRef().max_parser_depth);
    return MultiQueryProcessingStage::EXECUTE_QUERY;
}


bool ClientBase::processQueryText(const String & text)
{
    if (exit_strings.end() != exit_strings.find(trim(text, [](char c) { return isWhitespaceASCII(c) || c == ';'; })))
        return false;

    if (!is_multiquery)
    {
        assert(!query_fuzzer_runs);
        processTextAsSingleQuery(text);

        return true;
    }

    if (query_fuzzer_runs)
    {
        processWithFuzzing(text);
        return true;
    }

    return executeMultiQuery(text);
}


void ClientBase::runInteractive()
{
    if (config().has("query_id"))
        throw Exception("query_id could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);
    if (print_time_to_stderr)
        throw Exception("time option could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);

    /// Initialize DateLUT here to avoid counting time spent here as query execution time.
    const auto local_tz = DateLUT::instance().getTimeZone();

    std::optional<Suggest> suggest;
    suggest.emplace();
    if (load_suggestions)
    {
        /// Load suggestion data from the server.
        if (global_context->getApplicationType() == Context::ApplicationType::CLIENT)
            suggest->load<Connection>(global_context, connection_parameters, config().getInt("suggestion_limit"));
        else if (global_context->getApplicationType() == Context::ApplicationType::LOCAL)
            suggest->load<LocalConnection>(global_context, connection_parameters, config().getInt("suggestion_limit"));
    }

    if (home_path.empty())
    {
        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;
    }

    /// Initialize query_id_formats if any
    if (config().has("query_id_formats"))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config().keys("query_id_formats", keys);
        for (const auto & name : keys)
            query_id_formats.emplace_back(name + ":", config().getString("query_id_formats." + name));
    }

    if (query_id_formats.empty())
        query_id_formats.emplace_back("Query id:", " {query_id}\n");

    /// Load command history if present.
    if (config().has("history_file"))
        history_file = config().getString("history_file");
    else
    {
        auto * history_file_from_env = getenv("CLICKHOUSE_HISTORY_FILE");
        if (history_file_from_env)
            history_file = history_file_from_env;
        else if (!home_path.empty())
            history_file = home_path + "/.clickhouse-client-history";
    }

    if (!history_file.empty() && !fs::exists(history_file))
    {
        /// Avoid TOCTOU issue.
        try
        {
            FS::createFile(history_file);
        }
        catch (const ErrnoException & e)
        {
            if (e.getErrno() != EEXIST)
                throw;
        }
    }

    LineReader::Patterns query_extenders = {"\\"};
    LineReader::Patterns query_delimiters = {";", "\\G"};

#if USE_REPLXX
    replxx::Replxx::highlighter_callback_t highlight_callback{};
    if (config().getBool("highlight", true))
        highlight_callback = highlight;

    ReplxxLineReader lr(*suggest, history_file, config().has("multiline"), query_extenders, query_delimiters, highlight_callback);

#elif defined(USE_READLINE) && USE_READLINE
    ReadlineLineReader lr(*suggest, history_file, config().has("multiline"), query_extenders, query_delimiters);
#else
    LineReader lr(history_file, config().has("multiline"), query_extenders, query_delimiters);
#endif

    /// Enable bracketed-paste-mode only when multiquery is enabled and multiline is
    ///  disabled, so that we are able to paste and execute multiline queries in a whole
    ///  instead of erroring out, while be less intrusive.
    if (config().has("multiquery") && !config().has("multiline"))
        lr.enableBracketedPaste();

    do
    {
        auto input = lr.readLine(prompt(), ":-] ");
        if (input.empty())
            break;

        has_vertical_output_suffix = false;
        if (input.ends_with("\\G"))
        {
            input.resize(input.size() - 2);
            has_vertical_output_suffix = true;
        }

        try
        {
            if (!processQueryText(input))
                break;
        }
        catch (const Exception & e)
        {
            /// We don't need to handle the test hints in the interactive mode.
            std::cerr << "Exception on client:" << std::endl << getExceptionMessage(e, print_stack_trace, true) << std::endl << std::endl;
            client_exception = std::make_unique<Exception>(e);
        }

        if (client_exception)
        {
            /// client_exception may have been set above or elsewhere.
            /// Client-side exception during query execution can result in the loss of
            /// sync in the connection protocol.
            /// So we reconnect and allow to enter the next query.
            if (!connection->checkConnected())
                connect();
        }
    }
    while (true);

    if (isNewYearMode())
        std::cout << "Happy new year." << std::endl;
    else if (isChineseNewYearMode(local_tz))
        std::cout << "Happy Chinese new year. 春节快乐!" << std::endl;
    else
        std::cout << "Bye." << std::endl;
}


void ClientBase::runNonInteractive()
{
    if (!queries_files.empty())
    {
        auto process_multi_query_from_file = [&](const String & file)
        {
            auto text = getQueryTextPrefix();
            String queries_from_file;

            ReadBufferFromFile in(file);
            readStringUntilEOF(queries_from_file, in);

            text += queries_from_file;
            return executeMultiQuery(text);
        };

        /// Read all queries into `text`.
        for (const auto & queries_file : queries_files)
        {
            for (const auto & interleave_file : interleave_queries_files)
                if (!process_multi_query_from_file(interleave_file))
                    return;

            if (!process_multi_query_from_file(queries_file))
                return;
        }

        return;
    }

    String text;
    if (is_multiquery)
        text = getQueryTextPrefix();

    if (config().has("query"))
    {
        text += config().getRawString("query"); /// Poco configuration should not process substitutions in form of ${...} inside query.
    }
    else
    {
        /// If 'query' parameter is not set, read a query from stdin.
        /// The query is read entirely into memory (streaming is disabled).
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        readStringUntilEOF(text, in);
    }

    if (query_fuzzer_runs)
        processWithFuzzing(text);
    else
        processQueryText(text);
}


void ClientBase::clearTerminal()
{
    /// Clear from cursor until end of screen.
    /// It is needed if garbage is left in terminal.
    /// Show cursor. It can be left hidden by invocation of previous programs.
    /// A test for this feature: perl -e 'print "x"x100000'; echo -ne '\033[0;0H\033[?25l'; clickhouse-client
    std::cout << "\033[0J" "\033[?25h";
}


void ClientBase::showClientVersion()
{
    std::cout << DBMS_NAME << " " + getName() + " version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
}


void ClientBase::readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> & external_tables_arguments)
{
    /** We allow different groups of arguments:
        * - common arguments;
        * - arguments for any number of external tables each in form "--external args...",
        *   where possible args are file, name, format, structure, types;
        * - param arguments for prepared statements.
        * Split these groups before processing.
        */

    bool in_external_group = false;
    for (int arg_num = 1; arg_num < argc; ++arg_num)
    {
        const char * arg = argv[arg_num];

        if (arg == "--external"sv)
        {
            in_external_group = true;
            external_tables_arguments.emplace_back(Arguments{""});
        }
        /// Options with value after equal sign.
        else if (in_external_group
            && (0 == strncmp(arg, "--file=", strlen("--file=")) || 0 == strncmp(arg, "--name=", strlen("--name="))
                || 0 == strncmp(arg, "--format=", strlen("--format=")) || 0 == strncmp(arg, "--structure=", strlen("--structure="))
                || 0 == strncmp(arg, "--types=", strlen("--types="))))
        {
            external_tables_arguments.back().emplace_back(arg);
        }
        /// Options with value after whitespace.
        else if (in_external_group
            && (arg == "--file"sv || arg == "--name"sv || arg == "--format"sv
                || arg == "--structure"sv || arg == "--types"sv))
        {
            if (arg_num + 1 < argc)
            {
                external_tables_arguments.back().emplace_back(arg);
                ++arg_num;
                arg = argv[arg_num];
                external_tables_arguments.back().emplace_back(arg);
            }
            else
                break;
        }
        else
        {
            in_external_group = false;

            /// Parameter arg after underline.
            if (startsWith(arg, "--param_"))
            {
                const char * param_continuation = arg + strlen("--param_");
                const char * equal_pos = strchr(param_continuation, '=');

                if (equal_pos == param_continuation)
                    throw Exception("Parameter name cannot be empty", ErrorCodes::BAD_ARGUMENTS);

                if (equal_pos)
                {
                    /// param_name=value
                    query_parameters.emplace(String(param_continuation, equal_pos), String(equal_pos + 1));
                }
                else
                {
                    /// param_name value
                    ++arg_num;
                    if (arg_num >= argc)
                        throw Exception("Parameter requires value", ErrorCodes::BAD_ARGUMENTS);
                    arg = argv[arg_num];
                    query_parameters.emplace(String(param_continuation), String(arg));
                }
            }
            else
                common_arguments.emplace_back(arg);
        }
    }
}

void ClientBase::parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments)
{
    cmd_settings.addProgramOptions(options_description.main_description.value());
    /// Parse main commandline options.
    auto parser = po::command_line_parser(arguments).options(options_description.main_description.value()).allow_unregistered();
    po::parsed_options parsed = parser.run();

    /// Check unrecognized options without positional options.
    auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::exclude_positional);
    if (!unrecognized_options.empty())
        throw Exception(ErrorCodes::UNRECOGNIZED_ARGUMENTS, "Unrecognized option '{}'", unrecognized_options[0]);

    /// Check positional options (options after ' -- ', ex: clickhouse-client -- <options>).
    unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::include_positional);
    if (unrecognized_options.size() > 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Positional options are not supported.");

    po::store(parsed, options);
}


void ClientBase::init(int argc, char ** argv)
{
    namespace po = boost::program_options;

    /// Don't parse options with Poco library, we prefer neat boost::program_options.
    stopOptionsProcessing();

    stdin_is_a_tty = isatty(STDIN_FILENO);
    stdout_is_a_tty = isatty(STDOUT_FILENO);
    terminal_width = getTerminalWidth();

    Arguments common_arguments{""}; /// 0th argument is ignored.
    std::vector<Arguments> external_tables_arguments;

    readArguments(argc, argv, common_arguments, external_tables_arguments);

    po::variables_map options;
    OptionsDescription options_description;
    options_description.main_description.emplace(createOptionsDescription("Main options", terminal_width));

    /// Common options for clickhouse-client and clickhouse-local.
    options_description.main_description->add_options()
        ("help", "produce help message")
        ("version,V", "print version information and exit")
        ("version-clean", "print version in machine-readable format and exit")

        ("config-file,C", po::value<std::string>(), "config-file path")
        ("queries-file", po::value<std::vector<std::string>>()->multitoken(),
            "file path with queries to execute; multiple files can be specified (--queries-file file1 file2...)")
        ("database,d", po::value<std::string>(), "database")
        ("history_file", po::value<std::string>(), "path to history file")

        ("query,q", po::value<std::string>(), "query")
        ("stage", po::value<std::string>()->default_value("complete"), "Request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
        ("query_id", po::value<std::string>(), "query_id")
        ("progress", "print progress of queries execution")

        ("disable_suggestion,A", "Disable loading suggestion data. Note that suggestion data is loaded asynchronously through a second connection to ClickHouse server. Also it is reasonable to disable suggestion if you want to paste a query with TAB characters. Shorthand option -A is for those who get used to mysql client.")
        ("time,t", "print query execution time to stderr in non-interactive mode (for benchmarks)")

        ("echo", "in batch mode, print query before execution")
        ("verbose", "print query and other debugging info")

        ("log-level", po::value<std::string>(), "log level")
        ("server_logs_file", po::value<std::string>(), "put server logs into specified file")

        ("multiline,m", "multiline")
        ("multiquery,n", "multiquery")

        ("suggestion_limit", po::value<int>()->default_value(10000),
            "Suggestion limit for how many databases, tables and columns to fetch.")

        ("format,f", po::value<std::string>(), "default output format")
        ("vertical,E", "vertical output format, same as --format=Vertical or FORMAT Vertical or \\G at end of command")
        ("highlight", po::value<bool>()->default_value(true), "enable or disable basic syntax highlight in interactive command line")

        ("ignore-error", "do not stop processing in multiquery mode")
        ("stacktrace", "print stack traces of exceptions")
        ("hardware-utilization", "print hardware utilization information in progress bar")
        ("print-profile-events", po::value(&profile_events.print)->zero_tokens(), "Printing ProfileEvents packets")
        ("profile-events-delay-ms", po::value<UInt64>()->default_value(profile_events.delay_ms), "Delay between printing `ProfileEvents` packets (-1 - print only totals, 0 - print every single packet)")
    ;

    addOptions(options_description);
    parseAndCheckOptions(options_description, options, common_arguments);
    po::notify(options);

    if (options.count("version") || options.count("V"))
    {
        showClientVersion();
        exit(0);
    }

    if (options.count("version-clean"))
    {
        std::cout << VERSION_STRING;
        exit(0);
    }

    /// Output of help message.
    if (options.count("help")
        || (options.count("host") && options["host"].as<std::string>() == "elp")) /// If user writes -help instead of --help.
    {
        printHelpMessage(options_description);
        exit(0);
    }

    /// Common options for clickhouse-client and clickhouse-local.
    if (options.count("time"))
        print_time_to_stderr = true;
    if (options.count("query"))
        config().setString("query", options["query"].as<std::string>());
    if (options.count("query_id"))
        config().setString("query_id", options["query_id"].as<std::string>());
    if (options.count("database"))
        config().setString("database", options["database"].as<std::string>());
    if (options.count("config-file"))
        config().setString("config-file", options["config-file"].as<std::string>());
    if (options.count("queries-file"))
        queries_files = options["queries-file"].as<std::vector<std::string>>();
    if (options.count("multiline"))
        config().setBool("multiline", true);
    if (options.count("multiquery"))
        config().setBool("multiquery", true);
    if (options.count("ignore-error"))
        config().setBool("ignore-error", true);
    if (options.count("format"))
        config().setString("format", options["format"].as<std::string>());
    if (options.count("vertical"))
        config().setBool("vertical", true);
    if (options.count("stacktrace"))
        config().setBool("stacktrace", true);
    if (options.count("print-profile-events"))
        config().setBool("print-profile-events", true);
    if (options.count("profile-events-delay-ms"))
        config().setInt("profile-events-delay-ms", options["profile-events-delay-ms"].as<UInt64>());
    if (options.count("progress"))
        config().setBool("progress", true);
    if (options.count("echo"))
        config().setBool("echo", true);
    if (options.count("disable_suggestion"))
        config().setBool("disable_suggestion", true);
    if (options.count("suggestion_limit"))
        config().setInt("suggestion_limit", options["suggestion_limit"].as<int>());
    if (options.count("highlight"))
        config().setBool("highlight", options["highlight"].as<bool>());
    if (options.count("history_file"))
        config().setString("history_file", options["history_file"].as<std::string>());
    if (options.count("verbose"))
        config().setBool("verbose", true);
    if (options.count("log-level"))
        Poco::Logger::root().setLevel(options["log-level"].as<std::string>());
    if (options.count("server_logs_file"))
        server_logs_file = options["server_logs_file"].as<std::string>();
    if (options.count("hardware-utilization"))
        progress_indication.print_hardware_utilization = true;

    query_processing_stage = QueryProcessingStage::fromString(options["stage"].as<std::string>());
    profile_events.print = options.count("print-profile-events");
    profile_events.delay_ms = options["profile-events-delay-ms"].as<UInt64>();

    processOptions(options_description, options, external_tables_arguments);
    argsToConfig(common_arguments, config(), 100);
    clearPasswordFromCommandLine(argc, argv);
}

}
