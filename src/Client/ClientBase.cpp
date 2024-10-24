#include <Client/ClientBase.h>
#include <Client/ClientBaseHelpers.h>
#include <Client/InternalTextLogs.h>
#include <Client/LineReader.h>
#include <Client/TerminalKeystrokeInterceptor.h>
#include <Client/TestHint.h>
#include <Client/TestTags.h>

#include <base/safeExit.h>
#include <Core/Block.h>
#include <Core/Protocol.h>
#include <Common/DateLUT.h>
#include <Common/MemoryTracker.h>
#include <Common/scope_guard_safe.h>
#include <Common/Exception.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/typeid_cast.h>
#include <Common/TerminalSize.h>
#include <Common/StringUtils.h>
#include <Common/filesystemHelpers.h>
#include <Common/NetException.h>
#include <Common/SignalHandlers.h>
#include <Common/tryGetFileNameByFileDescriptor.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Formats/FormatFactory.h>

#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTCreateFunctionQuery.h>
#include <Parsers/Access/ASTCreateUserQuery.h>
#include <Parsers/Access/ASTAuthenticationData.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTExplainQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/PRQL/ParserPRQLQuery.h>
#include <Parsers/Kusto/ParserKQLStatement.h>
#include <Parsers/Kusto/parseKQLQuery.h>

#include <Processors/Formats/Impl/NullFormat.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/BuildQueryPipelineSettings.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/Executors/PullingAsyncPipelineExecutor.h>
#include <Processors/Transforms/AddingDefaultsTransform.h>
#include <QueryPipeline/QueryPipeline.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Interpreters/ProfileEventsExt.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/CompressionMethod.h>
#include <IO/ForkWriteBuffer.h>

#include <Access/AccessControl.h>
#include <Storages/ColumnsDescription.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <iostream>
#include <filesystem>
#include <limits>
#include <map>
#include <memory>
#include <string_view>
#include <unordered_map>

#include <Common/config_version.h>
#include <base/find_symbols.h>
#include "config.h"
#include <IO/ReadHelpers.h>
#include <Processors/Formats/Impl/ValuesBlockInputFormat.h>

#if USE_GWP_ASAN
#    include <Common/GWPAsan.h>
#endif


namespace fs = std::filesystem;
using namespace std::literals;

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_settings_after_format_in_insert;
    extern const SettingsBool async_insert;
    extern const SettingsDialect dialect;
    extern const SettingsUInt64 max_block_size;
    extern const SettingsUInt64 max_insert_block_size;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 output_format_pretty_max_rows;
    extern const SettingsUInt64 output_format_pretty_max_value_width;
    extern const SettingsBool partial_result_on_first_cancel;
    extern const SettingsBool throw_if_no_data_to_insert;
    extern const SettingsBool implicit_select;
}

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
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_ALREADY_EXISTS;
    extern const int USER_SESSION_LIMIT_EXCEEDED;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_READ_FROM_FILE_DESCRIPTOR;
    extern const int USER_EXPIRED;
}

}

namespace ProfileEvents
{
    extern const Event UserTimeMicroseconds;
    extern const Event SystemTimeMicroseconds;
}

namespace
{
constexpr UInt64 THREAD_GROUP_ID = 0;
}

namespace DB
{

ProgressOption toProgressOption(std::string progress)
{
    boost::to_upper(progress);

    if (progress == "OFF" || progress == "FALSE" || progress == "0" || progress == "NO")
        return ProgressOption::OFF;
    if (progress == "TTY" || progress == "ON" || progress == "TRUE" || progress == "1" || progress == "YES")
        return ProgressOption::TTY;
    if (progress == "ERR")
        return ProgressOption::ERR;
    if (progress == "DEFAULT")
        return ProgressOption::DEFAULT;

    throw boost::program_options::validation_error(boost::program_options::validation_error::invalid_option_value);
}

std::istream& operator>> (std::istream & in, ProgressOption & progress)
{
    std::string token;
    in >> token;
    progress = toProgressOption(token);
    return in;
}

static void incrementProfileEventsBlock(Block & dst, const Block & src)
{
    if (!dst)
    {
        dst = src.cloneEmpty();
    }

    assertBlocksHaveEqualStructure(src, dst, "ProfileEvents");

    std::unordered_map<String, size_t> name_pos;
    for (size_t i = 0; i < dst.columns(); ++i)
        name_pos[dst.getByPosition(i).name] = i;

    size_t dst_rows = dst.rows();
    MutableColumns mutable_columns = dst.mutateColumns();

    auto & dst_column_host_name = typeid_cast<ColumnString &>(*mutable_columns[name_pos["host_name"]]);
    auto & dst_array_current_time = typeid_cast<ColumnUInt32 &>(*mutable_columns[name_pos["current_time"]]).getData();
    auto & dst_array_type = typeid_cast<ColumnInt8 &>(*mutable_columns[name_pos["type"]]).getData();
    auto & dst_column_name = typeid_cast<ColumnString &>(*mutable_columns[name_pos["name"]]);
    auto & dst_array_value = typeid_cast<ColumnInt64 &>(*mutable_columns[name_pos["value"]]).getData();

    const auto & src_column_host_name = typeid_cast<const ColumnString &>(*src.getByName("host_name").column);
    const auto & src_array_current_time = typeid_cast<const ColumnUInt32 &>(*src.getByName("current_time").column).getData();
    const auto & src_array_thread_id = typeid_cast<const ColumnUInt64 &>(*src.getByName("thread_id").column).getData();
    const auto & src_column_name = typeid_cast<const ColumnString &>(*src.getByName("name").column);
    const auto & src_array_value = typeid_cast<const ColumnInt64 &>(*src.getByName("value").column).getData();

    struct Id
    {
        StringRef name;
        StringRef host_name;

        bool operator<(const Id & rhs) const
        {
            return std::tie(name, host_name)
                 < std::tie(rhs.name, rhs.host_name);
        }
    };
    std::map<Id, UInt64> rows_by_name;

    for (size_t src_row = 0; src_row < src.rows(); ++src_row)
    {
        /// Filter out threads stats, use stats from thread group
        /// Exactly stats from thread group is stored to the table system.query_log
        /// The stats from threads are less useful.
        /// They take more records, they need to be combined,
        /// there even could be several records from one thread.
        /// Server doesn't send it any more to the clients, so this code left for compatible
        auto thread_id = src_array_thread_id[src_row];
        if (thread_id != THREAD_GROUP_ID)
            continue;

        Id id{
            src_column_name.getDataAt(src_row),
            src_column_host_name.getDataAt(src_row),
        };
        rows_by_name[id] = src_row;
    }

    /// Merge src into dst.
    for (size_t dst_row = 0; dst_row < dst_rows; ++dst_row)
    {
        Id id{
            dst_column_name.getDataAt(dst_row),
            dst_column_host_name.getDataAt(dst_row),
        };

        if (auto it = rows_by_name.find(id); it != rows_by_name.end())
        {
            size_t src_row = it->second;

            dst_array_current_time[dst_row] = src_array_current_time[src_row];

            switch (static_cast<ProfileEvents::Type>(dst_array_type[dst_row]))
            {
                case ProfileEvents::Type::INCREMENT:
                    dst_array_value[dst_row] += src_array_value[src_row];
                    break;
                case ProfileEvents::Type::GAUGE:
                    dst_array_value[dst_row] = src_array_value[src_row];
                    break;
            }

            rows_by_name.erase(it);
        }
    }

    /// Copy rows from src that dst does not contains.
    for (const auto & [id, pos] : rows_by_name)
    {
        for (size_t col = 0; col < src.columns(); ++col)
        {
            mutable_columns[col]->insert((*src.getByPosition(col).column)[pos]);
        }
    }

    dst.setColumns(std::move(mutable_columns));
}

/// To cancel the query on local format error.
class LocalFormatError : public DB::Exception
{
public:
    using Exception::Exception;
};


ClientBase::~ClientBase() = default;

ClientBase::ClientBase(
    int in_fd_,
    int out_fd_,
    int err_fd_,
    std::istream & input_stream_,
    std::ostream & output_stream_,
    std::ostream & error_stream_
)
    : std_in(in_fd_)
    , std_out(out_fd_)
    , progress_indication(output_stream_, in_fd_, err_fd_)
    , progress_table(output_stream_, in_fd_, err_fd_)
    , in_fd(in_fd_)
    , out_fd(out_fd_)
    , err_fd(err_fd_)
    , input_stream(input_stream_)
    , output_stream(output_stream_)
    , error_stream(error_stream_)
{
    stdin_is_a_tty = isatty(in_fd);
    stdout_is_a_tty = isatty(out_fd);
    stderr_is_a_tty = isatty(err_fd);
    terminal_width = getTerminalWidth(in_fd, err_fd);
}

ASTPtr ClientBase::parseQuery(const char *& pos, const char * end, const Settings & settings, bool allow_multi_statements)
{
    std::unique_ptr<IParserBase> parser;
    ASTPtr res;

    size_t max_length = 0;

    if (!allow_multi_statements)
        max_length = settings[Setting::max_query_size];

    const Dialect dialect = settings[Setting::dialect];

    if (dialect == Dialect::kusto)
        parser = std::make_unique<ParserKQLStatement>(end, settings[Setting::allow_settings_after_format_in_insert]);
    else if (dialect == Dialect::prql)
        parser = std::make_unique<ParserPRQLQuery>(max_length, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    else
        parser = std::make_unique<ParserQuery>(end, settings[Setting::allow_settings_after_format_in_insert], settings[Setting::implicit_select]);

    if (is_interactive || ignore_error)
    {
        String message;
        if (dialect == Dialect::kusto)
            res = tryParseKQLQuery(*parser, pos, end, message, true, "", allow_multi_statements, max_length, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks], true);
        else
            res = tryParseQuery(*parser, pos, end, message, true, "", allow_multi_statements, max_length, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks], true);

        if (!res)
        {
            error_stream << std::endl << message << std::endl << std::endl;
            return nullptr;
        }
    }
    else
    {
        if (dialect == Dialect::kusto)
            res = parseKQLQueryAndMovePosition(*parser, pos, end, "", allow_multi_statements, max_length, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
        else
            res = parseQueryAndMovePosition(*parser, pos, end, "", allow_multi_statements, max_length, settings[Setting::max_parser_depth], settings[Setting::max_parser_backtracks]);
    }

    if (is_interactive)
    {
        output_stream << std::endl;
        WriteBufferFromOStream res_buf(output_stream, 4096);
        IAST::FormatSettings format_settings(res_buf, /* one_line */ false);
        format_settings.hilite = true;
        format_settings.show_secrets = true;
        format_settings.print_pretty_type_names = true;
        res->format(format_settings);
        res_buf.finalize();
        output_stream << std::endl << std::endl;
    }

    return res;
}


/// Consumes trailing semicolons and tries to consume the same-line trailing comment.
void ClientBase::adjustQueryEnd(
    const char *& this_query_end, const char * all_queries_end, uint32_t max_parser_depth, uint32_t max_parser_backtracks)
{
    // We have to skip the trailing semicolon that might be left
    // after VALUES parsing or just after a normal semicolon-terminated query.
    Tokens after_query_tokens(this_query_end, all_queries_end);
    IParser::Pos after_query_iterator(after_query_tokens, max_parser_depth, max_parser_backtracks);
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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "External tables could be sent only with select query");

    std::vector<ExternalTableDataPtr> data;
    for (auto & table : external_tables)
        data.emplace_back(table.getData(client_context));

    connection->sendExternalTablesData(data);
}


void ClientBase::onData(Block & block, ASTPtr parsed_query)
{
    if (!block)
        return;

    processed_rows += block.rows();
    /// Even if all blocks are empty, we still need to initialize the output stream to write empty resultset.
    initOutputFormat(block, parsed_query);

    /// The header block containing zero rows was used to initialize
    /// output_format, do not output it.
    /// Also do not output too much data if we're fuzzing.
    if (block.rows() == 0 || (query_fuzzer_runs != 0 && processed_rows >= 100))
        return;

    /// If results are written INTO OUTFILE, we can avoid clearing progress to avoid flicker.
    if (need_render_progress && tty_buf && (!select_into_file || select_into_file_and_stdout))
        progress_indication.clearProgressOutput(*tty_buf);
    if (need_render_progress_table && tty_buf && (!select_into_file || select_into_file_and_stdout))
        progress_table.clearTableOutput(*tty_buf);

    try
    {
        output_format->write(materializeBlock(block));
        written_first_block = true;
    }
    catch (const Exception &)
    {
        /// Catch client errors like NO_ROW_DELIMITER
        throw LocalFormatError(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
    }

    /// Received data block is immediately displayed to the user.
    output_format->flush();

    /// Restore progress bar and progress table after data block.
    if (need_render_progress && tty_buf)
    {
        if (select_into_file && !select_into_file_and_stdout)
            error_stream << "\r";
        progress_indication.writeProgress(*tty_buf);
    }
    if (need_render_progress_table && tty_buf && !cancelled)
    {
        if (!need_render_progress && select_into_file && !select_into_file_and_stdout)
            error_stream << "\r";
        bool toggle_enabled = getClientConfiguration().getBool("enable-progress-table-toggle", true);
        progress_table.writeTable(*tty_buf, progress_table_toggle_on.load(), toggle_enabled);
    }
}


void ClientBase::onLogData(Block & block)
{
    initLogsOutputStream();
    if (need_render_progress && tty_buf)
        progress_indication.clearProgressOutput(*tty_buf);
    if (need_render_progress_table && tty_buf)
        progress_table.clearTableOutput(*tty_buf);
    logs_out_stream->writeLogs(block);
    logs_out_stream->flush();
}


void ClientBase::onTotals(Block & block, ASTPtr parsed_query)
{
    initOutputFormat(block, parsed_query);
    output_format->setTotals(materializeBlock(block));
}


void ClientBase::onExtremes(Block & block, ASTPtr parsed_query)
{
    initOutputFormat(block, parsed_query);
    output_format->setExtremes(materializeBlock(block));
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
    if (profile_info.hasAppliedAggregation() && output_format)
        output_format->setRowsBeforeAggregation(profile_info.getRowsBeforeAggregation());
}


void ClientBase::initOutputFormat(const Block & block, ASTPtr parsed_query)
try
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
        if (!pager.empty())
        {
            if (SIG_ERR == signal(SIGPIPE, SIG_IGN))
                throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler for SIGPIPE");
            /// We need to reset signals that had been installed in the
            /// setupSignalHandler() since terminal will send signals to both
            /// processes and so signals will be delivered to the
            /// clickhouse-client/local as well, which will be terminated when
            /// signal will be delivered second time.
            if (SIG_ERR == signal(SIGINT, SIG_IGN))
                throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler for SIGINT");
            if (SIG_ERR == signal(SIGQUIT, SIG_IGN))
                throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler for SIGQUIT");

            ShellCommand::Config config(pager);
            config.pipe_stdin_only = true;
            pager_cmd = ShellCommand::execute(config);
            out_buf = &pager_cmd->in;
        }
        else
        {
            out_buf = &std_out;
        }

        String current_format = default_output_format;

        select_into_file = false;
        select_into_file_and_stdout = false;
        /// The query can specify output format or output file.
        if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get()))
        {
            String out_file;
            if (query_with_output->out_file)
            {
                select_into_file = true;

                const auto & out_file_node = query_with_output->out_file->as<ASTLiteral &>();
                out_file = out_file_node.value.safeGet<std::string>();

                std::string compression_method_string;

                if (query_with_output->compression)
                {
                    const auto & compression_method_node = query_with_output->compression->as<ASTLiteral &>();
                    compression_method_string = compression_method_node.value.safeGet<std::string>();
                }

                CompressionMethod compression_method = chooseCompressionMethod(out_file, compression_method_string);
                UInt64 compression_level = 3;

                if (query_with_output->compression_level)
                {
                    const auto & compression_level_node = query_with_output->compression_level->as<ASTLiteral &>();
                    compression_level_node.value.tryGet<UInt64>(compression_level);
                }

                auto flags = O_WRONLY | O_EXCL;

                auto file_exists = fs::exists(out_file);
                if (file_exists && query_with_output->is_outfile_append)
                    flags |= O_APPEND;
                else if (file_exists && query_with_output->is_outfile_truncate)
                    flags |= O_TRUNC;
                else
                    flags |= O_CREAT;

                out_file_buf = wrapWriteBufferWithCompressionMethod(
                    std::make_unique<WriteBufferFromFile>(out_file, DBMS_DEFAULT_BUFFER_SIZE, flags),
                    compression_method,
                    static_cast<int>(compression_level)
                );

                if (query_with_output->is_into_outfile_with_stdout)
                {
                    select_into_file_and_stdout = true;
                    out_file_buf = std::make_unique<ForkWriteBuffer>(std::vector<WriteBufferPtr>{std::move(out_file_buf),
                            std::make_shared<WriteBufferFromFileDescriptor>(STDOUT_FILENO)});
                }

                // We are writing to file, so default format is the same as in non-interactive mode.
                if (is_interactive && is_default_format)
                    current_format = "TabSeparated";
            }
            if (query_with_output->format != nullptr)
            {
                if (has_vertical_output_suffix)
                    throw Exception(ErrorCodes::CLIENT_OUTPUT_FORMAT_SPECIFIED, "Output format already specified");
                const auto & id = query_with_output->format->as<ASTIdentifier &>();
                current_format = id.name();
            }
            else if (query_with_output->out_file)
            {
                auto format_name = FormatFactory::instance().tryGetFormatFromFileName(out_file);
                if (format_name)
                    current_format = *format_name;
            }
        }

        if (has_vertical_output_suffix)
            current_format = "Vertical";

        bool logs_into_stdout = server_logs_file == "-";
        bool extras_into_stdout = need_render_progress || logs_into_stdout;
        bool select_only_into_file = select_into_file && !select_into_file_and_stdout;

        if (!out_file_buf && default_output_compression_method != CompressionMethod::None)
            out_file_buf = wrapWriteBufferWithCompressionMethod(out_buf, default_output_compression_method, 3, 0);

        /// It is not clear how to write progress and logs
        /// intermixed with data with parallel formatting.
        /// It may increase code complexity significantly.
        if (!extras_into_stdout || select_only_into_file)
            output_format = client_context->getOutputFormatParallelIfPossible(
                current_format, out_file_buf ? *out_file_buf : *out_buf, block);
        else
            output_format = client_context->getOutputFormat(
                current_format, out_file_buf ? *out_file_buf : *out_buf, block);

        output_format->setAutoFlush();
    }
}
catch (...)
{
    throw LocalFormatError(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
}


void ClientBase::initLogsOutputStream()
{
    if (!logs_out_stream)
    {
        WriteBuffer * wb = out_logs_buf.get();

        bool color_logs = false;
        if (!out_logs_buf)
        {
            if (server_logs_file.empty())
            {
                /// Use stderr by default
                out_logs_buf = std::make_unique<WriteBufferFromFileDescriptor>(STDERR_FILENO);
                wb = out_logs_buf.get();
                color_logs = stderr_is_a_tty;
            }
            else if (server_logs_file == "-")
            {
                /// Use stdout if --server_logs_file=- specified
                wb = &std_out;
                color_logs = stdout_is_a_tty;
            }
            else
            {
                out_logs_buf
                    = std::make_unique<WriteBufferFromFile>(server_logs_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
                wb = out_logs_buf.get();
            }
        }

        logs_out_stream = std::make_unique<InternalTextLogs>(*wb, color_logs);
    }
}

void ClientBase::adjustSettings()
{
    Settings settings = global_context->getSettingsCopy();

    /// NOTE: Do not forget to set changed=false to avoid sending it to the server (to avoid breakage read only profiles)

    /// Do not limit pretty format output in case of --pager specified or in case of stdout is not a tty.
    if (!pager.empty() || !stdout_is_a_tty)
    {
        if (!global_context->getSettingsRef()[Setting::output_format_pretty_max_rows].changed)
        {
            settings[Setting::output_format_pretty_max_rows] = std::numeric_limits<UInt64>::max();
            settings[Setting::output_format_pretty_max_rows].changed = false;
        }

        if (!global_context->getSettingsRef()[Setting::output_format_pretty_max_value_width].changed)
        {
            settings[Setting::output_format_pretty_max_value_width] = std::numeric_limits<UInt64>::max();
            settings[Setting::output_format_pretty_max_value_width].changed = false;
        }
    }

    global_context->setSettings(settings);
}

void ClientBase::initClientContext()
{
    client_context->setClientName(std::string(DEFAULT_CLIENT_NAME));
    client_context->setQuotaClientKey(getClientConfiguration().getString("quota_key", ""));
    client_context->setQueryKindInitial();
    client_context->setQueryKind(query_kind);
    client_context->setQueryParameters(query_parameters);
}

bool ClientBase::isRegularFile(int fd)
{
    struct stat file_stat;
    return fstat(fd, &file_stat) == 0 && S_ISREG(file_stat.st_mode);
}

void ClientBase::setDefaultFormatsAndCompressionFromConfiguration()
{
    if (getClientConfiguration().has("output-format"))
    {
        default_output_format = getClientConfiguration().getString("output-format");
        is_default_format = false;
    }
    else if (getClientConfiguration().has("format"))
    {
        default_output_format = getClientConfiguration().getString("format");
        is_default_format = false;
    }
    else if (getClientConfiguration().has("vertical"))
    {
        default_output_format = "Vertical";
        is_default_format = false;
    }
    else if (isRegularFile(STDOUT_FILENO))
    {
        std::optional<String> format_from_file_name = FormatFactory::instance().tryGetFormatFromFileDescriptor(STDOUT_FILENO);
        if (format_from_file_name)
            default_output_format = *format_from_file_name;
        else
            default_output_format = "TSV";

        std::optional<String> file_name = tryGetFileNameFromFileDescriptor(STDOUT_FILENO);
        if (file_name)
            default_output_compression_method = chooseCompressionMethod(*file_name, "");
    }
    else if (is_interactive)
    {
        default_output_format = "PrettyCompact";
    }
    else
    {
        default_output_format = "TSV";
    }

    if (getClientConfiguration().has("input-format"))
    {
        default_input_format = getClientConfiguration().getString("input-format");
    }
    else if (getClientConfiguration().has("format"))
    {
        default_input_format = getClientConfiguration().getString("format");
    }
    else if (getClientConfiguration().getString("table-file", "-") != "-")
    {
        auto file_name = getClientConfiguration().getString("table-file");
        std::optional<String> format_from_file_name = FormatFactory::instance().tryGetFormatFromFileName(file_name);
        if (format_from_file_name)
            default_input_format = *format_from_file_name;
        else
            default_input_format = "TSV";
    }
    else
    {
        std::optional<String> format_from_file_name = FormatFactory::instance().tryGetFormatFromFileDescriptor(STDIN_FILENO);
        if (format_from_file_name)
            default_input_format = *format_from_file_name;
        else
            default_input_format = "TSV";
    }

    format_max_block_size = getClientConfiguration().getUInt64("format_max_block_size", global_context->getSettingsRef()[Setting::max_block_size]);

    /// Setting value from cmd arg overrides one from config
    if (global_context->getSettingsRef()[Setting::max_insert_block_size].changed)
    {
        insert_format_max_block_size = global_context->getSettingsRef()[Setting::max_insert_block_size];
    }
    else
    {
        insert_format_max_block_size
            = getClientConfiguration().getUInt64("insert_format_max_block_size", global_context->getSettingsRef()[Setting::max_insert_block_size]);
    }
}

void ClientBase::initTTYBuffer(ProgressOption progress_option, ProgressOption progress_table_option)
{
    if (tty_buf)
        return;

    if (progress_option == ProgressOption::OFF || (!is_interactive && progress_option == ProgressOption::DEFAULT))
        need_render_progress = false;

    if (progress_table_option == ProgressOption::OFF || (!is_interactive && progress_table_option == ProgressOption::DEFAULT))
        need_render_progress_table = false;

    if (!need_render_progress && !need_render_progress_table)
        return;

    /// If need_render_progress and need_render_progress_table are enabled,
    /// use ProgressOption that was set for the progress bar for progress table as well.
    ProgressOption progress = progress_option ? progress_option : progress_table_option;

    static constexpr auto tty_file_name = "/dev/tty";

    /// Output all progress bar commands to terminal at once to avoid flicker.
    /// This size is usually greater than the window size.
    static constexpr size_t buf_size = 1024;

    if (is_interactive || progress == ProgressOption::TTY)
    {
        std::error_code ec;
        std::filesystem::file_status tty = std::filesystem::status(tty_file_name, ec);

        if (!ec && exists(tty) && is_character_file(tty)
            && (tty.permissions() & std::filesystem::perms::others_write) != std::filesystem::perms::none)
        {
            try
            {
                tty_buf = std::make_unique<WriteBufferFromFile>(tty_file_name, buf_size);

                /// It is possible that the terminal file has writeable permissions
                /// but we cannot write anything there. Check it with invisible character.
                tty_buf->write('\0');
                tty_buf->next();

                return;
            }
            catch (const Exception & e)
            {
                if (tty_buf)
                    tty_buf.reset();

                if (e.code() != ErrorCodes::CANNOT_OPEN_FILE)
                    throw;

                /// It is normal if file exists, indicated as writeable but still cannot be opened.
                /// Fallback to other options.
            }
        }
    }

    if (stderr_is_a_tty || progress == ProgressOption::ERR)
    {
        tty_buf = std::make_unique<WriteBufferFromFileDescriptor>(STDERR_FILENO, buf_size);
    }
    else
    {
        need_render_progress = false;
        need_render_progress_table = false;
    }
}

void ClientBase::initKeystrokeInterceptor()
{
    if (is_interactive && need_render_progress_table && getClientConfiguration().getBool("enable-progress-table-toggle", true))
    {
        keystroke_interceptor = std::make_unique<TerminalKeystrokeInterceptor>(in_fd, error_stream);
        keystroke_interceptor->registerCallback(' ', [this]() { progress_table_toggle_on = !progress_table_toggle_on; });
    }
}

void ClientBase::updateSuggest(const ASTPtr & ast)
{
    std::vector<std::string> new_words;

    if (auto * create = ast->as<ASTCreateQuery>())
    {
        if (create->database)
            new_words.push_back(create->getDatabase());
        new_words.push_back(create->getTable());

        if (create->columns_list && create->columns_list->columns)
        {
            for (const auto & elem : create->columns_list->columns->children)
            {
                if (const auto * column = elem->as<ASTColumnDeclaration>())
                    new_words.push_back(column->name);
            }
        }
    }

    if (const auto * create_function = ast->as<ASTCreateFunctionQuery>())
    {
        new_words.push_back(create_function->getFunctionName());
    }

    if (!new_words.empty())
        suggest->addWords(std::move(new_words));
}

bool ClientBase::isSyncInsertWithData(const ASTInsertQuery & insert_query, const ContextPtr & context)
{
    if (!insert_query.data)
        return false;

    auto settings = context->getSettingsCopy();
    if (insert_query.settings_ast)
        settings.applyChanges(insert_query.settings_ast->as<ASTSetQuery>()->changes);

    return !settings[Setting::async_insert];
}

void ClientBase::processTextAsSingleQuery(const String & full_query)
{
    /// Some parts of a query (result output and formatting) are executed
    /// client-side. Thus we need to parse the query.
    const char * begin = full_query.data();
    auto parsed_query = parseQuery(begin, begin + full_query.size(),
        client_context->getSettingsRef(),
        /*allow_multi_statements=*/ false);

    if (!parsed_query)
        return;

    String query_to_execute;

    /// Query will be parsed before checking the result because error does not
    /// always means a problem, i.e. if table already exists, and it is no a
    /// huge problem if suggestion will be added even on error, since this is
    /// just suggestion.
    ///
    /// Do not update suggest, until suggestion will be ready
    /// (this will avoid extra complexity)
    if (suggest)
        updateSuggest(parsed_query);

    /// An INSERT query may have the data that follows query text.
    /// Send part of the query without data, because data will be sent separately.
    /// But for asynchronous inserts we don't extract data, because it's needed
    /// to be done on server side in that case (for coalescing the data from multiple inserts on server side).
    const auto * insert = parsed_query->as<ASTInsertQuery>();
    if (insert && isSyncInsertWithData(*insert, client_context))
        query_to_execute = full_query.substr(0, insert->data - full_query.data());
    else
        query_to_execute = full_query;

    try
    {
        processParsedSingleQuery(full_query, query_to_execute, parsed_query, echo_queries);
    }
    catch (Exception & e)
    {
        if (server_exception)
            server_exception->rethrow();
        if (!is_interactive)
            e.addMessage("(in query: {})", full_query);
        throw;
    }

    if (have_error)
        processError(full_query);
}

void ClientBase::processOrdinaryQuery(const String & query_to_execute, ASTPtr parsed_query)
{
    auto query = query_to_execute;

    /// Rewrite query only when we have query parameters.
    /// Note that if query is rewritten, comments in query are lost.
    /// But the user often wants to see comments in server logs, query log, processlist, etc.
    /// For recent versions of the server query parameters will be transferred by network and applied on the server side.
    if (!query_parameters.empty()
        && connection->getServerRevision(connection_parameters.timeouts) < DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
    {
        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        ReplaceQueryParameterVisitor visitor(query_parameters);
        visitor.visit(parsed_query);

        /// Get new query after substitutions.
        if (visitor.getNumberOfReplacedParameters())
            query = serializeAST(*parsed_query);
        chassert(!query.empty());
    }

    if (allow_merge_tree_settings && parsed_query->as<ASTCreateQuery>())
    {
        /// Rewrite query if new settings were added.
        if (addMergeTreeSettings(*parsed_query->as<ASTCreateQuery>()))
        {
            /// Replace query parameters because AST cannot be serialized otherwise.
            if (!query_parameters.empty())
            {
                ReplaceQueryParameterVisitor visitor(query_parameters);
                visitor.visit(parsed_query);
            }

            query = serializeAST(*parsed_query);
        }
    }

    // Run some local checks to make sure queries into output file will work before sending to server.
    if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get()))
    {
        String out_file;
        if (query_with_output->out_file)
        {
            const auto & out_file_node = query_with_output->out_file->as<ASTLiteral &>();
            out_file = out_file_node.value.safeGet<std::string>();

            std::string compression_method_string;

            if (query_with_output->compression)
            {
                const auto & compression_method_node = query_with_output->compression->as<ASTLiteral &>();
                compression_method_string = compression_method_node.value.safeGet<std::string>();
            }

            CompressionMethod compression_method = chooseCompressionMethod(out_file, compression_method_string);
            UInt64 compression_level = 3;

            if (query_with_output->is_outfile_append && query_with_output->is_outfile_truncate)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot use INTO OUTFILE with APPEND and TRUNCATE simultaneously.");
            }

            if (query_with_output->is_outfile_append && compression_method != CompressionMethod::None)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot append to compressed file. Please use uncompressed file or remove APPEND keyword.");
            }

            if (query_with_output->compression_level)
            {
                const auto & compression_level_node = query_with_output->compression_level->as<ASTLiteral &>();
                bool res = compression_level_node.value.tryGet<UInt64>(compression_level);
                auto range = getCompressionLevelRange(compression_method);

                if (!res || compression_level < range.first || compression_level > range.second)
                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Invalid compression level, must be positive integer in range {}-{}",
                        range.first,
                        range.second);
            }

            if (fs::exists(out_file))
            {
                if (!query_with_output->is_outfile_append && !query_with_output->is_outfile_truncate)
                {
                    throw Exception(
                        ErrorCodes::FILE_ALREADY_EXISTS,
                        "File {} exists, consider using APPEND or TRUNCATE.",
                        out_file);
                }
            }
        }
    }

    const auto & settings = client_context->getSettingsRef();
    const Int32 signals_before_stop = settings[Setting::partial_result_on_first_cancel] ? 2 : 1;

    int retries_left = 10;
    while (retries_left)
    {
        try
        {
            query_interrupt_handler.start(signals_before_stop);
            SCOPE_EXIT({ query_interrupt_handler.stop(); });

            try {
                connection->sendQuery(
                    connection_parameters.timeouts,
                    query,
                    query_parameters,
                    client_context->getCurrentQueryId(),
                    query_processing_stage,
                    &client_context->getSettingsRef(),
                    &client_context->getClientInfo(),
                    true,
                    [&](const Progress & progress) { onProgress(progress); });

                if (send_external_tables)
                    sendExternalTables(parsed_query);
            }
            catch (const NetException &)
            {
                // We still want to attempt to process whatever we already received or can receive (socket receive buffer can be not empty)
                receiveResult(parsed_query, signals_before_stop, settings[Setting::partial_result_on_first_cancel]);
                throw;
            }

            receiveResult(parsed_query, signals_before_stop, settings[Setting::partial_result_on_first_cancel]);

            break;
        }
        catch (const Exception & e)
        {
            /// Retry when the server said "Client should retry" and no rows
            /// has been received yet.
            if (processed_rows == 0 && e.code() == ErrorCodes::DEADLOCK_AVOIDED && --retries_left)
            {
                error_stream << "Got a transient error from the server, will"
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
void ClientBase::receiveResult(ASTPtr parsed_query, Int32 signals_before_stop, bool partial_result_on_first_cancel)
{
    // TODO: get the poll_interval from commandline.
    const auto receive_timeout = connection_parameters.timeouts.receive_timeout;
    constexpr size_t default_poll_interval = 1000000; /// in microseconds
    constexpr size_t min_poll_interval = 5000; /// in microseconds
    const size_t poll_interval
        = std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));

    bool break_on_timeout = connection->getConnectionType() != IServerConnection::Type::LOCAL;

    std::exception_ptr local_format_error;

    if (keystroke_interceptor)
    {
        try
        {
            keystroke_interceptor->startIntercept();
        }
        catch (const DB::Exception &)
        {
            error_stream << getCurrentExceptionMessage(false);
            keystroke_interceptor.reset();
        }
    }

    SCOPE_EXIT({
        if (keystroke_interceptor)
        {
            try
            {
                keystroke_interceptor->stopIntercept();
            }
            catch (...)
            {
                error_stream << getCurrentExceptionMessage(false);
                keystroke_interceptor.reset();
            }
        }
    });

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
                if (partial_result_on_first_cancel && query_interrupt_handler.cancelled_status() == signals_before_stop - 1)
                {
                    connection->sendCancel();
                    /// First cancel reading request was sent. Next requests will only be with a full cancel
                    partial_result_on_first_cancel = false;
                }
                else if (query_interrupt_handler.cancelled())
                {
                    cancelQuery();
                }
                else
                {
                    double elapsed = receive_watch.elapsedSeconds();
                    if (break_on_timeout && elapsed > receive_timeout.totalSeconds())
                    {
                        output_stream << "Timeout exceeded while receiving data from server."
                                    << " Waited for " << static_cast<size_t>(elapsed) << " seconds,"
                                    << " timeout is " << receive_timeout.totalSeconds() << " seconds." << std::endl;

                        cancelQuery();
                    }
                }
            }

            /// Poll for changes after a cancellation check, otherwise it never reached
            /// because of progress updates from server.

            if (connection->poll(poll_interval))
                break;
        }

        try
        {
            if (!receiveAndProcessPacket(parsed_query, cancelled))
                break;
        }
        catch (const LocalFormatError &)
        {
            /// Remember the first exception.
            if (!local_format_error)
                local_format_error = std::current_exception();
            connection->sendCancel();
        }
    }

    if (local_format_error)
        std::rethrow_exception(local_format_error);

    if (cancelled && is_interactive && !cancelled_printed.exchange(true))
        output_stream << "Query was cancelled." << std::endl;
}


/// Receive a part of the result, or progress info or an exception and process it.
/// Returns true if one should continue receiving packets.
/// Output of result is suppressed if query was cancelled.
bool ClientBase::receiveAndProcessPacket(ASTPtr parsed_query, bool cancelled_)
{
    Packet packet = connection->receivePacket();

    switch (packet.type)
    {
        case Protocol::Server::PartUUIDs:
            return true;

        case Protocol::Server::Data:
            if (!cancelled_)
                onData(packet.block, parsed_query);
            return true;

        case Protocol::Server::Progress:
            onProgress(packet.progress);
            return true;

        case Protocol::Server::ProfileInfo:
            onProfileInfo(packet.profile_info);
            return true;

        case Protocol::Server::Totals:
            if (!cancelled_)
                onTotals(packet.block, parsed_query);
            return true;

        case Protocol::Server::Extremes:
            if (!cancelled_)
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

        case Protocol::Server::TimezoneUpdate:
            onTimezoneUpdate(packet.server_timezone);
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

    if (need_render_progress && tty_buf)
        progress_indication.writeProgress(*tty_buf);
}

void ClientBase::onTimezoneUpdate(const String & tz)
{
    client_context->setSetting("session_timezone", tz);
}


void ClientBase::onEndOfStream()
{
    if (need_render_progress && tty_buf)
        progress_indication.clearProgressOutput(*tty_buf);
    if (need_render_progress_table && tty_buf)
        progress_table.clearTableOutput(*tty_buf);

    if (output_format)
    {
        /// Do our best to estimate the start of the query so the output format matches the one reported by the server
        bool is_running = false;
        output_format->setStartTime(
            clock_gettime_ns(CLOCK_MONOTONIC) - static_cast<UInt64>(progress_indication.elapsedSeconds() * 1000000000), is_running);

        try
        {
            output_format->finalize();
        }
        catch (...)
        {
            /// Format should be reset to make it work for subsequent query
            /// (otherwise it will throw again in resetOutput())
            output_format.reset();
            throw;
        }
    }

    resetOutput();

    if (is_interactive)
    {
        if (cancelled && !cancelled_printed.exchange(true))
            output_stream << "Query was cancelled." << std::endl;
        else if (!written_first_block)
            output_stream << "Ok." << std::endl;
    }
}


void ClientBase::onProfileEvents(Block & block)
{
    const auto rows = block.rows();
    if (rows == 0)
        return;

    if (getName() == "local" || server_revision >= DBMS_MIN_PROTOCOL_VERSION_WITH_INCREMENTAL_PROFILE_EVENTS)
    {
        const auto & array_thread_id = typeid_cast<const ColumnUInt64 &>(*block.getByName("thread_id").column).getData();
        const auto & names = typeid_cast<const ColumnString &>(*block.getByName("name").column);
        const auto & host_names = typeid_cast<const ColumnString &>(*block.getByName("host_name").column);
        const auto & array_values = typeid_cast<const ColumnInt64 &>(*block.getByName("value").column).getData();

        const auto * user_time_name = ProfileEvents::getName(ProfileEvents::UserTimeMicroseconds);
        const auto * system_time_name = ProfileEvents::getName(ProfileEvents::SystemTimeMicroseconds);

        HostToTimesMap thread_times;
        for (size_t i = 0; i < rows; ++i)
        {
            auto thread_id = array_thread_id[i];
            auto host_name = host_names.getDataAt(i).toString();

            /// In ProfileEvents packets thread id 0 specifies common profiling information
            /// for all threads executing current query on specific host. So instead of summing per thread
            /// consumption it's enough to look for data with thread id 0.
            if (thread_id != THREAD_GROUP_ID)
                continue;

            auto event_name = names.getDataAt(i);
            auto value = array_values[i];

            /// Ignore negative time delta or memory usage just in case.
            if (value < 0)
                continue;

            if (event_name == user_time_name)
                thread_times[host_name].user_ms = value;
            else if (event_name == system_time_name)
                thread_times[host_name].system_ms = value;
            else if (event_name == MemoryTracker::USAGE_EVENT_NAME)
                thread_times[host_name].memory_usage = value;
            else if (event_name == MemoryTracker::PEAK_USAGE_EVENT_NAME)
                thread_times[host_name].peak_memory_usage = value;
        }
        progress_indication.updateThreadEventData(thread_times);
        progress_table.updateTable(block);

        if (need_render_progress && tty_buf)
            progress_indication.writeProgress(*tty_buf);
        if (need_render_progress_table && tty_buf && !cancelled)
        {
            bool toggle_enabled = getClientConfiguration().getBool("enable-progress-table-toggle", true);
            progress_table.writeTable(*tty_buf, progress_table_toggle_on.load(), toggle_enabled);
        }

        if (profile_events.print)
        {
            if (profile_events.watch.elapsedMilliseconds() >= profile_events.delay_ms)
            {
                /// We need to restart the watch each time we flushed these events
                profile_events.watch.restart();
                initLogsOutputStream();
                if (need_render_progress && tty_buf)
                    progress_indication.clearProgressOutput(*tty_buf);
                if (need_render_progress_table && tty_buf)
                    progress_table.clearTableOutput(*tty_buf);
                logs_out_stream->writeProfileEvents(block);
                logs_out_stream->flush();

                profile_events.last_block = {};
            }
            else
            {
                incrementProfileEventsBlock(profile_events.last_block, block);
            }
        }
    }
}


/// Flush all buffers.
void ClientBase::resetOutput()
{
    /// Order is important: format, compression, file

    if (output_format)
        output_format->finalize();
    output_format.reset();

    logs_out_stream.reset();

    if (out_file_buf)
    {
        out_file_buf->finalize();
        out_file_buf.reset();
    }

    if (pager_cmd)
    {
        pager_cmd->in.close();
        pager_cmd->wait();

        if (SIG_ERR == signal(SIGPIPE, SIG_DFL))
            throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler for SIGPIPE");
        if (SIG_ERR == signal(SIGINT, SIG_DFL))
            throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler for SIGINT");
        if (SIG_ERR == signal(SIGQUIT, SIG_DFL))
            throw ErrnoException(ErrorCodes::CANNOT_SET_SIGNAL_HANDLER, "Cannot set signal handler for SIGQUIT");

        setupSignalHandler();
    }
    pager_cmd = nullptr;

    if (out_logs_buf)
    {
        out_logs_buf->finalize();
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

            case Protocol::Server::TimezoneUpdate:
                onTimezoneUpdate(packet.server_timezone);
                break;

            default:
                throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet from server (expected Data, Exception, Log or TimezoneUpdate, got {})",
                    String(Protocol::Server::toString(packet.type)));
        }
    }
}


void ClientBase::setInsertionTable(const ASTInsertQuery & insert_query)
{
    if (!client_context->hasInsertionTable() && insert_query.table)
    {
        String table = insert_query.table->as<ASTIdentifier &>().shortName();
        if (!table.empty())
        {
            String database = insert_query.database ? insert_query.database->as<ASTIdentifier &>().shortName() : "";
            client_context->setInsertionTable(StorageID(database, table));
        }
    }
}


namespace
{
bool isStdinNotEmptyAndValid(ReadBufferFromFileDescriptor & std_in)
{
    try
    {
        return !std_in.eof();
    }
    catch (const Exception & e)
    {
        if (e.code() == ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR)
            return false;
        throw;
    }
}
}


void ClientBase::processInsertQuery(const String & query_to_execute, ASTPtr parsed_query)
{
    auto query = query_to_execute;
    if (!query_parameters.empty()
        && connection->getServerRevision(connection_parameters.timeouts) < DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS)
    {
        /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
        ReplaceQueryParameterVisitor visitor(query_parameters);
        visitor.visit(parsed_query);

        /// Get new query after substitutions.
        if (visitor.getNumberOfReplacedParameters())
            query = serializeAST(*parsed_query);
        chassert(!query.empty());
    }

    /// Process the query that requires transferring data blocks to the server.
    const auto & parsed_insert_query = parsed_query->as<ASTInsertQuery &>();
    if ((!parsed_insert_query.data && !parsed_insert_query.infile) && (is_interactive || (!stdin_is_a_tty && !isStdinNotEmptyAndValid(std_in))))
    {
        const auto & settings = client_context->getSettingsRef();
        if (settings[Setting::throw_if_no_data_to_insert])
            throw Exception(ErrorCodes::NO_DATA_TO_INSERT, "No data to insert");
        return;
    }

    query_interrupt_handler.start();
    SCOPE_EXIT({ query_interrupt_handler.stop(); });

    connection->sendQuery(
        connection_parameters.timeouts,
        query,
        query_parameters,
        client_context->getCurrentQueryId(),
        query_processing_stage,
        &client_context->getSettingsRef(),
        &client_context->getClientInfo(),
        true,
        [&](const Progress & progress) { onProgress(progress); });

    if (send_external_tables)
        sendExternalTables(parsed_query);

    /// Receive description of table structure.
    Block sample;
    ColumnsDescription columns_description;
    if (receiveSampleBlock(sample, columns_description, parsed_query))
    {
        /// If structure was received (thus, server has not thrown an exception),
        /// send our data with that structure.
        setInsertionTable(parsed_insert_query);

        sendData(sample, columns_description, parsed_query);
        receiveEndOfQuery();
    }
}


void ClientBase::sendData(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query)
{
    /// Get columns description from variable or (if it was empty) create it from sample.
    auto columns_description_for_query = columns_description.empty() ? ColumnsDescription(sample.getNamesAndTypesList()) : columns_description;
    if (columns_description_for_query.empty())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Column description is empty and it can't be built from sample from table. "
                        "Cannot execute query.");
    }

    /// If INSERT data must be sent.
    auto * parsed_insert_query = parsed_query->as<ASTInsertQuery>();
    if (!parsed_insert_query)
        return;

    bool have_data_in_stdin = !is_interactive && !stdin_is_a_tty && isStdinNotEmptyAndValid(std_in);

    if (need_render_progress)
    {
        /// Set total_bytes_to_read for current fd.
        FileProgress file_progress(0, std_in.getFileSize());
        progress_indication.updateProgress(Progress(file_progress));

        /// Set callback to be called on file progress.
        if (tty_buf)
            progress_indication.setFileProgressCallback(client_context, *tty_buf);
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

        String current_format = parsed_insert_query->format;
        if (current_format.empty())
            current_format = FormatFactory::instance().getFormatFromFileName(in_file);

        /// Create temporary storage file, to support globs and parallel reading
        /// StorageFile doesn't support ephemeral/materialized/alias columns.
        /// We should change ephemeral columns to ordinary and ignore materialized/alias columns.
        ColumnsDescription columns_for_storage_file;
        for (const auto & [name, _] : columns_description_for_query.getInsertable())
        {
            ColumnDescription column = columns_description_for_query.get(name);
            column.default_desc.kind = ColumnDefaultKind::Default;
            columns_for_storage_file.add(std::move(column));
        }

        StorageFile::CommonArguments args{
            WithContext(client_context),
            parsed_insert_query->table_id,
            current_format,
            getFormatSettings(client_context),
            compression_method,
            columns_for_storage_file,
            ConstraintsDescription{},
            String{},
            {},
            String{},
        };
        StoragePtr storage = std::make_shared<StorageFile>(in_file, client_context->getUserFilesPath(), args);
        storage->startup();
        SelectQueryInfo query_info;

        try
        {
            auto metadata = storage->getInMemoryMetadataPtr();
            QueryPlan plan;
            storage->read(
                plan,
                sample.getNames(),
                storage->getStorageSnapshot(metadata, client_context),
                query_info,
                client_context,
                {},
                client_context->getSettingsRef()[Setting::max_block_size],
                getNumberOfCPUCoresToUse());

            auto builder = plan.buildQueryPipeline(
                QueryPlanOptimizationSettings::fromContext(client_context),
                BuildQueryPipelineSettings::fromContext(client_context));

            QueryPlanResourceHolder resources;
            auto pipe = QueryPipelineBuilder::getPipe(std::move(*builder), resources);

            sendDataFromPipe(
                std::move(pipe),
                parsed_query,
                have_data_in_stdin);
        }
        catch (Exception & e)
        {
            e.addMessage("data for INSERT was parsed from file");
            throw;
        }

        if (have_data_in_stdin && !cancelled)
            sendDataFromStdin(sample, columns_description_for_query, parsed_query);
    }
    else if (parsed_insert_query->data)
    {
        /// Send data contained in the query.
        ReadBufferFromMemory data_in(parsed_insert_query->data, parsed_insert_query->end - parsed_insert_query->data);
        try
        {
            sendDataFrom(data_in, sample, columns_description_for_query, parsed_query, have_data_in_stdin);
            if (have_data_in_stdin && !cancelled)
                sendDataFromStdin(sample, columns_description_for_query, parsed_query);
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
        sendDataFromStdin(sample, columns_description_for_query, parsed_query);
    }
    else
        throw Exception(ErrorCodes::NO_DATA_TO_INSERT, "No data to insert");
}


void ClientBase::sendDataFrom(ReadBuffer & buf, Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query, bool have_more_data)
{
    String current_format = "Values";

    /// Data format can be specified in the INSERT query.
    if (const auto * insert = parsed_query->as<ASTInsertQuery>())
    {
        if (!insert->format.empty())
            current_format = insert->format;
    }

    auto source = client_context->getInputFormat(current_format, buf, sample, insert_format_max_block_size);
    Pipe pipe(source);

    if (columns_description.hasDefaults())
    {
        pipe.addSimpleTransform([&](const Block & header)
        {
            return std::make_shared<AddingDefaultsTransform>(header, columns_description, *source, client_context);
        });
    }

    sendDataFromPipe(std::move(pipe), parsed_query, have_more_data);
}

void ClientBase::sendDataFromPipe(Pipe&& pipe, ASTPtr parsed_query, bool have_more_data)
try
{
    QueryPipeline pipeline(std::move(pipe));
    PullingAsyncPipelineExecutor executor(pipeline);

    /// Concurrency control in client is not required
    pipeline.setConcurrencyControl(false);

    if (need_render_progress)
    {
        pipeline.setProgressCallback([this](const Progress & progress){ onProgress(progress); });
    }

    Block block;
    while (executor.pull(block))
    {
        if (!cancelled && query_interrupt_handler.cancelled())
        {
            cancelQuery();
            executor.cancel();
            return;
        }

        /// Check if server send Log packet
        receiveLogsAndProfileEvents(parsed_query);

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

    if (!have_more_data)
        connection->sendData({}, "", false);
}
catch (...)
{
    connection->sendCancel();
    receiveEndOfQuery();
    throw;
}

void ClientBase::sendDataFromStdin(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query)
{
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


/// Process Log packets, used when inserting data by blocks
void ClientBase::receiveLogsAndProfileEvents(ASTPtr parsed_query)
{
    auto packet_type = connection->checkPacket(0);

    while (packet_type && (*packet_type == Protocol::Server::Log
            || *packet_type == Protocol::Server::ProfileEvents
            || *packet_type == Protocol::Server::TimezoneUpdate))
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
                break;

            case Protocol::Server::ProfileEvents:
                onProfileEvents(packet.block);
                break;

            case Protocol::Server::TimezoneUpdate:
                onTimezoneUpdate(packet.server_timezone);
                break;

            default:
                throw NetException(ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER,
                    "Unexpected packet from server (expected Exception, EndOfStream, Log, Progress or ProfileEvents. Got {})",
                    String(Protocol::Server::toString(packet.type)));
        }
    }
}

void ClientBase::cancelQuery()
{
    connection->sendCancel();

    if (keystroke_interceptor)
        try
        {
            keystroke_interceptor->stopIntercept();
        }
        catch (const DB::Exception &)
        {
            error_stream << getCurrentExceptionMessage(false);
        }

    if (need_render_progress && tty_buf)
        progress_indication.clearProgressOutput(*tty_buf);
    if (need_render_progress_table && tty_buf)
        progress_table.clearTableOutput(*tty_buf);

    if (is_interactive)
        output_stream << "Cancelling query." << std::endl;

    cancelled = true;
}

void ClientBase::processParsedSingleQuery(const String & full_query, const String & query_to_execute,
        ASTPtr parsed_query, std::optional<bool> echo_query_, bool report_error)
{
    resetOutput();
    have_error = false;
    cancelled = false;
    cancelled_printed = false;
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
        client_context->setCurrentQueryId("");
        // Generate a new query_id
        for (const auto & query_id_format : query_id_formats)
        {
            writeString(query_id_format.first, std_out);
            writeString(fmt::format(fmt::runtime(query_id_format.second), fmt::arg("query_id", client_context->getCurrentQueryId())), std_out);
            writeChar('\n', std_out);
            std_out.next();
        }
    }

    if (const auto * set_query = parsed_query->as<ASTSetQuery>())
    {
        const auto * logs_level_field = set_query->changes.tryGet(std::string_view{"send_logs_level"});
        if (logs_level_field)
        {
            auto logs_level = logs_level_field->safeGet<String>();
            /// Check that setting value is correct before updating logger level.
            SettingFieldLogsLevelTraits::fromString(logs_level);
            updateLoggerLevel(logs_level);
        }
    }

    if (const auto * create_user_query = parsed_query->as<ASTCreateUserQuery>())
    {
        if (!create_user_query->attach && !create_user_query->authentication_methods.empty())
        {
            for (const auto & authentication_method : create_user_query->authentication_methods)
            {
                auto password = authentication_method->getPassword();

                if (password)
                    client_context->getAccessControl().checkPasswordComplexityRules(*password);
            }
        }
    }

    processed_rows = 0;
    written_first_block = false;
    progress_indication.resetProgress();
    progress_table.resetTable();
    profile_events.watch.restart();

    {
        /// Temporarily apply query settings to context.
        std::optional<Settings> old_settings;
        SCOPE_EXIT_SAFE({
            try
            {
                /// We need to park ParallelFormating threads,
                /// because they can use settings from global context
                /// and it can lead to data race with `setSettings`
                resetOutput();
            }
            catch (...)
            {
                if (!have_error)
                {
                    client_exception = std::make_unique<Exception>(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
                    have_error = true;
                }
            }
            if (old_settings)
                client_context->setSettings(*old_settings);
        });

        auto apply_query_settings = [&](const IAST & settings_ast)
        {
            if (!old_settings)
                old_settings.emplace(client_context->getSettingsRef());
            client_context->applySettingsChanges(settings_ast.as<ASTSetQuery>()->changes);
            client_context->resetSettingsToDefaultValue(settings_ast.as<ASTSetQuery>()->default_settings);
        };

        const auto * insert = parsed_query->as<ASTInsertQuery>();
        if (const auto * select = parsed_query->as<ASTSelectQuery>(); select && select->settings())
            apply_query_settings(*select->settings());
        else if (const auto * select_with_union = parsed_query->as<ASTSelectWithUnionQuery>())
        {
            const ASTs & children = select_with_union->list_of_selects->children;
            if (!children.empty())
            {
                // On the client it is enough to apply settings only for the
                // last SELECT, since the only thing that is important to apply
                // on the client is format settings.
                const auto * last_select = children.back()->as<ASTSelectQuery>();
                if (last_select && last_select->settings())
                {
                    apply_query_settings(*last_select->settings());
                }
            }
        }
        else if (const auto * query_with_output = parsed_query->as<ASTQueryWithOutput>(); query_with_output && query_with_output->settings_ast)
            apply_query_settings(*query_with_output->settings_ast);
        else if (insert && insert->settings_ast)
            apply_query_settings(*insert->settings_ast);

        if (!connection->checkConnected(connection_parameters.timeouts))
            connect();

        ASTPtr input_function;
        if (insert && insert->select)
            insert->tryFindInputFunction(input_function);

        bool is_async_insert_with_inlined_data = client_context->getSettingsRef()[Setting::async_insert] && insert && insert->hasInlinedData();

        if (is_async_insert_with_inlined_data)
        {
            bool have_data_in_stdin = !is_interactive && !stdin_is_a_tty && isStdinNotEmptyAndValid(std_in);
            bool have_external_data = have_data_in_stdin || insert->infile;

            if (have_external_data)
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "Processing async inserts with both inlined and external data (from stdin or infile) is not supported");
        }

        /// INSERT query for which data transfer is needed (not an INSERT SELECT or input()) is processed separately.
        if (insert && (!insert->select || input_function) && !is_async_insert_with_inlined_data)
        {
            if (input_function && insert->format.empty())
                throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "FORMAT must be specified for function input()");

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
                    client_context->applySettingChange(change);
            }
            client_context->resetSettingsToDefaultValue(set_query->default_settings);

            /// Query parameters inside SET queries should be also saved on the client side
            ///  to override their previous definitions set with --param_* arguments
            ///  and for substitutions to work inside INSERT ... VALUES queries
            for (const auto & [name, value] : set_query->query_parameters)
                query_parameters.insert_or_assign(name, value);

            client_context->addQueryParameters(NameToNameMap{set_query->query_parameters.begin(), set_query->query_parameters.end()});
        }
        if (const auto * use_query = parsed_query->as<ASTUseQuery>())
        {
            const String & new_database = use_query->getDatabase();
            /// If the client initiates the reconnection, it takes the settings from the config.
            getClientConfiguration().setString("database", new_database);
            /// If the connection initiates the reconnection, it uses its variable.
            connection->setDefaultDatabase(new_database);
        }
    }

    /// Always print last block (if it was not printed already)
    if (profile_events.last_block)
    {
        initLogsOutputStream();
        if (need_render_progress && tty_buf)
            progress_indication.clearProgressOutput(*tty_buf);
        if (need_render_progress_table && tty_buf)
            progress_table.clearTableOutput(*tty_buf);
        logs_out_stream->writeProfileEvents(profile_events.last_block);
        logs_out_stream->flush();

        profile_events.last_block = {};
    }

    if (is_interactive)
    {
        output_stream << std::endl;
        if (!server_exception || processed_rows != 0)
            output_stream << processed_rows << " row" << (processed_rows == 1 ? "" : "s") << " in set. ";
        output_stream << "Elapsed: " << progress_indication.elapsedSeconds() << " sec. ";
        progress_indication.writeFinalProgress();
        bool toggle_enabled = getClientConfiguration().getBool("enable-progress-table-toggle", true);
        bool show_progress_table = !toggle_enabled || progress_table_toggle_on;
        if (need_render_progress_table && show_progress_table)
            progress_table.writeFinalTable();
        output_stream << std::endl << std::endl;
    }
    else
    {
        const auto & config = getClientConfiguration();
        if (config.getBool("print-time-to-stderr", false))
            error_stream << progress_indication.elapsedSeconds() << "\n";

        const auto & print_memory_mode = config.getString("print-memory-to-stderr", "");
        auto peak_memory_usage = std::max<Int64>(progress_indication.getMemoryUsage().peak, 0);
        if (print_memory_mode == "default")
            error_stream << peak_memory_usage << "\n";
        else if (print_memory_mode == "readable")
            error_stream << formatReadableSizeWithBinarySuffix(peak_memory_usage) << "\n";
    }

    if (!is_interactive && getClientConfiguration().getBool("print-num-processed-rows", false))
    {
        output_stream << "Processed rows: " << processed_rows << "\n";
    }

    if (have_error && report_error)
        processError(full_query);
}


MultiQueryProcessingStage ClientBase::analyzeMultiQueryText(
    const char *& this_query_begin, const char *& this_query_end, const char * all_queries_end,
    String & query_to_execute, ASTPtr & parsed_query, const String & all_queries_text,
    std::unique_ptr<Exception> & current_exception)
{
    if (!is_interactive && cancelled)
        return MultiQueryProcessingStage::QUERIES_END;

    if (this_query_begin >= all_queries_end)
        return MultiQueryProcessingStage::QUERIES_END;

    // Remove leading empty newlines and other whitespace, because they
    // are annoying to filter in the query log. This is mostly relevant for
    // the tests.
    while (this_query_begin < all_queries_end && isWhitespaceASCII(*this_query_begin))
        ++this_query_begin;

    if (this_query_begin >= all_queries_end)
        return MultiQueryProcessingStage::QUERIES_END;

    unsigned max_parser_depth = static_cast<unsigned>(client_context->getSettingsRef()[Setting::max_parser_depth]);
    unsigned max_parser_backtracks = static_cast<unsigned>(client_context->getSettingsRef()[Setting::max_parser_backtracks]);

    // If there are only comments left until the end of file, we just
    // stop. The parser can't handle this situation because it always
    // expects that there is some query that it can parse.
    // We can get into this situation because the parser also doesn't
    // skip the trailing comments after parsing a query. This is because
    // they may as well be the leading comments for the next query,
    // and it makes more sense to treat them as such.
    {
        Tokens tokens(this_query_begin, all_queries_end);
        IParser::Pos token_iterator(tokens, max_parser_depth, max_parser_backtracks);
        if (!token_iterator.isValid())
            return MultiQueryProcessingStage::QUERIES_END;
    }

    this_query_end = this_query_begin;
    try
    {
        parsed_query = parseQuery(this_query_end, all_queries_end,
            client_context->getSettingsRef(),
            /*allow_multi_statements=*/ true);
    }
    catch (const Exception & e)
    {
        current_exception.reset(e.clone());
        return MultiQueryProcessingStage::PARSING_EXCEPTION;
    }

    if (!parsed_query)
    {
        if (ignore_error)
        {
            Tokens tokens(this_query_begin, all_queries_end);
            IParser::Pos token_iterator(tokens, max_parser_depth, max_parser_backtracks);
            while (token_iterator->type != TokenType::Semicolon && token_iterator.isValid())
                ++token_iterator;
            this_query_begin = token_iterator->end;

            return MultiQueryProcessingStage::CONTINUE_PARSING;
        }

        return MultiQueryProcessingStage::PARSING_FAILED;
    }

    // INSERT queries may have the inserted data in the query text that follow the query itself, e.g. "insert into t format CSV 1,2". They
    // need special handling.
    // - If the INSERT statement FORMAT is VALUES, we use the VALUES format parser to skip the inserted data until we reach the trailing single semicolon.
    // - Other formats (e.g. FORMAT CSV) are arbitrarily more complex and tricky to parse. For example, we may be unable to distinguish if the semicolon
    //   is part of the data or ends the statement. In this case, we simply assume that the end of the INSERT statement is determined by \n\n (two newlines).
    auto * insert_ast = parsed_query->as<ASTInsertQuery>();
    // We also consider the INSERT query in EXPLAIN queries (same as normal INSERT queries)
    if (!insert_ast)
    {
        auto * explain_ast = parsed_query->as<ASTExplainQuery>();
        if (explain_ast && explain_ast->getExplainedQuery())
        {
            insert_ast = explain_ast->getExplainedQuery()->as<ASTInsertQuery>();
        }
    }
    const char * query_to_execute_end = this_query_end;
    if (insert_ast && insert_ast->data)
    {
        if (insert_ast->format == "Values")
        {
            // Invoke the VALUES format parser to skip the inserted data
            ReadBufferFromMemory data_in(insert_ast->data, all_queries_end - insert_ast->data);
            skipBOMIfExists(data_in);
            do
            {
                skipWhitespaceIfAny(data_in);
                if (data_in.eof() || *data_in.position() == ';')
                    break;
            }
            while (ValuesBlockInputFormat::skipToNextRow(&data_in, 1, 0));
            // Handle the case of a comment followed by a semicolon
            //   Example: INSERT INTO tab VALUES xx; -- {serverError xx}
            // If we use this error hint, the next query should not be placed on the same line
            this_query_end = insert_ast->data + data_in.count();
            const auto * pos_newline = find_first_symbols<'\n'>(this_query_end, all_queries_end);
            if (pos_newline != this_query_end)
            {
                TestHint hint(String(this_query_end, pos_newline - this_query_end));
                if (hint.hasClientErrors() || hint.hasServerErrors())
                    this_query_end = pos_newline;
            }
        }
        else
        {
            // Handling of generic formats
            auto pos_newline = String(insert_ast->data, all_queries_end).find("\n\n");
            if (pos_newline != std::string::npos)
                this_query_end = insert_ast->data + pos_newline;
            else
                this_query_end = all_queries_end;
        }
        insert_ast->end = this_query_end;
        query_to_execute_end = isSyncInsertWithData(*insert_ast, client_context) ? insert_ast->data : this_query_end;
    }

    query_to_execute = all_queries_text.substr(this_query_begin - all_queries_text.data(), query_to_execute_end - this_query_begin);

    // Try to include the trailing comment with test hints. It is just
    // a guess for now, because we don't yet know where the query ends
    // if it is an INSERT query with inline data. We will do it again
    // after we have processed the query. But even this guess is
    // beneficial so that we see proper trailing comments in "echo" and
    // server log.
    adjustQueryEnd(this_query_end, all_queries_end, max_parser_depth, max_parser_backtracks);
    return MultiQueryProcessingStage::EXECUTE_QUERY;
}


bool ClientBase::executeMultiQuery(const String & all_queries_text)
{
    bool echo_query = echo_queries;

    {
        /// disable logs if expects errors
        TestHint test_hint(all_queries_text);
        if (test_hint.hasClientErrors() || test_hint.hasServerErrors())
            processTextAsSingleQuery("SET send_logs_level = 'fatal'");
    }

    /// Test tags are started with "--" so they are interpreted as comments anyway.
    /// But if the echo is enabled we have to remove the test tags from `all_queries_text`
    /// because we don't want test tags to be echoed.
    size_t test_tags_length = getTestTagsLength(all_queries_text);

    /// Several queries separated by ';'.
    /// INSERT data is ended by the empty line (\n\n), not ';'.
    /// Unnecessary semicolons may cause data to be parsed containing ';'
    /// e.g. 'insert into xx format csv val;' will insert "val;" instead of "val"
    ///      'insert into xx format csv val\n;' will insert "val" and ";"
    /// An exception is VALUES format where we also support semicolon in
    /// addition to end of line.
    const char * this_query_begin = all_queries_text.data() + test_tags_length;
    const char * this_query_end;
    const char * all_queries_end = all_queries_text.data() + all_queries_text.size();

    String full_query; // full_query is the query + inline INSERT data + trailing comments (the latter is our best guess for now).
    String query_to_execute;
    ASTPtr parsed_query;
    std::unique_ptr<Exception> current_exception;
    size_t retries_count = 0;
    bool is_first = true;

    while (true)
    {
        auto stage = analyzeMultiQueryText(this_query_begin, this_query_end, all_queries_end,
                                           query_to_execute, parsed_query, all_queries_text, current_exception);
        switch (stage)
        {
            case MultiQueryProcessingStage::QUERIES_END:
            {
                /// Compatible with old version when run interactive, e.g. "", "\ld"
                if (is_first && is_interactive)
                    processTextAsSingleQuery(all_queries_text);
                return true;
            }
            case MultiQueryProcessingStage::PARSING_FAILED:
            {
                return true;
            }
            case MultiQueryProcessingStage::CONTINUE_PARSING:
            {
                is_first = false;
                continue;
            }
            case MultiQueryProcessingStage::PARSING_EXCEPTION:
            {
                is_first = false;
                this_query_end = find_first_symbols<'\n'>(this_query_end, all_queries_end);

                // Try to find test hint for syntax error. We don't know where
                // the query ends because we failed to parse it, so we consume
                // the entire line.
                TestHint hint(String(this_query_begin, this_query_end - this_query_begin));
                if (hint.hasServerErrors())
                {
                    // Syntax errors are considered as client errors
                    current_exception->addMessage("\nExpected server error: {}.", hint.serverErrors());
                    current_exception->rethrow();
                }

                if (!hint.hasExpectedClientError(current_exception->code()))
                {
                    if (hint.hasClientErrors())
                        current_exception->addMessage("\nExpected client error: {}.", hint.clientErrors());

                    current_exception->rethrow();
                }

                /// It's expected syntax error, skip the line
                this_query_begin = this_query_end;
                current_exception.reset();

                continue;
            }
            case MultiQueryProcessingStage::EXECUTE_QUERY:
            {
                is_first = false;
                full_query = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);
                if (query_fuzzer_runs)
                {
                    if (!processWithFuzzing(full_query))
                        return false;

                    this_query_begin = this_query_end;
                    continue;
                }
                if (suggest)
                    updateSuggest(parsed_query);

                // Now we know for sure where the query ends.
                // Look for the hint in the text of query + insert data + trailing
                // comments, e.g. insert into t format CSV 'a' -- { serverError 123 }.
                // Use the updated query boundaries we just calculated.
                TestHint test_hint(full_query);

                // Echo all queries if asked; makes for a more readable reference file.
                echo_query = test_hint.echoQueries().value_or(echo_query);

                try
                {
                    processParsedSingleQuery(full_query, query_to_execute, parsed_query, echo_query, false);
                }
                catch (...)
                {
                    // Surprisingly, this is a client error. A server error would
                    // have been reported without throwing (see onReceiveExceptionFromServer()).
                    client_exception = std::make_unique<Exception>(getCurrentExceptionMessageAndPattern(print_stack_trace), getCurrentExceptionCode());
                    have_error = true;
                }

                // Check whether the error (or its absence) matches the test hints
                // (or their absence).
                bool error_matches_hint = true;
                bool need_retry = test_hint.needRetry(server_exception, &retries_count);
                if (need_retry)
                {
                    std::this_thread::sleep_for(std::chrono::seconds(1));
                }
                else if (have_error)
                {
                    if (test_hint.hasServerErrors())
                    {
                        if (!server_exception)
                        {
                            error_matches_hint = false;
                            fmt::print(stderr, "Expected server error code '{}' but got no server error (query: {}).\n",
                                       test_hint.serverErrors(), full_query);
                        }
                        else if (!test_hint.hasExpectedServerError(server_exception->code()))
                        {
                            error_matches_hint = false;
                            fmt::print(stderr, "Expected server error code: {} but got: {} (query: {}).\n",
                                       test_hint.serverErrors(), server_exception->code(), full_query);
                        }
                    }
                    if (test_hint.hasClientErrors())
                    {
                        if (!client_exception)
                        {
                            error_matches_hint = false;
                            fmt::print(stderr, "Expected client error code '{}' but got no client error (query: {}).\n",
                                       test_hint.clientErrors(), full_query);
                        }
                        else if (!test_hint.hasExpectedClientError(client_exception->code()))
                        {
                            error_matches_hint = false;
                            fmt::print(stderr, "Expected client error code '{}' but got '{}' (query: {}).\n",
                                       test_hint.clientErrors(), client_exception->code(), full_query);
                        }
                    }
                    if (!test_hint.hasClientErrors() && !test_hint.hasServerErrors())
                    {
                        // No error was expected but it still occurred. This is the
                        // default case without test hint, doesn't need additional
                        // diagnostics.
                        error_matches_hint = false;
                    }
                }
                else
                {
                    if (test_hint.hasClientErrors())
                    {
                        error_matches_hint = false;
                        fmt::print(stderr,
                                   "The query succeeded but the client error '{}' was expected (query: {}).\n",
                                   test_hint.clientErrors(), full_query);
                    }
                    if (test_hint.hasServerErrors())
                    {
                        error_matches_hint = false;
                        fmt::print(stderr,
                                   "The query succeeded but the server error '{}' was expected (query: {}).\n",
                                   test_hint.serverErrors(), full_query);
                    }
                }

                // If the error is expected, force reconnect and ignore it.
                if (have_error && error_matches_hint)
                {
                    client_exception.reset();
                    server_exception.reset();

                    have_error = false;

                    if (!connection->checkConnected(connection_parameters.timeouts))
                        connect();
                }

                // For INSERTs with inline data: use the end of inline data as
                // reported by the format parser (it is saved in sendData()).
                // This allows us to handle queries like:
                //   insert into t values (1); select 1
                // , where the inline data is delimited by semicolon and not by a
                // newline.
                auto * insert_ast = parsed_query->as<ASTInsertQuery>();
                if (insert_ast && isSyncInsertWithData(*insert_ast, client_context))
                {
                    this_query_end = insert_ast->end;
                    adjustQueryEnd(
                        this_query_end,
                        all_queries_end,
                        static_cast<unsigned>(client_context->getSettingsRef()[Setting::max_parser_depth]),
                        static_cast<unsigned>(client_context->getSettingsRef()[Setting::max_parser_backtracks]));
                }

                // Report error.
                if (have_error)
                    processError(full_query);

                // Stop processing queries if needed.
                if (have_error && !ignore_error)
                    return is_interactive;

                if (!need_retry)
                    this_query_begin = this_query_end;
                break;
            }
        }
    }
}


bool ClientBase::processQueryText(const String & text)
{
    auto trimmed_input = trim(text, [](char c) { return isWhitespaceASCII(c) || c == ';'; });

    if (exit_strings.end() != exit_strings.find(trimmed_input))
        return false;

    if (trimmed_input.starts_with("\\i"))
    {
        size_t skip_prefix_size = std::strlen("\\i");
        auto file_name = trim(
            trimmed_input.substr(skip_prefix_size, trimmed_input.size() - skip_prefix_size),
            [](char c) { return isWhitespaceASCII(c); });

        return processMultiQueryFromFile(file_name);
    }

    if (query_fuzzer_runs)
    {
        processWithFuzzing(text);
        return true;
    }

    return executeMultiQuery(text);
}


String ClientBase::prompt() const
{
    return prompt_by_server_display_name;
}


void ClientBase::initQueryIdFormats()
{
    if (!query_id_formats.empty())
        return;

    /// Initialize query_id_formats if any
    if (getClientConfiguration().has("query_id_formats"))
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        getClientConfiguration().keys("query_id_formats", keys);
        for (const auto & name : keys)
            query_id_formats.emplace_back(name + ":", getClientConfiguration().getString("query_id_formats." + name));
    }

    if (query_id_formats.empty())
        query_id_formats.emplace_back("Query id:", " {query_id}\n");
}


bool ClientBase::addMergeTreeSettings(ASTCreateQuery & ast_create)
{
    if (ast_create.attach
        || !ast_create.storage
        || !ast_create.storage->isExtendedStorageDefinition()
        || !ast_create.storage->engine
        || ast_create.storage->engine->name.find("MergeTree") == std::string::npos)
        return false;

    auto all_changed = cmd_merge_tree_settings.changes();
    if (all_changed.begin() == all_changed.end())
        return false;

    if (!ast_create.storage->settings)
    {
        auto settings_ast = std::make_shared<ASTSetQuery>();
        settings_ast->is_standalone = false;
        ast_create.storage->set(ast_create.storage->settings, settings_ast);
    }

    auto & storage_settings = *ast_create.storage->settings;
    bool added_new_setting = false;

    for (const auto & change : all_changed)
    {
        if (!storage_settings.changes.tryGet(change.name))
        {
            storage_settings.changes.emplace_back(change.name, change.value);
            added_new_setting = true;
        }
    }

    return added_new_setting;
}

void ClientBase::runInteractive()
{
    if (getClientConfiguration().has("query_id"))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "query_id could be specified only in non-interactive mode");
    if (getClientConfiguration().getBool("print-time-to-stderr", false))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "time option could be specified only in non-interactive mode");

    initQueryIdFormats();

    /// Initialize DateLUT here to avoid counting time spent here as query execution time.
    const auto local_tz = DateLUT::instance().getTimeZone();

    suggest.emplace();
    if (load_suggestions)
    {
        /// Load suggestion data from the server.
        if (client_context->getApplicationType() == Context::ApplicationType::CLIENT)
            suggest->load<Connection>(client_context, connection_parameters, getClientConfiguration().getInt("suggestion_limit"), wait_for_suggestions_to_load);
        else if (client_context->getApplicationType() == Context::ApplicationType::LOCAL)
            suggest->load<LocalConnection>(client_context, connection_parameters, getClientConfiguration().getInt("suggestion_limit"), wait_for_suggestions_to_load);
    }

    if (home_path.empty())
    {
        const char * home_path_cstr = getenv("HOME"); // NOLINT(concurrency-mt-unsafe)
        if (home_path_cstr)
            home_path = home_path_cstr;
    }

    /// Load command history if present.
    if (getClientConfiguration().has("history_file"))
        history_file = getClientConfiguration().getString("history_file");
    else
    {
        auto * history_file_from_env = getenv("CLICKHOUSE_HISTORY_FILE"); // NOLINT(concurrency-mt-unsafe)
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
            {
                error_stream << getCurrentExceptionMessage(false) << '\n';
            }
        }
    }

    LineReader::Patterns query_extenders = {"\\"};
    LineReader::Patterns query_delimiters = {";", "\\G", "\\G;"};
    char word_break_characters[] = " \t\v\f\a\b\r\n`~!@#$%^&*()-=+[{]}\\|;:'\",<.>/?";

#if USE_REPLXX
    replxx::Replxx::highlighter_callback_t highlight_callback{};
    if (getClientConfiguration().getBool("highlight", true))
        highlight_callback = highlight;

    ReplxxLineReader lr(
        *suggest,
        history_file,
        getClientConfiguration().has("multiline"),
        getClientConfiguration().getBool("ignore_shell_suspend", true),
        query_extenders,
        query_delimiters,
        word_break_characters,
        highlight_callback);
#else
    (void)word_break_characters;
    LineReader lr(
        history_file,
        getClientConfiguration().has("multiline"),
        query_extenders,
        query_delimiters);
#endif

    static const std::initializer_list<std::pair<String, String>> backslash_aliases =
        {
            { "\\l", "SHOW DATABASES" },
            { "\\d", "SHOW TABLES" },
            { "\\c", "USE" },
        };

    static const std::initializer_list<String> repeat_last_input_aliases =
        {
            ".",  /// Vim shortcut
            "/"   /// Oracle SQL Plus shortcut
        };

    String last_input;

    do
    {
        String input;
        {
            /// Enable bracketed-paste-mode so that we are able to paste multiline queries as a whole.
            /// But keep it disabled outside of query input, because it breaks password input
            /// (e.g. if we need to reconnect and show a password prompt).
            /// (Alternatively, we could make the password input ignore the control sequences.)
            lr.enableBracketedPaste();
            SCOPE_EXIT({ lr.disableBracketedPaste(); });

            input = lr.readLine(prompt(), ":-] ");
        }

        if (input.empty())
            break;

        has_vertical_output_suffix = false;
        if (input.ends_with("\\G") || input.ends_with("\\G;"))
        {
            if (input.ends_with("\\G"))
                input.resize(input.size() - 2);
            else if (input.ends_with("\\G;"))
                input.resize(input.size() - 3);

            has_vertical_output_suffix = true;
        }

        for (const auto & [alias, command] : backslash_aliases)
        {
            auto it = std::search(input.begin(), input.end(), alias.begin(), alias.end());
            if (it != input.end() && std::all_of(input.begin(), it, isWhitespaceASCII))
            {
                it += alias.size();
                if (it == input.end() || isWhitespaceASCII(*it))
                {
                    String new_input = command;
                    // append the rest of input to the command
                    // for parameters support, e.g. \c db_name -> USE db_name
                    new_input.append(it, input.end());
                    input = std::move(new_input);
                    break;
                }
            }
        }

        for (const auto & alias : repeat_last_input_aliases)
        {
            if (input == alias)
            {
                input  = last_input;
                break;
            }
        }

        if (suggest && suggest->getLastError() == ErrorCodes::USER_SESSION_LIMIT_EXCEEDED)
        {
            // If a separate connection loading suggestions failed to open a new session,
            // use the main session to receive them.
            suggest->load(*connection, connection_parameters.timeouts, getClientConfiguration().getInt("suggestion_limit"), client_context->getClientInfo());
        }

        try
        {
            if (!processQueryText(input))
                break;
            last_input = input;
        }
        catch (const Exception & e)
        {
            if (e.code() == ErrorCodes::USER_EXPIRED)
                break;

            /// We don't need to handle the test hints in the interactive mode.
            error_stream << "Exception on client:" << std::endl << getExceptionMessage(e, print_stack_trace, true) << std::endl << std::endl;
            client_exception.reset(e.clone());
        }

        if (client_exception)
        {
            /// client_exception may have been set above or elsewhere.
            /// Client-side exception during query execution can result in the loss of
            /// sync in the connection protocol.
            /// So we reconnect and allow to enter the next query.
            if (!connection->checkConnected(connection_parameters.timeouts))
                connect();
        }
    }
    while (true);

    if (isNewYearMode())
        output_stream << "Happy new year." << std::endl;
    else if (isChineseNewYearMode(local_tz))
        output_stream << "Happy Chinese new year. 春节快乐!" << std::endl;
    else
        output_stream << "Bye." << std::endl;
}


bool ClientBase::processMultiQueryFromFile(const String & file_name)
{
    String queries_from_file;

    ReadBufferFromFile in(file_name);
    readStringUntilEOF(queries_from_file, in);

    return executeMultiQuery(queries_from_file);
}


void ClientBase::runNonInteractive()
{
    if (delayed_interactive)
        initQueryIdFormats();

    if (!queries_files.empty())
    {
        for (const auto & queries_file : queries_files)
        {
            for (const auto & interleave_file : interleave_queries_files)
                if (!processMultiQueryFromFile(interleave_file))
                    return;

            if (!processMultiQueryFromFile(queries_file))
                return;
        }

        return;
    }

    if (!queries.empty())
    {
        for (const auto & query : queries)
        {
            if (query_fuzzer_runs)
            {
                if (!processWithFuzzing(query))
                    return;
            }
            else
            {
                if (!processQueryText(query))
                    return;
            }
        }
    }
    else
    {
        /// If 'query' parameter is not set, read a query from stdin.
        /// The query is read entirely into memory (streaming is disabled).
        ReadBufferFromFileDescriptor in(STDIN_FILENO);
        String text;
        readStringUntilEOF(text, in);
        if (query_fuzzer_runs)
            processWithFuzzing(text);
        else
            processQueryText(text);
    }
}


#if defined(FUZZING_MODE)
extern "C" int LLVMFuzzerRunDriver(int * argc, char *** argv, int (*callback)(const uint8_t * data, size_t size));
ClientBase * app;

void ClientBase::runLibFuzzer()
{
    app = this;
    std::vector<String> fuzzer_args_holder;

    if (const char * fuzzer_args_env = getenv("FUZZER_ARGS")) // NOLINT(concurrency-mt-unsafe)
        boost::split(fuzzer_args_holder, fuzzer_args_env, isWhitespaceASCII, boost::token_compress_on);

    std::vector<char *> fuzzer_args;
    fuzzer_args.push_back(argv0);
    for (auto & arg : fuzzer_args_holder)
        fuzzer_args.emplace_back(arg.data());

    int fuzzer_argc = static_cast<int>(fuzzer_args.size());
    char ** fuzzer_argv = fuzzer_args.data();

    LLVMFuzzerRunDriver(&fuzzer_argc, &fuzzer_argv, [](const uint8_t * data, size_t size)
    {
        try
        {
            String query(reinterpret_cast<const char *>(data), size);
            app->processQueryText(query);
        }
        catch (...)
        {
            return -1;
        }

        return 0;
    });
}
#else
void ClientBase::runLibFuzzer() {}
#endif

void ClientBase::clearTerminal()
{
    /// Move to the beginning of the line
    /// and clear until end of screen.
    /// It is needed if garbage is left in terminal.
    /// Show cursor. It can be left hidden by invocation of previous programs.
    /// A test for this feature: perl -e 'print "x"x100000'; echo -ne '\033[0;0H\033[?25l'; clickhouse-client
    output_stream << "\r" "\033[0J" "\033[?25h";
}

void ClientBase::showClientVersion()
{
    output_stream << VERSION_NAME << " " + getName() + " version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
}

}
