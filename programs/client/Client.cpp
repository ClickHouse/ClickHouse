#include "ConnectionParameters.h"
#include "QueryFuzzer.h"
#include "Suggest.h"
#include "TestHint.h"

#if USE_REPLXX
#   include <common/ReplxxLineReader.h>
#elif defined(USE_READLINE) && USE_READLINE
#   include <common/ReadlineLineReader.h>
#else
#   include <common/LineReader.h>
#endif

#include <stdlib.h>
#include <fcntl.h>
#include <signal.h>
#include <map>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <unordered_set>
#include <algorithm>
#include <optional>
#include <common/scope_guard_safe.h>
#include <boost/program_options.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <Poco/String.h>
#include <Poco/Util/Application.h>
#include <common/find_symbols.h>
#include <common/LineReader.h>
#include <Common/ClickHouseRevision.h>
#include <Common/Exception.h>
#include <Common/ShellCommand.h>
#include <Common/UnicodeBar.h>
#include <Common/formatReadable.h>
#include <Common/NetException.h>
#include <Common/Throttler.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/clearPasswordFromCommandLine.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/PODArray.h>
#include <Core/Types.h>
#include <Core/QueryProcessingStage.h>
#include <Core/ExternalTable.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFileDescriptor.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <IO/UseSSL.h>
#include <IO/WriteBufferFromOStream.h>
#include <DataStreams/AsynchronousBlockInputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/InternalTextLogsRowOutputStream.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTUseQuery.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/formatAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterSetQuery.h>
#include <Interpreters/ReplaceQueryParameterVisitor.h>
#include <Client/Connection.h>
#include <Common/InterruptListener.h>
#include <Functions/registerFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Formats/registerFormats.h>
#include <Common/Config/configReadClient.h>
#include <Storages/ColumnsDescription.h>
#include <common/argsToConfig.h>
#include <Common/TerminalSize.h>
#include <Common/UTF8Helpers.h>
#include <Common/ProgressIndication.h>
#include <filesystem>
#include <Common/filesystemHelpers.h>

#if !defined(ARCADIA_BUILD)
#    include <Common/config_version.h>
#endif

#ifndef __clang__
#pragma GCC optimize("-fno-var-tracking-assignments")
#endif

namespace fs = std::filesystem;

namespace DB
{
namespace ErrorCodes
{
    extern const int NETWORK_ERROR;
    extern const int NO_DATA_TO_INSERT;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_PACKET_FROM_SERVER;
    extern const int UNEXPECTED_PACKET_FROM_SERVER;
    extern const int CLIENT_OUTPUT_FORMAT_SPECIFIED;
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int DEADLOCK_AVOIDED;
    extern const int UNRECOGNIZED_ARGUMENTS;
    extern const int SYNTAX_ERROR;
    extern const int TOO_DEEP_RECURSION;
}


static bool queryHasWithClause(const IAST * ast)
{
    if (const auto * select = dynamic_cast<const ASTSelectQuery *>(ast); select && select->with())
    {
        return true;
    }

    // This full recursive walk is somewhat excessive, because most of the
    // children are not queries, but on the other hand it will let us to avoid
    // breakage when the AST structure changes and some new variant of query
    // nesting is added. This function is used in fuzzer, so it's better to be
    // defensive and avoid weird unexpected errors.
    // clang-tidy is confused by this function: it thinks that if `select` is
    // nullptr, `ast` is also nullptr, and complains about nullptr dereference.
    // NOLINTNEXTLINE
    for (const auto & child : ast->children)
    {
        if (queryHasWithClause(child.get()))
        {
            return true;
        }
    }

    return false;
}


class Client : public Poco::Util::Application
{
public:
    Client() = default;

private:
    using StringSet = std::unordered_set<String>;
    StringSet exit_strings{"exit", "quit", "logout", "учше", "йгше", "дщпщге", "exit;", "quit;", "logout;", "учшеж",
                           "йгшеж", "дщпщгеж", "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"};
    bool is_interactive = true; /// Use either interactive line editing interface or batch mode.
    bool echo_queries = false; /// Print queries before execution in batch mode.
    bool ignore_error
        = false; /// In case of errors, don't print error message, continue to next query. Only applicable for non-interactive mode.
    bool print_time_to_stderr = false; /// Output execution time to stderr in batch mode.
    bool stdin_is_a_tty = false; /// stdin is a terminal.
    bool stdout_is_a_tty = false; /// stdout is a terminal.

    /// If not empty, queries will be read from these files
    std::vector<std::string> queries_files;
    /// If not empty, run queries from these files before processing every file from 'queries_files'.
    std::vector<std::string> interleave_queries_files;

    std::unique_ptr<Connection> connection; /// Connection to DB.
    String full_query; /// Current query as it was given to the client.

    // Current query as it will be sent to the server. It may differ from the
    // full query for INSERT queries, for which the data that follows the query
    // is stripped and sent separately.
    String query_to_send;

    String format; /// Query results output format.
    bool is_default_format = true; /// false, if format is set in the config or command line.
    size_t format_max_block_size = 0; /// Max block size for console output.
    String insert_format; /// Format of INSERT data that is read from stdin in batch mode.
    size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.
    size_t max_client_network_bandwidth = 0; /// The maximum speed of data exchange over the network for the client in bytes per second.

    bool has_vertical_output_suffix = false; /// Is \G present at the end of the query string?

    SharedContextHolder shared_context = Context::createShared();
    ContextMutablePtr context = Context::createGlobal(shared_context.get());

    /// Buffer that reads from stdin in batch mode.
    ReadBufferFromFileDescriptor std_in{STDIN_FILENO};

    /// Console output.
    WriteBufferFromFileDescriptor std_out{STDOUT_FILENO};
    std::unique_ptr<ShellCommand> pager_cmd;

    /// The user can specify to redirect query output to a file.
    std::optional<WriteBufferFromFile> out_file_buf;
    BlockOutputStreamPtr block_out_stream;

    /// The user could specify special file for server logs (stderr by default)
    std::unique_ptr<WriteBuffer> out_logs_buf;
    String server_logs_file;
    BlockOutputStreamPtr logs_out_stream;

    String home_path;

    String current_profile;

    String prompt_by_server_display_name;

    /// Path to a file containing command history.
    String history_file;

    /// How many rows have been read or written.
    size_t processed_rows = 0;

    /// Parsed query. Is used to determine some settings (e.g. format, output file).
    ASTPtr parsed_query;

    /// The last exception that was received from the server. Is used for the
    /// return code in batch mode.
    std::unique_ptr<Exception> server_exception;
    /// Likewise, the last exception that occurred on the client.
    std::unique_ptr<Exception> client_exception;

    /// If the last query resulted in exception. `server_exception` or
    /// `client_exception` must be set.
    bool have_error = false;

    UInt64 server_revision = 0;
    String server_version;
    String server_display_name;

    /// true by default - for interactive mode, might be changed when --progress option is checked for
    /// non-interactive mode.
    bool need_render_progress = true;

    bool written_first_block = false;

    ProgressIndication progress_indication;

    /// External tables info.
    std::list<ExternalTable> external_tables;

    /// Dictionary with query parameters for prepared statements.
    NameToNameMap query_parameters;

    ConnectionParameters connection_parameters;

    QueryFuzzer fuzzer;
    int query_fuzzer_runs = 0;

    std::optional<Suggest> suggest;

    /// We will format query_id in interactive mode in various ways, the default is just to print Query id: ...
    std::vector<std::pair<String, String>> query_id_formats;
    QueryProcessingStage::Enum query_processing_stage;

    void initialize(Poco::Util::Application & self) override
    {
        Poco::Util::Application::initialize(self);

        const char * home_path_cstr = getenv("HOME");
        if (home_path_cstr)
            home_path = home_path_cstr;

        configReadClient(config(), home_path);

        context->setApplicationType(Context::ApplicationType::CLIENT);
        context->setQueryParameters(query_parameters);

        /// settings and limits could be specified in config file, but passed settings has higher priority
        for (const auto & setting : context->getSettingsRef().allUnchanged())
        {
            const auto & name = setting.getName();
            if (config().has(name))
                context->setSetting(name, config().getString(name));
        }

        /// Set path for format schema files
        if (config().has("format_schema_path"))
            context->setFormatSchemaPath(fs::weakly_canonical(config().getString("format_schema_path")));

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
    }


    int main(const std::vector<std::string> & /*args*/) override
    {
        try
        {
            return mainImpl();
        }
        catch (const Exception & e)
        {
            bool print_stack_trace = config().getBool("stacktrace", false);

            std::string text = e.displayText();

            /** If exception is received from server, then stack trace is embedded in message.
              * If exception is thrown on client, then stack trace is in separate field.
              */

            auto embedded_stack_trace_pos = text.find("Stack trace");
            if (std::string::npos != embedded_stack_trace_pos && !print_stack_trace)
                text.resize(embedded_stack_trace_pos);

            std::cerr << "Code: " << e.code() << ". " << text << std::endl << std::endl;

            /// Don't print the stack trace on the client if it was logged on the server.
            /// Also don't print the stack trace in case of network errors.
            if (print_stack_trace && e.code() != ErrorCodes::NETWORK_ERROR && std::string::npos == embedded_stack_trace_pos)
            {
                std::cerr << "Stack trace:" << std::endl << e.getStackTraceString();
            }

            /// If exception code isn't zero, we should return non-zero return code anyway.
            return e.code() ? e.code() : -1;
        }
        catch (...)
        {
            std::cerr << getCurrentExceptionMessage(false) << std::endl;
            return getCurrentExceptionCode();
        }
    }

    /// Should we celebrate a bit?
    static bool isNewYearMode()
    {
        time_t current_time = time(nullptr);

        /// It's bad to be intrusive.
        if (current_time % 3 != 0)
            return false;

        LocalDate now(current_time);
        return (now.month() == 12 && now.day() >= 20) || (now.month() == 1 && now.day() <= 5);
    }

    static bool isChineseNewYearMode(const String & local_tz)
    {
        /// Days of Dec. 20 in Chinese calendar starting from year 2019 to year 2105
        static constexpr UInt16 chineseNewYearIndicators[]
            = {18275, 18659, 19014, 19368, 19752, 20107, 20491, 20845, 21199, 21583, 21937, 22292, 22676, 23030, 23414, 23768, 24122, 24506,
               24860, 25215, 25599, 25954, 26308, 26692, 27046, 27430, 27784, 28138, 28522, 28877, 29232, 29616, 29970, 30354, 30708, 31062,
               31446, 31800, 32155, 32539, 32894, 33248, 33632, 33986, 34369, 34724, 35078, 35462, 35817, 36171, 36555, 36909, 37293, 37647,
               38002, 38386, 38740, 39095, 39479, 39833, 40187, 40571, 40925, 41309, 41664, 42018, 42402, 42757, 43111, 43495, 43849, 44233,
               44587, 44942, 45326, 45680, 46035, 46418, 46772, 47126, 47510, 47865, 48249, 48604, 48958, 49342};

        /// All time zone names are acquired from https://www.iana.org/time-zones
        static constexpr const char * chineseNewYearTimeZoneIndicators[] = {
            /// Time zones celebrating Chinese new year.
            "Asia/Shanghai",
            "Asia/Chongqing",
            "Asia/Harbin",
            "Asia/Urumqi",
            "Asia/Hong_Kong",
            "Asia/Chungking",
            "Asia/Macao",
            "Asia/Macau",
            "Asia/Taipei",
            "Asia/Singapore",

            /// Time zones celebrating Chinese new year but with different festival names. Let's not print the message for now.
            // "Asia/Brunei",
            // "Asia/Ho_Chi_Minh",
            // "Asia/Hovd",
            // "Asia/Jakarta",
            // "Asia/Jayapura",
            // "Asia/Kashgar",
            // "Asia/Kuala_Lumpur",
            // "Asia/Kuching",
            // "Asia/Makassar",
            // "Asia/Pontianak",
            // "Asia/Pyongyang",
            // "Asia/Saigon",
            // "Asia/Seoul",
            // "Asia/Ujung_Pandang",
            // "Asia/Ulaanbaatar",
            // "Asia/Ulan_Bator",
        };
        static constexpr size_t M = sizeof(chineseNewYearTimeZoneIndicators) / sizeof(chineseNewYearTimeZoneIndicators[0]);

        time_t current_time = time(nullptr);

        if (chineseNewYearTimeZoneIndicators + M
            == std::find_if(chineseNewYearTimeZoneIndicators, chineseNewYearTimeZoneIndicators + M, [&local_tz](const char * tz)
                            {
                                return tz == local_tz;
                            }))
            return false;

        /// It's bad to be intrusive.
        if (current_time % 3 != 0)
            return false;

        auto days = DateLUT::instance().toDayNum(current_time).toUnderType();
        for (auto d : chineseNewYearIndicators)
        {
            /// Let's celebrate until Lantern Festival
            if (d <= days && d + 25 >= days)
                return true;
            else if (d > days)
                return false;
        }
        return false;
    }

#if USE_REPLXX
    static void highlight(const String & query, std::vector<replxx::Replxx::Color> & colors)
    {
        using namespace replxx;

        static const std::unordered_map<TokenType, Replxx::Color> token_to_color
            = {{TokenType::Whitespace, Replxx::Color::DEFAULT},
               {TokenType::Comment, Replxx::Color::GRAY},
               {TokenType::BareWord, Replxx::Color::DEFAULT},
               {TokenType::Number, Replxx::Color::GREEN},
               {TokenType::StringLiteral, Replxx::Color::CYAN},
               {TokenType::QuotedIdentifier, Replxx::Color::MAGENTA},
               {TokenType::OpeningRoundBracket, Replxx::Color::BROWN},
               {TokenType::ClosingRoundBracket, Replxx::Color::BROWN},
               {TokenType::OpeningSquareBracket, Replxx::Color::BROWN},
               {TokenType::ClosingSquareBracket, Replxx::Color::BROWN},
               {TokenType::OpeningCurlyBrace, Replxx::Color::INTENSE},
               {TokenType::ClosingCurlyBrace, Replxx::Color::INTENSE},

               {TokenType::Comma, Replxx::Color::INTENSE},
               {TokenType::Semicolon, Replxx::Color::INTENSE},
               {TokenType::Dot, Replxx::Color::INTENSE},
               {TokenType::Asterisk, Replxx::Color::INTENSE},
               {TokenType::Plus, Replxx::Color::INTENSE},
               {TokenType::Minus, Replxx::Color::INTENSE},
               {TokenType::Slash, Replxx::Color::INTENSE},
               {TokenType::Percent, Replxx::Color::INTENSE},
               {TokenType::Arrow, Replxx::Color::INTENSE},
               {TokenType::QuestionMark, Replxx::Color::INTENSE},
               {TokenType::Colon, Replxx::Color::INTENSE},
               {TokenType::Equals, Replxx::Color::INTENSE},
               {TokenType::NotEquals, Replxx::Color::INTENSE},
               {TokenType::Less, Replxx::Color::INTENSE},
               {TokenType::Greater, Replxx::Color::INTENSE},
               {TokenType::LessOrEquals, Replxx::Color::INTENSE},
               {TokenType::GreaterOrEquals, Replxx::Color::INTENSE},
               {TokenType::Concatenation, Replxx::Color::INTENSE},
               {TokenType::At, Replxx::Color::INTENSE},
               {TokenType::DoubleAt, Replxx::Color::MAGENTA},

               {TokenType::EndOfStream, Replxx::Color::DEFAULT},

               {TokenType::Error, Replxx::Color::RED},
               {TokenType::ErrorMultilineCommentIsNotClosed, Replxx::Color::RED},
               {TokenType::ErrorSingleQuoteIsNotClosed, Replxx::Color::RED},
               {TokenType::ErrorDoubleQuoteIsNotClosed, Replxx::Color::RED},
               {TokenType::ErrorSinglePipeMark, Replxx::Color::RED},
               {TokenType::ErrorWrongNumber, Replxx::Color::RED},
               { TokenType::ErrorMaxQuerySizeExceeded,
                 Replxx::Color::RED }};

        const Replxx::Color unknown_token_color = Replxx::Color::RED;

        Lexer lexer(query.data(), query.data() + query.size());
        size_t pos = 0;

        for (Token token = lexer.nextToken(); !token.isEnd(); token = lexer.nextToken())
        {
            size_t utf8_len = UTF8::countCodePoints(reinterpret_cast<const UInt8 *>(token.begin), token.size());
            for (size_t code_point_index = 0; code_point_index < utf8_len; ++code_point_index)
            {
                if (token_to_color.find(token.type) != token_to_color.end())
                    colors[pos + code_point_index] = token_to_color.at(token.type);
                else
                    colors[pos + code_point_index] = unknown_token_color;
            }

            pos += utf8_len;
        }
    }
#endif

    int mainImpl()
    {
        UseSSL use_ssl;

        registerFormats();
        registerFunctions();
        registerAggregateFunctions();

        /// Batch mode is enabled if one of the following is true:
        /// - -e (--query) command line option is present.
        ///   The value of the option is used as the text of query (or of multiple queries).
        ///   If stdin is not a terminal, INSERT data for the first query is read from it.
        /// - stdin is not a terminal. In this case queries are read from it.
        /// - -qf (--queries-file) command line option is present.
        ///   The value of the option is used as file with query (or of multiple queries) to execute.
        if (!stdin_is_a_tty || config().has("query") || !queries_files.empty())
            is_interactive = false;

        if (config().has("query") && !queries_files.empty())
        {
            throw Exception("Specify either `query` or `queries-file` option", ErrorCodes::BAD_ARGUMENTS);
        }

        std::cout << std::fixed << std::setprecision(3);
        std::cerr << std::fixed << std::setprecision(3);

        if (is_interactive)
        {
            clearTerminal();
            showClientVersion();
        }

        is_default_format = !config().has("vertical") && !config().has("format");
        if (config().has("vertical"))
            format = config().getString("format", "Vertical");
        else
            format = config().getString("format", is_interactive ? "PrettyCompact" : "TabSeparated");

        format_max_block_size = config().getInt("format_max_block_size", context->getSettingsRef().max_block_size);

        insert_format = "Values";

        /// Setting value from cmd arg overrides one from config
        if (context->getSettingsRef().max_insert_block_size.changed)
            insert_format_max_block_size = context->getSettingsRef().max_insert_block_size;
        else
            insert_format_max_block_size = config().getInt("insert_format_max_block_size", context->getSettingsRef().max_insert_block_size);

        if (!is_interactive)
        {
            need_render_progress = config().getBool("progress", false);
            echo_queries = config().getBool("echo", false);
            ignore_error = config().getBool("ignore-error", false);
        }

        ClientInfo & client_info = context->getClientInfo();
        client_info.setInitialQuery();
        client_info.quota_key = config().getString("quota_key", "");

        connect();

        /// Initialize DateLUT here to avoid counting time spent here as query execution time.
        const auto local_tz = DateLUT::instance().getTimeZone();

        if (is_interactive)
        {
            if (config().has("query_id"))
                throw Exception("query_id could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);
            if (print_time_to_stderr)
                throw Exception("time option could be specified only in non-interactive mode", ErrorCodes::BAD_ARGUMENTS);

            suggest.emplace();
            if (server_revision >= Suggest::MIN_SERVER_REVISION && !config().getBool("disable_suggestion", false))
            {
                /// Load suggestion data from the server.
                suggest->load(connection_parameters, config().getInt("suggestion_limit"));
            }

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
            if (config().getBool("highlight"))
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
                    // We don't need to handle the test hints in the interactive
                    // mode.
                    std::cerr << std::endl
                              << "Exception on client:" << std::endl
                              << "Code: " << e.code() << ". " << e.displayText() << std::endl;

                    if (config().getBool("stacktrace", false))
                        std::cerr << "Stack trace:" << std::endl << e.getStackTraceString() << std::endl;

                    std::cerr << std::endl;

                    client_exception = std::make_unique<Exception>(e);
                }

                if (client_exception)
                {
                    /// client_exception may have been set above or elsewhere.
                    /// Client-side exception during query execution can result in the loss of
                    /// sync in the connection protocol.
                    /// So we reconnect and allow to enter the next query.
                    connect();
                }
            } while (true);

            if (isNewYearMode())
                std::cout << "Happy new year." << std::endl;
            else if (isChineseNewYearMode(local_tz))
                std::cout << "Happy Chinese new year. 春节快乐!" << std::endl;
            else
                std::cout << "Bye." << std::endl;
            return 0;
        }
        else
        {
            auto query_id = config().getString("query_id", "");
            if (!query_id.empty())
                context->setCurrentQueryId(query_id);

            nonInteractive();

            // If exception code isn't zero, we should return non-zero return
            // code anyway.
            const auto * exception = server_exception ? server_exception.get() : client_exception.get();
            if (exception)
            {
                return exception->code() != 0 ? exception->code() : -1;
            }
            if (have_error)
            {
                // Shouldn't be set without an exception, but check it just in
                // case so that at least we don't lose an error.
                return -1;
            }

            return 0;
        }
    }


    void connect()
    {
        connection_parameters = ConnectionParameters(config());

        if (is_interactive)
            std::cout << "Connecting to "
                      << (!connection_parameters.default_database.empty() ? "database " + connection_parameters.default_database + " at "
                                                                          : "")
                      << connection_parameters.host << ":" << connection_parameters.port
                      << (!connection_parameters.user.empty() ? " as user " + connection_parameters.user : "") << "." << std::endl;

        connection = std::make_unique<Connection>(
            connection_parameters.host,
            connection_parameters.port,
            connection_parameters.default_database,
            connection_parameters.user,
            connection_parameters.password,
            "", /* cluster */
            "", /* cluster_secret */
            "client",
            connection_parameters.compression,
            connection_parameters.security);

        String server_name;
        UInt64 server_version_major = 0;
        UInt64 server_version_minor = 0;
        UInt64 server_version_patch = 0;

        if (max_client_network_bandwidth)
        {
            ThrottlerPtr throttler = std::make_shared<Throttler>(max_client_network_bandwidth, 0, "");
            connection->setThrottler(throttler);
        }

        connection->getServerVersion(
            connection_parameters.timeouts, server_name, server_version_major, server_version_minor, server_version_patch, server_revision);

        server_version = toString(server_version_major) + "." + toString(server_version_minor) + "." + toString(server_version_patch);

        if (server_display_name = connection->getServerDisplayName(connection_parameters.timeouts); server_display_name.empty())
        {
            server_display_name = config().getString("host", "localhost");
        }

        if (is_interactive)
        {
            std::cout << "Connected to " << server_name << " server version " << server_version << " revision " << server_revision << "."
                      << std::endl
                      << std::endl;

            auto client_version_tuple = std::make_tuple(VERSION_MAJOR, VERSION_MINOR, VERSION_PATCH);
            auto server_version_tuple = std::make_tuple(server_version_major, server_version_minor, server_version_patch);

            if (client_version_tuple < server_version_tuple)
            {
                std::cout << "ClickHouse client version is older than ClickHouse server. "
                          << "It may lack support for new features." << std::endl
                          << std::endl;
            }
            else if (client_version_tuple > server_version_tuple)
            {
                std::cout << "ClickHouse server version is older than ClickHouse client. "
                          << "It may indicate that the server is out of date and can be upgraded." << std::endl
                          << std::endl;
            }
        }

        if (!context->getSettingsRef().use_client_time_zone)
        {
            const auto & time_zone = connection->getServerTimezone(connection_parameters.timeouts);
            if (!time_zone.empty())
            {
                try
                {
                    DateLUT::setDefaultTimezone(time_zone);
                }
                catch (...)
                {
                    std::cerr << "Warning: could not switch to server time zone: " << time_zone
                              << ", reason: " << getCurrentExceptionMessage(/* with_stacktrace = */ false) << std::endl
                              << "Proceeding with local time zone." << std::endl
                              << std::endl;
                }
            }
            else
            {
                std::cerr << "Warning: could not determine server time zone. "
                          << "Proceeding with local time zone." << std::endl
                          << std::endl;
            }
        }

        Strings keys;

        prompt_by_server_display_name = config().getRawString("prompt_by_server_display_name.default", "{display_name} :) ");

        config().keys("prompt_by_server_display_name", keys);

        for (const String & key : keys)
        {
            if (key != "default" && server_display_name.find(key) != std::string::npos)
            {
                prompt_by_server_display_name = config().getRawString("prompt_by_server_display_name." + key);
                break;
            }
        }

        /// Prompt may contain escape sequences including \e[ or \x1b[ sequences to set terminal color.
        {
            String unescaped_prompt_by_server_display_name;
            ReadBufferFromString in(prompt_by_server_display_name);
            readEscapedString(unescaped_prompt_by_server_display_name, in);
            prompt_by_server_display_name = std::move(unescaped_prompt_by_server_display_name);
        }

        /// Prompt may contain the following substitutions in a form of {name}.
        std::map<String, String> prompt_substitutions{
            {"host", connection_parameters.host},
            {"port", toString(connection_parameters.port)},
            {"user", connection_parameters.user},
            {"display_name", server_display_name},
        };

        /// Quite suboptimal.
        for (const auto & [key, value] : prompt_substitutions)
            boost::replace_all(prompt_by_server_display_name, "{" + key + "}", value);
    }


    inline String prompt() const
    {
        return boost::replace_all_copy(prompt_by_server_display_name, "{database}", config().getString("database", "default"));
    }


    void nonInteractive()
    {
        String text;

        if (!queries_files.empty())
        {
            auto process_file = [&](const std::string & file)
            {
                connection->setDefaultDatabase(connection_parameters.default_database);
                ReadBufferFromFile in(file);
                readStringUntilEOF(text, in);
                return processMultiQuery(text);
            };

            for (const auto & queries_file : queries_files)
            {
                for (const auto & interleave_file : interleave_queries_files)
                    if (!process_file(interleave_file))
                        return;

                if (!process_file(queries_file))
                    return;
            }
            return;
        }
        else if (config().has("query"))
        {
            text = config().getRawString("query"); /// Poco configuration should not process substitutions in form of ${...} inside query.
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

    bool processQueryText(const String & text)
    {
        if (exit_strings.end() != exit_strings.find(trim(text, [](char c) { return isWhitespaceASCII(c) || c == ';'; })))
            return false;

        if (!config().has("multiquery"))
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

        return processMultiQuery(text);
    }

    // Consumes trailing semicolons and tries to consume the same-line trailing
    // comment.
    static void adjustQueryEnd(const char *& this_query_end, const char * all_queries_end, int max_parser_depth)
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

    void reportQueryError() const
    {
        if (server_exception)
        {
            std::string text = server_exception->displayText();
            auto embedded_stack_trace_pos = text.find("Stack trace");
            if (std::string::npos != embedded_stack_trace_pos && !config().getBool("stacktrace", false))
            {
                text.resize(embedded_stack_trace_pos);
            }
            std::cerr << "Received exception from server (version " << server_version << "):" << std::endl
                      << "Code: " << server_exception->code() << ". " << text << std::endl;
            if (is_interactive)
            {
                std::cerr << std::endl;
            }
        }

        if (client_exception)
        {
            fmt::print(stderr, "Error on processing query '{}':\n{}\n", full_query, client_exception->message());
            if (is_interactive)
            {
                fmt::print(stderr, "\n");
            }
        }

        // A debug check -- at least some exception must be set, if the error
        // flag is set, and vice versa.
        assert(have_error == (client_exception || server_exception));
    }

    bool processMultiQuery(const String & all_queries_text)
    {
        // It makes sense not to base any control flow on this, so that it is
        // the same in tests and in normal usage. The only difference is that in
        // normal mode we ignore the test hints.
        const bool test_mode = config().has("testmode");

        {
            /// disable logs if expects errors
            TestHint test_hint(test_mode, all_queries_text);
            if (test_hint.clientError() || test_hint.serverError())
                processTextAsSingleQuery("SET send_logs_level = 'fatal'");
        }

        bool echo_query = echo_queries;

        /// Several queries separated by ';'.
        /// INSERT data is ended by the end of line, not ';'.
        /// An exception is VALUES format where we also support semicolon in
        /// addition to end of line.

        const char * this_query_begin = all_queries_text.data();
        const char * all_queries_end = all_queries_text.data() + all_queries_text.size();

        while (this_query_begin < all_queries_end)
        {
            // Remove leading empty newlines and other whitespace, because they
            // are annoying to filter in query log. This is mostly relevant for
            // the tests.
            while (this_query_begin < all_queries_end && isWhitespaceASCII(*this_query_begin))
            {
                ++this_query_begin;
            }
            if (this_query_begin >= all_queries_end)
            {
                break;
            }

            // If there are only comments left until the end of file, we just
            // stop. The parser can't handle this situation because it always
            // expects that there is some query that it can parse.
            // We can get into this situation because the parser also doesn't
            // skip the trailing comments after parsing a query. This is because
            // they may as well be the leading comments for the next query,
            // and it makes more sense to treat them as such.
            {
                Tokens tokens(this_query_begin, all_queries_end);
                IParser::Pos token_iterator(tokens, context->getSettingsRef().max_parser_depth);
                if (!token_iterator.isValid())
                {
                    break;
                }
            }

            // Try to parse the query.
            const char * this_query_end = this_query_begin;
            try
            {
                parsed_query = parseQuery(this_query_end, all_queries_end, true);
            }
            catch (Exception & e)
            {
                // Try to find test hint for syntax error. We don't know where
                // the query ends because we failed to parse it, so we consume
                // the entire line.
                this_query_end = find_first_symbols<'\n'>(this_query_end, all_queries_end);

                TestHint hint(test_mode, String(this_query_begin, this_query_end - this_query_begin));

                if (hint.serverError())
                {
                    // Syntax errors are considered as client errors
                    e.addMessage("\nExpected server error '{}'.", hint.serverError());
                    throw;
                }

                if (hint.clientError() != e.code())
                {
                    if (hint.clientError())
                        e.addMessage("\nExpected client error: " + std::to_string(hint.clientError()));
                    throw;
                }

                /// It's expected syntax error, skip the line
                this_query_begin = this_query_end;
                continue;
            }

            if (!parsed_query)
            {
                if (ignore_error)
                {
                    Tokens tokens(this_query_begin, all_queries_end);
                    IParser::Pos token_iterator(tokens, context->getSettingsRef().max_parser_depth);
                    while (token_iterator->type != TokenType::Semicolon && token_iterator.isValid())
                        ++token_iterator;
                    this_query_begin = token_iterator->end;

                    continue;
                }

                return true;
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
                query_to_send = all_queries_text.substr(this_query_begin - all_queries_text.data(), insert_ast->data - this_query_begin);
            }
            else
            {
                query_to_send = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);
            }

            // Try to include the trailing comment with test hints. It is just
            // a guess for now, because we don't yet know where the query ends
            // if it is an INSERT query with inline data. We will do it again
            // after we have processed the query. But even this guess is
            // beneficial so that we see proper trailing comments in "echo" and
            // server log.
            adjustQueryEnd(this_query_end, all_queries_end, context->getSettingsRef().max_parser_depth);

            // full_query is the query + inline INSERT data + trailing comments
            // (the latter is our best guess for now).
            full_query = all_queries_text.substr(this_query_begin - all_queries_text.data(), this_query_end - this_query_begin);

            if (query_fuzzer_runs)
            {
                if (!processWithFuzzing(full_query))
                    return false;

                this_query_begin = this_query_end;
                continue;
            }

            // Now we know for sure where the query ends.
            // Look for the hint in the text of query + insert data + trailing
            // comments,
            // e.g. insert into t format CSV 'a' -- { serverError 123 }.
            // Use the updated query boundaries we just calculated.
            TestHint test_hint(test_mode, std::string(this_query_begin, this_query_end - this_query_begin));

            // Echo all queries if asked; makes for a more readable reference
            // file.
            echo_query = test_hint.echoQueries().value_or(echo_query);

            try
            {
                processParsedSingleQuery(echo_query);
            }
            catch (...)
            {
                // Surprisingly, this is a client error. A server error would
                // have been reported w/o throwing (see onReceiveSeverException()).
                client_exception = std::make_unique<Exception>(getCurrentExceptionMessage(true), getCurrentExceptionCode());
                have_error = true;
            }

            // For INSERTs with inline data: use the end of inline data as
            // reported by the format parser (it is saved in sendData()).
            // This allows us to handle queries like:
            //   insert into t values (1); select 1
            // , where the inline data is delimited by semicolon and not by a
            // newline.
            if (insert_ast && insert_ast->data)
            {
                this_query_end = insert_ast->end;
                adjustQueryEnd(this_query_end, all_queries_end, context->getSettingsRef().max_parser_depth);
            }

            // Check whether the error (or its absence) matches the test hints
            // (or their absence).
            bool error_matches_hint = true;
            if (have_error)
            {
                if (test_hint.serverError())
                {
                    if (!server_exception)
                    {
                        error_matches_hint = false;
                        fmt::print(stderr, "Expected server error code '{}' but got no server error.\n", test_hint.serverError());
                    }
                    else if (server_exception->code() != test_hint.serverError())
                    {
                        error_matches_hint = false;
                        std::cerr << "Expected server error code: " << test_hint.serverError() << " but got: " << server_exception->code()
                                  << "." << std::endl;
                    }
                }

                if (test_hint.clientError())
                {
                    if (!client_exception)
                    {
                        error_matches_hint = false;
                        fmt::print(stderr, "Expected client error code '{}' but got no client error.\n", test_hint.clientError());
                    }
                    else if (client_exception->code() != test_hint.clientError())
                    {
                        error_matches_hint = false;
                        fmt::print(
                            stderr, "Expected client error code '{}' but got '{}'.\n", test_hint.clientError(), client_exception->code());
                    }
                }

                if (!test_hint.clientError() && !test_hint.serverError())
                {
                    // No error was expected but it still occurred. This is the
                    // default case w/o test hint, doesn't need additional
                    // diagnostics.
                    error_matches_hint = false;
                }
            }
            else
            {
                if (test_hint.clientError())
                {
                    fmt::print(stderr, "The query succeeded but the client error '{}' was expected.\n", test_hint.clientError());
                    error_matches_hint = false;
                }

                if (test_hint.serverError())
                {
                    fmt::print(stderr, "The query succeeded but the server error '{}' was expected.\n", test_hint.serverError());
                    error_matches_hint = false;
                }
            }

            // If the error is expected, force reconnect and ignore it.
            if (have_error && error_matches_hint)
            {
                client_exception.reset();
                server_exception.reset();
                have_error = false;

                if (!connection->checkConnected())
                    connect();
            }

            // Report error.
            if (have_error)
            {
                reportQueryError();
            }

            // Stop processing queries if needed.
            if (have_error && !ignore_error)
            {
                if (is_interactive)
                {
                    break;
                }
                else
                {
                    return false;
                }
            }

            this_query_begin = this_query_end;
        }

        return true;
    }

    // Prints changed settings to stderr. Useful for debugging fuzzing failures.
    void printChangedSettings() const
    {
        const auto & changes = context->getSettingsRef().changes();
        if (!changes.empty())
        {
            fmt::print(stderr, "Changed settings: ");
            for (size_t i = 0; i < changes.size(); ++i)
            {
                if (i)
                {
                    fmt::print(stderr, ", ");
                }
                fmt::print(stderr, "{} = '{}'", changes[i].name, toString(changes[i].value));
            }
            fmt::print(stderr, "\n");
        }
        else
        {
            fmt::print(stderr, "No changed settings.\n");
        }
    }

    /// Returns false when server is not available.
    bool processWithFuzzing(const String & text)
    {
        ASTPtr orig_ast;

        try
        {
            const char * begin = text.data();
            orig_ast = parseQuery(begin, begin + text.size(), true);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::SYNTAX_ERROR &&
                e.code() != ErrorCodes::TOO_DEEP_RECURSION)
                throw;
        }

        if (!orig_ast)
        {
            // Can't continue after a parsing error
            return true;
        }

        // Don't repeat inserts, the tables grow too big. Also don't repeat
        // creates because first we run the unmodified query, it will succeed,
        // and the subsequent queries will fail. When we run out of fuzzer
        // errors, it may be interesting to add fuzzing of create queries that
        // wraps columns into LowCardinality or Nullable. Also there are other
        // kinds of create queries such as CREATE DICTIONARY, we could fuzz
        // them as well. Also there is no point fuzzing DROP queries.
        size_t this_query_runs = query_fuzzer_runs;
        if (orig_ast->as<ASTInsertQuery>() || orig_ast->as<ASTCreateQuery>() || orig_ast->as<ASTDropQuery>())
        {
            this_query_runs = 1;
        }

        ASTPtr fuzz_base = orig_ast;
        for (size_t fuzz_step = 0; fuzz_step < this_query_runs; ++fuzz_step)
        {
            fmt::print(stderr, "Fuzzing step {} out of {}\n", fuzz_step, this_query_runs);

            ASTPtr ast_to_process;
            try
            {
                WriteBufferFromOwnString dump_before_fuzz;
                fuzz_base->dumpTree(dump_before_fuzz);
                auto base_before_fuzz = fuzz_base->formatForErrorMessage();

                ast_to_process = fuzz_base->clone();

                WriteBufferFromOwnString dump_of_cloned_ast;
                ast_to_process->dumpTree(dump_of_cloned_ast);

                // Run the original query as well.
                if (fuzz_step > 0)
                {
                    fuzzer.fuzzMain(ast_to_process);
                }

                auto base_after_fuzz = fuzz_base->formatForErrorMessage();

                // Check that the source AST didn't change after fuzzing. This
                // helps debug AST cloning errors, where the cloned AST doesn't
                // clone all its children, and erroneously points to some source
                // child elements.
                if (base_before_fuzz != base_after_fuzz)
                {
                    printChangedSettings();

                    fmt::print(
                        stderr,
                        "Base before fuzz: {}\n"
                        "Base after fuzz: {}\n",
                        base_before_fuzz,
                        base_after_fuzz);
                    fmt::print(stderr, "Dump before fuzz:\n{}\n", dump_before_fuzz.str());
                    fmt::print(stderr, "Dump of cloned AST:\n{}\n", dump_of_cloned_ast.str());
                    fmt::print(stderr, "Dump after fuzz:\n");

                    WriteBufferFromOStream cerr_buf(std::cerr, 4096);
                    fuzz_base->dumpTree(cerr_buf);
                    cerr_buf.next();

                    fmt::print(
                        stderr,
                        "Found error: IAST::clone() is broken for some AST node. This is a bug. The original AST ('dump before fuzz') and its cloned copy ('dump of cloned AST') refer to the same nodes, which must never happen. This means that their parent node doesn't implement clone() correctly.");

                    exit(1);
                }

                auto fuzzed_text = ast_to_process->formatForErrorMessage();
                if (fuzz_step > 0 && fuzzed_text == base_before_fuzz)
                {
                    fmt::print(stderr, "Got boring AST\n");
                    continue;
                }

                parsed_query = ast_to_process;
                query_to_send = parsed_query->formatForErrorMessage();

                processParsedSingleQuery();
            }
            catch (...)
            {
                // Some functions (e.g. protocol parsers) don't throw, but
                // set last_exception instead, so we'll also do it here for
                // uniformity.
                // Surprisingly, this is a client exception, because we get the
                // server exception w/o throwing (see onReceiveException()).
                client_exception = std::make_unique<Exception>(getCurrentExceptionMessage(true), getCurrentExceptionCode());
                have_error = true;
            }

            const auto * exception = server_exception ? server_exception.get() : client_exception.get();
            // Sometimes you may get TOO_DEEP_RECURSION from the server,
            // and TOO_DEEP_RECURSION should not fail the fuzzer check.
            if (have_error && exception->code() == ErrorCodes::TOO_DEEP_RECURSION)
            {
                have_error = false;
                server_exception.reset();
                client_exception.reset();
                return true;
            }

            if (have_error)
            {
                fmt::print(stderr, "Error on processing query '{}': {}\n", ast_to_process->formatForErrorMessage(), exception->message());

                // Try to reconnect after errors, for two reasons:
                // 1. We might not have realized that the server died, e.g. if
                //    it sent us a <Fatal> trace and closed connection properly.
                // 2. The connection might have gotten into a wrong state and
                //    the next query will get false positive about
                //    "Unknown packet from server".
                try
                {
                    connection->forceConnected(connection_parameters.timeouts);
                }
                catch (...)
                {
                    // Just report it, we'll terminate below.
                    fmt::print(stderr,
                        "Error while reconnecting to the server: Code: {}: {}\n",
                        getCurrentExceptionCode(),
                        getCurrentExceptionMessage(true));

                    assert(!connection->isConnected());
                }
            }

            if (!connection->isConnected())
            {
                // Probably the server is dead because we found an assertion
                // failure. Fail fast.
                fmt::print(stderr, "Lost connection to the server.\n");

                // Print the changed settings because they might be needed to
                // reproduce the error.
                printChangedSettings();

                return false;
            }

            // Check that after the query is formatted, we can parse it back,
            // format again and get the same result. Unfortunately, we can't
            // compare the ASTs, which would be more sensitive to errors. This
            // double formatting check doesn't catch all errors, e.g. we can
            // format query incorrectly, but to a valid SQL that we can then
            // parse and format into the same SQL.
            // There are some complicated cases where we can generate the SQL
            // which we can't parse:
            // * first argument of lambda() replaced by fuzzer with
            //   something else, leading to constructs such as
            //   arrayMap((min(x) + 3) -> x + 1, ....)
            // * internals of Enum replaced, leading to:
            //   Enum(equals(someFunction(y), 3)).
            // And there are even the cases when we can parse the query, but
            // it's logically incorrect and its formatting is a mess, such as
            // when `lambda()` function gets substituted into a wrong place.
            // To avoid dealing with these cases, run the check only for the
            // queries we were able to successfully execute.
            // Another caveat is that sometimes WITH queries are not executed,
            // if they are not referenced by the main SELECT, so they can still
            // have the aforementioned problems. Disable this check for such
            // queries, for lack of a better solution.
            // There is also a problem that fuzzer substitutes positive Int64
            // literals or Decimal literals, which are then parsed back as
            // UInt64, and suddenly duplicate alias substitition starts or stops
            // working (ASTWithAlias::formatImpl) or something like that.
            // So we compare not even the first and second formatting of the
            // query, but second and third.
            // If you have to add any more workarounds to this check, just remove
            // it altogether, it's not so useful.
            if (!have_error && !queryHasWithClause(parsed_query.get()))
            {
                ASTPtr ast_2;
                try
                {
                    const auto * tmp_pos = query_to_send.c_str();

                    ast_2 = parseQuery(tmp_pos, tmp_pos + query_to_send.size(),
                        false /* allow_multi_statements */);
                }
                catch (Exception & e)
                {
                    if (e.code() != ErrorCodes::SYNTAX_ERROR &&
                        e.code() != ErrorCodes::TOO_DEEP_RECURSION)
                        throw;
                }

                if (ast_2)
                {
                    const auto text_2 = ast_2->formatForErrorMessage();
                    const auto * tmp_pos = text_2.c_str();
                    const auto ast_3 = parseQuery(tmp_pos, tmp_pos + text_2.size(),
                        false /* allow_multi_statements */);
                    const auto text_3 = ast_3->formatForErrorMessage();
                    if (text_3 != text_2)
                    {
                        fmt::print(stderr, "Found error: The query formatting is broken.\n");

                        printChangedSettings();

                        fmt::print(stderr,
                            "Got the following (different) text after formatting the fuzzed query and parsing it back:\n'{}'\n, expected:\n'{}'\n",
                            text_3, text_2);
                        fmt::print(stderr, "In more detail:\n");
                        fmt::print(stderr, "AST-1 (generated by fuzzer):\n'{}'\n", parsed_query->dumpTree());
                        fmt::print(stderr, "Text-1 (AST-1 formatted):\n'{}'\n", query_to_send);
                        fmt::print(stderr, "AST-2 (Text-1 parsed):\n'{}'\n", ast_2->dumpTree());
                        fmt::print(stderr, "Text-2 (AST-2 formatted):\n'{}'\n", text_2);
                        fmt::print(stderr, "AST-3 (Text-2 parsed):\n'{}'\n", ast_3->dumpTree());
                        fmt::print(stderr, "Text-3 (AST-3 formatted):\n'{}'\n", text_3);
                        fmt::print(stderr, "Text-3 must be equal to Text-2, but it is not.\n");

                        exit(1);
                    }
                }
            }

            // The server is still alive so we're going to continue fuzzing.
            // Determine what we're going to use as the starting AST.
            if (have_error)
            {
                // Query completed with error, keep the previous starting AST.
                // Also discard the exception that we now know to be non-fatal,
                // so that it doesn't influence the exit code.
                server_exception.reset();
                client_exception.reset();
                have_error = false;
            }
            else if (ast_to_process->formatForErrorMessage().size() > 500)
            {
                // ast too long, start from original ast
                fmt::print(stderr, "Current AST is too long, discarding it and using the original AST as a start\n");
                fuzz_base = orig_ast;
            }
            else
            {
                // fuzz starting from this successful query
                fmt::print(stderr, "Query succeeded, using this AST as a start\n");
                fuzz_base = ast_to_process;
            }
        }

        return true;
    }

    void processTextAsSingleQuery(const String & text_)
    {
        full_query = text_;

        /// Some parts of a query (result output and formatting) are executed
        /// client-side. Thus we need to parse the query.
        const char * begin = full_query.data();
        parsed_query = parseQuery(begin, begin + full_query.size(), false);

        if (!parsed_query)
            return;

        // An INSERT query may have the data that follow query text. Remove the
        /// Send part of query without data, because data will be sent separately.
        auto * insert = parsed_query->as<ASTInsertQuery>();
        if (insert && insert->data)
        {
            query_to_send = full_query.substr(0, insert->data - full_query.data());
        }
        else
        {
            query_to_send = full_query;
        }

        processParsedSingleQuery();

        if (have_error)
        {
            reportQueryError();
        }
    }

    // Parameters are in global variables:
    // 'parsed_query' -- the query AST,
    // 'query_to_send' -- the query text that is sent to server,
    // 'full_query' -- for INSERT queries, contains the query and the data that
    // follow it. Its memory is referenced by ASTInsertQuery::begin, end.
    void processParsedSingleQuery(std::optional<bool> echo_query = {})
    {
        resetOutput();
        client_exception.reset();
        server_exception.reset();
        have_error = false;

        if (echo_query.value_or(echo_queries))
        {
            writeString(full_query, std_out);
            writeChar('\n', std_out);
            std_out.next();
        }

        if (is_interactive)
        {
            // Generate a new query_id
            context->setCurrentQueryId("");
            for (const auto & query_id_format : query_id_formats)
            {
                writeString(query_id_format.first, std_out);
                writeString(fmt::format(query_id_format.second, fmt::arg("query_id", context->getCurrentQueryId())), std_out);
                writeChar('\n', std_out);
                std_out.next();
            }
        }

        processed_rows = 0;
        written_first_block = false;
        progress_indication.resetProgress();

        {
            /// Temporarily apply query settings to context.
            std::optional<Settings> old_settings;
            SCOPE_EXIT_SAFE({
                if (old_settings)
                    context->setSettings(*old_settings);
            });
            auto apply_query_settings = [&](const IAST & settings_ast)
            {
                if (!old_settings)
                    old_settings.emplace(context->getSettingsRef());
                context->applySettingsChanges(settings_ast.as<ASTSetQuery>()->changes);
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
                processInsertQuery();
            }
            else
                processOrdinaryQuery();
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
                        context->applySettingChange(change);
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

        if (is_interactive)
        {
            std::cout << std::endl << processed_rows << " rows in set. Elapsed: " << progress_indication.elapsedSeconds() << " sec. ";
            /// Write final progress if it makes sense to do so.
            writeFinalProgress();

            std::cout << std::endl << std::endl;
        }
        else if (print_time_to_stderr)
        {
            std::cerr << progress_indication.elapsedSeconds() << "\n";
        }
    }


    /// Convert external tables to ExternalTableData and send them using the connection.
    void sendExternalTables()
    {
        const auto * select = parsed_query->as<ASTSelectWithUnionQuery>();
        if (!select && !external_tables.empty())
            throw Exception("External tables could be sent only with select query", ErrorCodes::BAD_ARGUMENTS);

        std::vector<ExternalTableDataPtr> data;
        for (auto & table : external_tables)
            data.emplace_back(table.getData(context));

        connection->sendExternalTablesData(data);
    }


    /// Process the query that doesn't require transferring data blocks to the server.
    void processOrdinaryQuery()
    {
        /// Rewrite query only when we have query parameters.
        /// Note that if query is rewritten, comments in query are lost.
        /// But the user often wants to see comments in server logs, query log, processlist, etc.
        if (!query_parameters.empty())
        {
            /// Replace ASTQueryParameter with ASTLiteral for prepared statements.
            ReplaceQueryParameterVisitor visitor(query_parameters);
            visitor.visit(parsed_query);

            /// Get new query after substitutions. Note that it cannot be done for INSERT query with embedded data.
            query_to_send = serializeAST(*parsed_query);
        }

        int retries_left = 10;
        for (;;)
        {
            assert(retries_left > 0);

            try
            {
                connection->sendQuery(
                    connection_parameters.timeouts,
                    query_to_send,
                    context->getCurrentQueryId(),
                    query_processing_stage,
                    &context->getSettingsRef(),
                    &context->getClientInfo(),
                    true);

                sendExternalTables();
                receiveResult();

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
    }


    /// Process the query that requires transferring data blocks to the server.
    void processInsertQuery()
    {
        const auto parsed_insert_query = parsed_query->as<ASTInsertQuery &>();
        if (!parsed_insert_query.data && (is_interactive || (!stdin_is_a_tty && std_in.eof())))
            throw Exception("No data to insert", ErrorCodes::NO_DATA_TO_INSERT);

        connection->sendQuery(
            connection_parameters.timeouts,
            query_to_send,
            context->getCurrentQueryId(),
            query_processing_stage,
            &context->getSettingsRef(),
            &context->getClientInfo(),
            true);

        sendExternalTables();

        /// Receive description of table structure.
        Block sample;
        ColumnsDescription columns_description;
        if (receiveSampleBlock(sample, columns_description))
        {
            /// If structure was received (thus, server has not thrown an exception),
            /// send our data with that structure.
            sendData(sample, columns_description);
            receiveEndOfQuery();
        }
    }


    ASTPtr parseQuery(const char *& pos, const char * end, bool allow_multi_statements)
    {
        ParserQuery parser(end);
        ASTPtr res;

        const auto & settings = context->getSettingsRef();
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
            res = parseQueryAndMovePosition(parser, pos, end, "", allow_multi_statements, max_length, settings.max_parser_depth);

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


    void sendData(Block & sample, const ColumnsDescription & columns_description)
    {
        /// If INSERT data must be sent.
        auto * parsed_insert_query = parsed_query->as<ASTInsertQuery>();
        if (!parsed_insert_query)
            return;

        if (parsed_insert_query->data)
        {
            /// Send data contained in the query.
            ReadBufferFromMemory data_in(parsed_insert_query->data, parsed_insert_query->end - parsed_insert_query->data);
            try
            {
                sendDataFrom(data_in, sample, columns_description);
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
            /// Send data read from stdin.
            try
            {
                if (need_render_progress)
                {
                    /// Set total_bytes_to_read for current fd.
                    FileProgress file_progress(0, std_in.size());
                    progress_indication.updateProgress(Progress(file_progress));

                    /// Set callback to be called on file progress.
                    progress_indication.setFileProgressCallback(context, true);

                    /// Add callback to track reading from fd.
                    std_in.setProgressCallback(context);
                }

                sendDataFrom(std_in, sample, columns_description);
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


    void sendDataFrom(ReadBuffer & buf, Block & sample, const ColumnsDescription & columns_description)
    {
        String current_format = insert_format;

        /// Data format can be specified in the INSERT query.
        if (const auto * insert = parsed_query->as<ASTInsertQuery>())
        {
            if (!insert->format.empty())
                current_format = insert->format;
        }

        BlockInputStreamPtr block_input = context->getInputFormat(current_format, buf, sample, insert_format_max_block_size);

        if (columns_description.hasDefaults())
            block_input = std::make_shared<AddingDefaultsBlockInputStream>(block_input, columns_description, context);

        BlockInputStreamPtr async_block_input = std::make_shared<AsynchronousBlockInputStream>(block_input);

        async_block_input->readPrefix();

        while (true)
        {
            Block block = async_block_input->read();

            /// Check if server send Log packet
            receiveLogs();

            /// Check if server send Exception packet
            auto packet_type = connection->checkPacket();
            if (packet_type && *packet_type == Protocol::Server::Exception)
            {
                /*
                 * We're exiting with error, so it makes sense to kill the
                 * input stream without waiting for it to complete.
                 */
                async_block_input->cancel(true);
                return;
            }

            connection->sendData(block);
            processed_rows += block.rows();

            if (!block)
                break;
        }

        async_block_input->readSuffix();
    }


    /// Flush all buffers.
    void resetOutput()
    {
        block_out_stream.reset();
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


    /// Receives and processes packets coming from server.
    /// Also checks if query execution should be cancelled.
    void receiveResult()
    {
        InterruptListener interrupt_listener;
        bool cancelled = false;

        // TODO: get the poll_interval from commandline.
        const auto receive_timeout = connection_parameters.timeouts.receive_timeout;
        constexpr size_t default_poll_interval = 1000000; /// in microseconds
        constexpr size_t min_poll_interval = 5000; /// in microseconds
        const size_t poll_interval
            = std::max(min_poll_interval, std::min<size_t>(receive_timeout.totalMicroseconds(), default_poll_interval));

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
                        cancelled = true;
                        if (is_interactive)
                        {
                            progress_indication.clearProgressOutput();
                            std::cout << "Cancelling query." << std::endl;
                        }

                        /// Pressing Ctrl+C twice results in shut down.
                        interrupt_listener.unblock();
                    };

                    if (interrupt_listener.check())
                    {
                        cancel_query();
                    }
                    else
                    {
                        double elapsed = receive_watch.elapsedSeconds();
                        if (elapsed > receive_timeout.totalSeconds())
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

            if (!receiveAndProcessPacket(cancelled))
                break;
        }

        if (cancelled && is_interactive)
            std::cout << "Query was cancelled." << std::endl;
    }


    /// Receive a part of the result, or progress info or an exception and process it.
    /// Returns true if one should continue receiving packets.
    /// Output of result is suppressed if query was cancelled.
    bool receiveAndProcessPacket(bool cancelled)
    {
        Packet packet = connection->receivePacket();

        switch (packet.type)
        {
            case Protocol::Server::PartUUIDs:
                return true;

            case Protocol::Server::Data:
                if (!cancelled)
                    onData(packet.block);
                return true;

            case Protocol::Server::Progress:
                onProgress(packet.progress);
                return true;

            case Protocol::Server::ProfileInfo:
                onProfileInfo(packet.profile_info);
                return true;

            case Protocol::Server::Totals:
                if (!cancelled)
                    onTotals(packet.block);
                return true;

            case Protocol::Server::Extremes:
                if (!cancelled)
                    onExtremes(packet.block);
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

            default:
                throw Exception(
                    ErrorCodes::UNKNOWN_PACKET_FROM_SERVER, "Unknown packet {} from server {}", packet.type, connection->getDescription());
        }
    }


    /// Receive the block that serves as an example of the structure of table where data will be inserted.
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description)
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
                    return receiveSampleBlock(out, columns_description);

                default:
                    throw NetException(
                        "Unexpected packet from server (expected Data, Exception or Log, got "
                            + String(Protocol::Server::toString(packet.type)) + ")",
                        ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
            }
        }
    }


    /// Process Log packets, exit when receive Exception or EndOfStream
    bool receiveEndOfQuery()
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

                default:
                    throw NetException(
                        "Unexpected packet from server (expected Exception, EndOfStream or Log, got "
                            + String(Protocol::Server::toString(packet.type)) + ")",
                        ErrorCodes::UNEXPECTED_PACKET_FROM_SERVER);
            }
        }
    }

    /// Process Log packets, used when inserting data by blocks
    void receiveLogs()
    {
        auto packet_type = connection->checkPacket();

        while (packet_type && *packet_type == Protocol::Server::Log)
        {
            receiveAndProcessPacket(false);
            packet_type = connection->checkPacket();
        }
    }

    void initBlockOutputStream(const Block & block)
    {
        if (!block_out_stream)
        {
            /// Ignore all results when fuzzing as they can be huge.
            if (query_fuzzer_runs)
            {
                block_out_stream = std::make_shared<NullBlockOutputStream>(block);
                return;
            }

            WriteBuffer * out_buf = nullptr;
            String pager = config().getString("pager", "");
            if (!pager.empty())
            {
                signal(SIGPIPE, SIG_IGN);
                pager_cmd = ShellCommand::execute(pager, true);
                out_buf = &pager_cmd->in;
            }
            else
            {
                out_buf = &std_out;
            }

            String current_format = format;

            /// The query can specify output format or output file.
            /// FIXME: try to prettify this cast using `as<>()`
            if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(parsed_query.get()))
            {
                if (query_with_output->out_file)
                {
                    const auto & out_file_node = query_with_output->out_file->as<ASTLiteral &>();
                    const auto & out_file = out_file_node.value.safeGet<std::string>();

                    out_file_buf.emplace(out_file, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_EXCL | O_CREAT);
                    out_buf = &*out_file_buf;

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

            /// It is not clear how to write progress with parallel formatting. It may increase code complexity significantly.
            if (!need_render_progress)
                block_out_stream = context->getOutputStreamParallelIfPossible(current_format, *out_buf, block);
            else
                block_out_stream = context->getOutputStream(current_format, *out_buf, block);

            block_out_stream->writePrefix();
        }
    }


    void initLogsOutputStream()
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

            logs_out_stream = std::make_shared<InternalTextLogsRowOutputStream>(*wb, stdout_is_a_tty);
            logs_out_stream->writePrefix();
        }
    }


    void onData(Block & block)
    {
        if (!block)
            return;

        processed_rows += block.rows();

        /// Even if all blocks are empty, we still need to initialize the output stream to write empty resultset.
        initBlockOutputStream(block);

        /// The header block containing zero rows was used to initialize
        /// block_out_stream, do not output it.
        /// Also do not output too much data if we're fuzzing.
        if (block.rows() == 0 || (query_fuzzer_runs != 0 && processed_rows >= 100))
            return;

        if (need_render_progress)
            progress_indication.clearProgressOutput();

        block_out_stream->write(block);
        written_first_block = true;

        /// Received data block is immediately displayed to the user.
        block_out_stream->flush();

        /// Restore progress bar after data block.
        if (need_render_progress)
            progress_indication.writeProgress();
    }


    void onLogData(Block & block)
    {
        initLogsOutputStream();
        progress_indication.clearProgressOutput();
        logs_out_stream->write(block);
        logs_out_stream->flush();
    }


    void onTotals(Block & block)
    {
        initBlockOutputStream(block);
        block_out_stream->setTotals(block);
    }

    void onExtremes(Block & block)
    {
        initBlockOutputStream(block);
        block_out_stream->setExtremes(block);
    }


    void onProgress(const Progress & value)
    {
        if (!progress_indication.updateProgress(value))
        {
            // Just a keep-alive update.
            return;
        }

        if (block_out_stream)
            block_out_stream->onProgress(value);

        if (need_render_progress)
            progress_indication.writeProgress();
    }


    void writeFinalProgress()
    {
        progress_indication.writeFinalProgress();
    }


    void onReceiveExceptionFromServer(std::unique_ptr<Exception> && e)
    {
        have_error = true;
        server_exception = std::move(e);
        resetOutput();
    }


    void onProfileInfo(const BlockStreamProfileInfo & profile_info)
    {
        if (profile_info.hasAppliedLimit() && block_out_stream)
            block_out_stream->setRowsBeforeLimit(profile_info.getRowsBeforeLimit());
    }


    void onEndOfStream()
    {
        progress_indication.clearProgressOutput();

        if (block_out_stream)
            block_out_stream->writeSuffix();

        if (logs_out_stream)
            logs_out_stream->writeSuffix();

        resetOutput();

        if (is_interactive && !written_first_block)
        {
            progress_indication.clearProgressOutput();
            std::cout << "Ok." << std::endl;
        }
    }

    static void showClientVersion()
    {
        std::cout << DBMS_NAME << " client version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
    }

    static void clearTerminal()
    {
        /// Clear from cursor until end of screen.
        /// It is needed if garbage is left in terminal.
        /// Show cursor. It can be left hidden by invocation of previous programs.
        /// A test for this feature: perl -e 'print "x"x100000'; echo -ne '\033[0;0H\033[?25l'; clickhouse-client
        std::cout << "\033[0J"
                     "\033[?25h";
    }

public:
    void init(int argc, char ** argv)
    {
        /// Don't parse options with Poco library. We need more sophisticated processing.
        stopOptionsProcessing();

        /** We allow different groups of arguments:
          * - common arguments;
          * - arguments for any number of external tables each in form "--external args...",
          *   where possible args are file, name, format, structure, types;
          * - param arguments for prepared statements.
          * Split these groups before processing.
          */
        using Arguments = std::vector<std::string>;

        Arguments common_arguments{""}; /// 0th argument is ignored.
        std::vector<Arguments> external_tables_arguments;

        bool in_external_group = false;
        for (int arg_num = 1; arg_num < argc; ++arg_num)
        {
            const char * arg = argv[arg_num];

            if (0 == strcmp(arg, "--external"))
            {
                in_external_group = true;
                external_tables_arguments.emplace_back(Arguments{""});
            }
            /// Options with value after equal sign.
            else if (
                in_external_group
                && (0 == strncmp(arg, "--file=", strlen("--file=")) || 0 == strncmp(arg, "--name=", strlen("--name="))
                    || 0 == strncmp(arg, "--format=", strlen("--format=")) || 0 == strncmp(arg, "--structure=", strlen("--structure="))
                    || 0 == strncmp(arg, "--types=", strlen("--types="))))
            {
                external_tables_arguments.back().emplace_back(arg);
            }
            /// Options with value after whitespace.
            else if (
                in_external_group
                && (0 == strcmp(arg, "--file") || 0 == strcmp(arg, "--name") || 0 == strcmp(arg, "--format")
                    || 0 == strcmp(arg, "--structure") || 0 == strcmp(arg, "--types")))
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

        stdin_is_a_tty = isatty(STDIN_FILENO);
        stdout_is_a_tty = isatty(STDOUT_FILENO);

        uint64_t terminal_width = 0;
        if (stdin_is_a_tty)
            terminal_width = getTerminalWidth();

        namespace po = boost::program_options;

        /// Main commandline options related to client functionality and all parameters from Settings.
        po::options_description main_description = createOptionsDescription("Main options", terminal_width);
        main_description.add_options()
            ("help", "produce help message")
            ("config-file,C", po::value<std::string>(), "config-file path")
            ("config,c", po::value<std::string>(), "config-file path (another shorthand)")
            ("host,h", po::value<std::string>()->default_value("localhost"), "server host")
            ("port", po::value<int>()->default_value(9000), "server port")
            ("secure,s", "Use TLS connection")
            ("user,u", po::value<std::string>()->default_value("default"), "user")
            /** If "--password [value]" is used but the value is omitted, the bad argument exception will be thrown.
              * implicit_value is used to avoid this exception (to allow user to type just "--password")
              * Since currently boost provides no way to check if a value has been set implicitly for an option,
              * the "\n" is used to distinguish this case because there is hardly a chance a user would use "\n"
              * as the password.
              */
            ("password", po::value<std::string>()->implicit_value("\n", ""), "password")
            ("ask-password", "ask-password")
            ("quota_key", po::value<std::string>(), "A string to differentiate quotas when the user have keyed quotas configured on server")
            ("stage", po::value<std::string>()->default_value("complete"), "Request query processing up to specified stage: complete,fetch_columns,with_mergeable_state,with_mergeable_state_after_aggregation,with_mergeable_state_after_aggregation_and_limit")
            ("query_id", po::value<std::string>(), "query_id")
            ("query,q", po::value<std::string>(), "query")
            ("database,d", po::value<std::string>(), "database")
            ("pager", po::value<std::string>(), "pager")
            ("disable_suggestion,A", "Disable loading suggestion data. Note that suggestion data is loaded asynchronously through a second connection to ClickHouse server. Also it is reasonable to disable suggestion if you want to paste a query with TAB characters. Shorthand option -A is for those who get used to mysql client.")
            ("suggestion_limit", po::value<int>()->default_value(10000),
                "Suggestion limit for how many databases, tables and columns to fetch.")
            ("multiline,m", "multiline")
            ("multiquery,n", "multiquery")
            ("queries-file", po::value<std::vector<std::string>>()->multitoken(),
                "file path with queries to execute; multiple files can be specified (--queries-file file1 file2...)")
            ("format,f", po::value<std::string>(), "default output format")
            ("testmode,T", "enable test hints in comments")
            ("ignore-error", "do not stop processing in multiquery mode")
            ("vertical,E", "vertical output format, same as --format=Vertical or FORMAT Vertical or \\G at end of command")
            ("time,t", "print query execution time to stderr in non-interactive mode (for benchmarks)")
            ("stacktrace", "print stack traces of exceptions")
            ("progress", "print progress even in non-interactive mode")
            ("version,V", "print version information and exit")
            ("version-clean", "print version in machine-readable format and exit")
            ("echo", "in batch mode, print query before execution")
            ("max_client_network_bandwidth", po::value<int>(), "the maximum speed of data exchange over the network for the client in bytes per second.")
            ("compression", po::value<bool>(), "enable or disable compression")
            ("highlight", po::value<bool>()->default_value(true), "enable or disable basic syntax highlight in interactive command line")
            ("log-level", po::value<std::string>(), "client log level")
            ("server_logs_file", po::value<std::string>(), "put server logs into specified file")
            ("query-fuzzer-runs", po::value<int>()->default_value(0), "After executing every SELECT query, do random mutations in it and run again specified number of times. This is used for testing to discover unexpected corner cases.")
            ("interleave-queries-file", po::value<std::vector<std::string>>()->multitoken(),
                "file path with queries to execute before every file from 'queries-file'; multiple files can be specified (--queries-file file1 file2...); this is needed to enable more aggressive fuzzing of newly added tests (see 'query-fuzzer-runs' option)")
            ("opentelemetry-traceparent", po::value<std::string>(), "OpenTelemetry traceparent header as described by W3C Trace Context recommendation")
            ("opentelemetry-tracestate", po::value<std::string>(), "OpenTelemetry tracestate header as described by W3C Trace Context recommendation")
            ("history_file", po::value<std::string>(), "path to history file")
        ;

        Settings cmd_settings;
        cmd_settings.addProgramOptions(main_description);

        /// Commandline options related to external tables.
        po::options_description external_description = createOptionsDescription("External tables options", terminal_width);
        external_description.add_options()("file", po::value<std::string>(), "data file or - for stdin")(
            "name",
            po::value<std::string>()->default_value("_data"),
            "name of the table")("format", po::value<std::string>()->default_value("TabSeparated"), "data format")("structure", po::value<std::string>(), "structure")("types", po::value<std::string>(), "types");

        /// Parse main commandline options.
        po::parsed_options parsed = po::command_line_parser(common_arguments).options(main_description).run();
        auto unrecognized_options = po::collect_unrecognized(parsed.options, po::collect_unrecognized_mode::include_positional);
        // unrecognized_options[0] is "", I don't understand why we need "" as the first argument which unused
        if (unrecognized_options.size() > 1)
        {
            throw Exception("Unrecognized option '" + unrecognized_options[1] + "'", ErrorCodes::UNRECOGNIZED_ARGUMENTS);
        }
        po::variables_map options;
        po::store(parsed, options);
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
            std::cout << main_description << "\n";
            std::cout << external_description << "\n";
            std::cout << "In addition, --param_name=value can be specified for substitution of parameters for parametrized queries.\n";
            exit(0);
        }

        if (options.count("log-level"))
            Poco::Logger::root().setLevel(options["log-level"].as<std::string>());

        size_t number_of_external_tables_with_stdin_source = 0;
        for (size_t i = 0; i < external_tables_arguments.size(); ++i)
        {
            /// Parse commandline options related to external tables.
            po::parsed_options parsed_tables = po::command_line_parser(external_tables_arguments[i]).options(external_description).run();
            po::variables_map external_options;
            po::store(parsed_tables, external_options);

            try
            {
                external_tables.emplace_back(external_options);
                if (external_tables.back().file == "-")
                    ++number_of_external_tables_with_stdin_source;
                if (number_of_external_tables_with_stdin_source > 1)
                    throw Exception("Two or more external tables has stdin (-) set as --file field", ErrorCodes::BAD_ARGUMENTS);
            }
            catch (const Exception & e)
            {
                std::string text = e.displayText();
                std::cerr << "Code: " << e.code() << ". " << text << std::endl;
                std::cerr << "Table №" << i << std::endl << std::endl;
                /// Avoid the case when error exit code can possibly overflow to normal (zero).
                auto exit_code = e.code() % 256;
                if (exit_code == 0)
                    exit_code = 255;
                exit(exit_code);
            }
        }

        context->makeGlobalContext();
        context->setSettings(cmd_settings);

        /// Copy settings-related program options to config.
        /// TODO: Is this code necessary?
        for (const auto & setting : context->getSettingsRef().all())
        {
            const auto & name = setting.getName();
            if (options.count(name))
                config().setString(name, options[name].as<std::string>());
        }

        if (options.count("config-file") && options.count("config"))
            throw Exception("Two or more configuration files referenced in arguments", ErrorCodes::BAD_ARGUMENTS);

        query_processing_stage = QueryProcessingStage::fromString(options["stage"].as<std::string>());

        /// Save received data into the internal config.
        if (options.count("config-file"))
            config().setString("config-file", options["config-file"].as<std::string>());
        if (options.count("config"))
            config().setString("config-file", options["config"].as<std::string>());
        if (options.count("host") && !options["host"].defaulted())
            config().setString("host", options["host"].as<std::string>());
        if (options.count("query_id"))
            config().setString("query_id", options["query_id"].as<std::string>());
        if (options.count("query"))
            config().setString("query", options["query"].as<std::string>());
        if (options.count("queries-file"))
            queries_files = options["queries-file"].as<std::vector<std::string>>();
        if (options.count("interleave-queries-file"))
            interleave_queries_files = options["interleave-queries-file"].as<std::vector<std::string>>();
        if (options.count("database"))
            config().setString("database", options["database"].as<std::string>());
        if (options.count("pager"))
            config().setString("pager", options["pager"].as<std::string>());
        if (options.count("port") && !options["port"].defaulted())
            config().setInt("port", options["port"].as<int>());
        if (options.count("secure"))
            config().setBool("secure", true);
        if (options.count("user") && !options["user"].defaulted())
            config().setString("user", options["user"].as<std::string>());
        if (options.count("password"))
            config().setString("password", options["password"].as<std::string>());
        if (options.count("ask-password"))
            config().setBool("ask-password", true);
        if (options.count("quota_key"))
            config().setString("quota_key", options["quota_key"].as<std::string>());
        if (options.count("multiline"))
            config().setBool("multiline", true);
        if (options.count("multiquery"))
            config().setBool("multiquery", true);
        if (options.count("testmode"))
            config().setBool("testmode", true);
        if (options.count("ignore-error"))
            config().setBool("ignore-error", true);
        if (options.count("format"))
            config().setString("format", options["format"].as<std::string>());
        if (options.count("vertical"))
            config().setBool("vertical", true);
        if (options.count("stacktrace"))
            config().setBool("stacktrace", true);
        if (options.count("progress"))
            config().setBool("progress", true);
        if (options.count("echo"))
            config().setBool("echo", true);
        if (options.count("time"))
            print_time_to_stderr = true;
        if (options.count("max_client_network_bandwidth"))
            max_client_network_bandwidth = options["max_client_network_bandwidth"].as<int>();
        if (options.count("compression"))
            config().setBool("compression", options["compression"].as<bool>());
        if (options.count("server_logs_file"))
            server_logs_file = options["server_logs_file"].as<std::string>();
        if (options.count("disable_suggestion"))
            config().setBool("disable_suggestion", true);
        if (options.count("suggestion_limit"))
            config().setInt("suggestion_limit", options["suggestion_limit"].as<int>());
        if (options.count("highlight"))
            config().setBool("highlight", options["highlight"].as<bool>());
        if (options.count("history_file"))
            config().setString("history_file", options["history_file"].as<std::string>());

        if ((query_fuzzer_runs = options["query-fuzzer-runs"].as<int>()))
        {
            // Fuzzer implies multiquery.
            config().setBool("multiquery", true);

            // Ignore errors in parsing queries.
            config().setBool("ignore-error", true);
            ignore_error = true;
        }

        if (options.count("opentelemetry-traceparent"))
        {
            std::string traceparent = options["opentelemetry-traceparent"].as<std::string>();
            std::string error;
            if (!context->getClientInfo().client_trace_context.parseTraceparentHeader(traceparent, error))
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse OpenTelemetry traceparent '{}': {}", traceparent, error);
            }
        }

        if (options.count("opentelemetry-tracestate"))
        {
            context->getClientInfo().client_trace_context.tracestate = options["opentelemetry-tracestate"].as<std::string>();
        }

        argsToConfig(common_arguments, config(), 100);

        clearPasswordFromCommandLine(argc, argv);
    }
};
}

#pragma GCC diagnostic ignored "-Wunused-function"
#pragma GCC diagnostic ignored "-Wmissing-declarations"

int mainEntryClickHouseClient(int argc, char ** argv)
{
    try
    {
        DB::Client client;
        client.init(argc, argv);
        return client.run();
    }
    catch (const boost::program_options::error & e)
    {
        std::cerr << "Bad arguments: " << e.what() << std::endl;
        return 1;
    }
    catch (const DB::Exception & e)
    {
        std::string text = e.displayText();
        std::cerr << "Code: " << e.code() << ". " << text << std::endl;
        return 1;
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << std::endl;
        return 1;
    }
}
