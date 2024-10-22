#pragma once


#include <Client/ProgressTable.h>
#include <Client/Suggest.h>
#include <Core/ExternalTable.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTCreateQuery.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/SimpleFileChannel.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/Application.h>
#include <Common/DNSResolver.h>
#include <Common/InterruptListener.h>
#include <Common/ProgressIndication.h>
#include <Common/QueryFuzzer.h>
#include <Common/ShellCommand.h>
#include <Common/Stopwatch.h>

#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageFile.h>

#include <boost/program_options.hpp>

#include <atomic>
#include <optional>
#include <string_view>
#include <string>

namespace po = boost::program_options;


namespace DB
{

static constexpr std::string_view DEFAULT_CLIENT_NAME = "client";

static const NameSet exit_strings
{
    "exit", "quit", "logout", "учше", "йгше", "дщпщге",
    "exit;", "quit;", "logout;", "учшеж", "йгшеж", "дщпщгеж",
    "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"
};

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

enum MultiQueryProcessingStage
{
    QUERIES_END,
    PARSING_EXCEPTION,
    CONTINUE_PARSING,
    EXECUTE_QUERY,
    PARSING_FAILED,
};

enum ProgressOption
{
    DEFAULT,
    OFF,
    TTY,
    ERR,
};
ProgressOption toProgressOption(std::string progress);
std::istream& operator>> (std::istream & in, ProgressOption & progress);

class InternalTextLogs;
class TerminalKeystrokeInterceptor;
class WriteBufferFromFileDescriptor;

/**
 * The base class which encapsulates the core functionality of a client.
 * Can be used in a standalone application (clickhouse-client or clickhouse-local),
 * or be embedded into server.
 * Always keep in mind that there can be several instances of this class within
 * a process. Thus, it cannot keep its state in global shared variables or even use them.
 * The best example - std::cin, std::cout and std::cerr.
 */
class ClientBase
{
public:
    using Arguments = std::vector<String>;

    explicit ClientBase
    (
        int in_fd_ = STDIN_FILENO,
        int out_fd_ = STDOUT_FILENO,
        int err_fd_ = STDERR_FILENO,
        std::istream & input_stream_ = std::cin,
        std::ostream & output_stream_ = std::cout,
        std::ostream & error_stream_ = std::cerr
    );
    virtual ~ClientBase();

    bool tryStopQuery() { return query_interrupt_handler.tryStop(); }
    void stopQuery() { query_interrupt_handler.stop(); }

    ASTPtr parseQuery(const char *& pos, const char * end, const Settings & settings, bool allow_multi_statements);

protected:
    void runInteractive();
    void runNonInteractive();

    char * argv0 = nullptr;
    void runLibFuzzer();

    /// This is the analogue of Poco::Application::config()
    virtual Poco::Util::LayeredConfiguration & getClientConfiguration() = 0;

    virtual bool processWithFuzzing(const String &)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Query processing with fuzzing is not implemented");
    }

    virtual void connect() = 0;
    virtual void processError(const String & query) const = 0;
    virtual String getName() const = 0;

    void processOrdinaryQuery(const String & query_to_execute, ASTPtr parsed_query);
    void processInsertQuery(const String & query_to_execute, ASTPtr parsed_query);

    void processTextAsSingleQuery(const String & full_query);
    void processParsedSingleQuery(const String & full_query, const String & query_to_execute,
        ASTPtr parsed_query, std::optional<bool> echo_query_ = {}, bool report_error = false);

    static void adjustQueryEnd(const char *& this_query_end, const char * all_queries_end, uint32_t max_parser_depth, uint32_t max_parser_backtracks);
    virtual void setupSignalHandler() = 0;

    bool executeMultiQuery(const String & all_queries_text);
    MultiQueryProcessingStage analyzeMultiQueryText(
        const char *& this_query_begin, const char *& this_query_end, const char * all_queries_end,
        String & query_to_execute, ASTPtr & parsed_query, const String & all_queries_text,
        std::unique_ptr<Exception> & current_exception);

    void clearTerminal();
    void showClientVersion();

    using ProgramOptionsDescription = boost::program_options::options_description;
    using CommandLineOptions = boost::program_options::variables_map;

    struct OptionsDescription
    {
        std::optional<ProgramOptionsDescription> main_description;
        std::optional<ProgramOptionsDescription> external_description;
        std::optional<ProgramOptionsDescription> hosts_and_ports_description;
    };

    virtual void updateLoggerLevel(const String &) {}
    virtual void printHelpMessage(const OptionsDescription & options_description, bool verbose) = 0;
    virtual void addOptions(OptionsDescription & options_description) = 0;
    virtual void processOptions(const OptionsDescription & options_description,
                                const CommandLineOptions & options,
                                const std::vector<Arguments> & external_tables_arguments,
                                const std::vector<Arguments> & hosts_and_ports_arguments) = 0;
    virtual void processConfig() = 0;

    /// Returns true if query processing was successful.
    bool processQueryText(const String & text);

    virtual void readArguments(
        int argc,
        char ** argv,
        Arguments & common_arguments,
        std::vector<Arguments> & external_tables_arguments,
        std::vector<Arguments> & hosts_and_ports_arguments) = 0;

    void setInsertionTable(const ASTInsertQuery & insert_query);

private:
    void receiveResult(ASTPtr parsed_query, Int32 signals_before_stop, bool partial_result_on_first_cancel);
    bool receiveAndProcessPacket(ASTPtr parsed_query, bool cancelled_);
    void receiveLogsAndProfileEvents(ASTPtr parsed_query);
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query);
    bool receiveEndOfQuery();
    void cancelQuery();

    void onProgress(const Progress & value);
    void onTimezoneUpdate(const String & tz);
    void onData(Block & block, ASTPtr parsed_query);
    void onLogData(Block & block);
    void onTotals(Block & block, ASTPtr parsed_query);
    void onExtremes(Block & block, ASTPtr parsed_query);
    void onReceiveExceptionFromServer(std::unique_ptr<Exception> && e);
    void onProfileInfo(const ProfileInfo & profile_info);
    void onEndOfStream();
    void onProfileEvents(Block & block);

    void sendData(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query);
    void sendDataFrom(ReadBuffer & buf, Block & sample,
                      const ColumnsDescription & columns_description, ASTPtr parsed_query, bool have_more_data = false);
    void sendDataFromPipe(Pipe && pipe, ASTPtr parsed_query, bool have_more_data = false);
    void sendDataFromStdin(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query);
    void sendExternalTables(ASTPtr parsed_query);

    void initOutputFormat(const Block & block, ASTPtr parsed_query);
    void initLogsOutputStream();

    String prompt() const;

    void resetOutput();

    void updateSuggest(const ASTPtr & ast);

    void initQueryIdFormats();
    bool addMergeTreeSettings(ASTCreateQuery & ast_create);

protected:

    class QueryInterruptHandler : private boost::noncopyable
    {
    public:
        /// Store how much interrupt signals can be before stopping the query
        /// by default stop after the first interrupt signal.
        void start(Int32 signals_before_stop = 1) { exit_after_signals.store(signals_before_stop); }

        /// Set value not greater then 0 to mark the query as stopped.
        void stop() { exit_after_signals.store(0); }

        /// Return true if the query was stopped.
        /// Query was stopped if it received at least "signals_before_stop" interrupt signals.
        bool tryStop() { return exit_after_signals.fetch_sub(1) <= 0; }
        bool cancelled() { return exit_after_signals.load() <= 0; }

        /// Return how much interrupt signals remain before stop.
        Int32 cancelled_status() { return exit_after_signals.load(); }

    private:
        std::atomic<Int32> exit_after_signals = 0;
    };

    QueryInterruptHandler query_interrupt_handler;

    static bool isSyncInsertWithData(const ASTInsertQuery & insert_query, const ContextPtr & context);
    bool processMultiQueryFromFile(const String & file_name);

    static bool isRegularFile(int fd);

    /// Adjust some settings after command line options and config had been processed.
    void adjustSettings();

    /// Initializes the client context.
    void initClientContext();

    void setDefaultFormatsAndCompressionFromConfiguration();

    void initTTYBuffer(ProgressOption progress_option, ProgressOption progress_table_option);
    void initKeystrokeInterceptor();

    /// Should be one of the first, to be destroyed the last,
    /// since other members can use them.
    SharedContextHolder shared_context;
    ContextMutablePtr global_context;

    /// Client context is a context used only by the client to parse queries, process query parameters and to connect to clickhouse-server.
    ContextMutablePtr client_context;

    std::unique_ptr<TerminalKeystrokeInterceptor> keystroke_interceptor;

    bool is_interactive = false; /// Use either interactive line editing interface or batch mode.
    bool delayed_interactive = false;

    bool echo_queries = false; /// Print queries before execution in batch mode.
    bool ignore_error = false; /// In case of errors, don't print error message, continue to next query. Only applicable for non-interactive mode.

    std::optional<Suggest> suggest;
    bool load_suggestions = false;
    bool wait_for_suggestions_to_load = false;

    std::vector<String> queries; /// Queries passed via '--query'
    std::vector<String> queries_files; /// If not empty, queries will be read from these files
    std::vector<String> interleave_queries_files; /// If not empty, run queries from these files before processing every file from 'queries_files'.

    bool stdin_is_a_tty = false; /// stdin is a terminal.
    bool stdout_is_a_tty = false; /// stdout is a terminal.
    bool stderr_is_a_tty = false; /// stderr is a terminal.
    uint64_t terminal_width = 0;

    String pager;

    String default_output_format; /// Query results output format.
    CompressionMethod default_output_compression_method = CompressionMethod::None;
    String default_input_format; /// Tables' format for clickhouse-local.

    bool select_into_file = false; /// If writing result INTO OUTFILE. It affects progress rendering.
    bool select_into_file_and_stdout = false; /// If writing result INTO OUTFILE AND STDOUT. It affects progress rendering.
    bool is_default_format = true; /// false, if format is set in the config or command line.
    size_t format_max_block_size = 0; /// Max block size for console output.
    size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.
    size_t max_client_network_bandwidth = 0; /// The maximum speed of data exchange over the network for the client in bytes per second.

    bool has_vertical_output_suffix = false; /// Is \G present at the end of the query string?

    /// We will format query_id in interactive mode in various ways, the default is just to print Query id: ...
    std::vector<std::pair<String, String>> query_id_formats;

    /// Settings specified via command line args
    Settings cmd_settings;
    MergeTreeSettings cmd_merge_tree_settings;

    /// thread status should be destructed before shared context because it relies on process list.
    std::optional<ThreadStatus> thread_status;

    ServerConnectionPtr connection;
    ConnectionParameters connection_parameters;

    /// Buffer that reads from stdin in batch mode.
    ReadBufferFromFileDescriptor std_in;
    /// Console output.
    WriteBufferFromFileDescriptor std_out;
    std::unique_ptr<ShellCommand> pager_cmd;

    /// The user can specify to redirect query output to a file.
    std::unique_ptr<WriteBuffer> out_file_buf;
    std::shared_ptr<IOutputFormat> output_format;

    /// The user could specify special file for server logs (stderr by default)
    std::unique_ptr<WriteBuffer> out_logs_buf;
    String server_logs_file;
    std::unique_ptr<InternalTextLogs> logs_out_stream;

    /// /dev/tty if accessible or std::cerr - for progress bar.
    /// We prefer to output progress bar directly to tty to allow user to redirect stdout and stderr and still get the progress indication.
    std::unique_ptr<WriteBufferFromFileDescriptor> tty_buf;

    String home_path;
    String history_file; /// Path to a file containing command history.

    String current_profile;

    UInt64 server_revision = 0;
    String server_version;
    String prompt_by_server_display_name;
    String server_display_name;

    ProgressIndication progress_indication;
    ProgressTable progress_table;
    bool need_render_progress = true;
    bool need_render_progress_table = true;
    std::atomic_bool progress_table_toggle_on = false;
    bool need_render_profile_events = true;
    bool written_first_block = false;
    size_t processed_rows = 0; /// How many rows have been read or written.

    bool print_stack_trace = false;
    /// The last exception that was received from the server. Is used for the
    /// return code in batch mode.
    std::unique_ptr<Exception> server_exception;
    /// Likewise, the last exception that occurred on the client.
    std::unique_ptr<Exception> client_exception;

    /// If the last query resulted in exception. `server_exception` or
    /// `client_exception` must be set.
    bool have_error = false;

    std::list<ExternalTable> external_tables; /// External tables info.
    bool send_external_tables = false;
    NameToNameMap query_parameters; /// Dictionary with query parameters for prepared statements.

    QueryFuzzer fuzzer;
    int query_fuzzer_runs = 0;
    int create_query_fuzzer_runs = 0;

    struct
    {
        bool print = false;
        /// UINT64_MAX -- print only last
        UInt64 delay_ms = 0;
        Stopwatch watch;
        /// For printing only last (delay_ms == 0).
        Block last_block;
    } profile_events;

    QueryProcessingStage::Enum query_processing_stage;
    ClientInfo::QueryKind query_kind;

    struct HostAndPort
    {
        String host;
        std::optional<UInt16> port;
    };

    std::vector<HostAndPort> hosts_and_ports{};

    bool allow_repeated_settings = false;
    bool allow_merge_tree_settings = false;

    std::atomic_bool cancelled = false;
    std::atomic_bool cancelled_printed = false;

    /// Unpacked descriptors and streams for the ease of use.
    int in_fd = STDIN_FILENO;
    int out_fd = STDOUT_FILENO;
    int err_fd = STDERR_FILENO;
    std::istream & input_stream;
    std::ostream & output_stream;
    std::ostream & error_stream;

};

}
