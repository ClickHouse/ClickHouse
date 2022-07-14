#pragma once

#include "Common/NamePrompter.h"
#include <Common/ProgressIndication.h>
#include <Common/InterruptListener.h>
#include <Common/ShellCommand.h>
#include <Common/Stopwatch.h>
#include <Common/DNSResolver.h>
#include <Core/ExternalTable.h>
#include <Poco/Util/Application.h>
#include <Interpreters/Context.h>
#include <Client/Suggest.h>
#include <Client/QueryFuzzer.h>
#include <boost/program_options.hpp>
#include <Storages/StorageFile.h>
#include <Storages/SelectQueryInfo.h>

namespace po = boost::program_options;


namespace DB
{

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

void interruptSignalHandler(int signum);

class InternalTextLogs;

class ClientBase : public Poco::Util::Application, public IHints<2, ClientBase>
{

public:
    using Arguments = std::vector<String>;

    ClientBase();
    ~ClientBase() override;

    void init(int argc, char ** argv);

    std::vector<String> getAllRegisteredNames() const override { return cmd_options; }

protected:
    void runInteractive();
    void runNonInteractive();

    virtual bool processWithFuzzing(const String &)
    {
        throw Exception("Query processing with fuzzing is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void connect() = 0;
    virtual void processError(const String & query) const = 0;
    virtual String getName() const = 0;

    void processOrdinaryQuery(const String & query_to_execute, ASTPtr parsed_query);
    void processInsertQuery(const String & query_to_execute, ASTPtr parsed_query);

    void processTextAsSingleQuery(const String & full_query);
    void processParsedSingleQuery(const String & full_query, const String & query_to_execute,
        ASTPtr parsed_query, std::optional<bool> echo_query_ = {}, bool report_error = false);

    static void adjustQueryEnd(const char *& this_query_end, const char * all_queries_end, int max_parser_depth);
    ASTPtr parseQuery(const char *& pos, const char * end, bool allow_multi_statements) const;
    static void setupSignalHandler();

    bool executeMultiQuery(const String & all_queries_text);
    MultiQueryProcessingStage analyzeMultiQueryText(
        const char *& this_query_begin, const char *& this_query_end, const char * all_queries_end,
        String & query_to_execute, ASTPtr & parsed_query, const String & all_queries_text,
        std::unique_ptr<Exception> & current_exception);

    static void clearTerminal();
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
    virtual void printHelpMessage(const OptionsDescription & options_description) = 0;
    virtual void addOptions(OptionsDescription & options_description) = 0;
    virtual void processOptions(const OptionsDescription & options_description,
                                const CommandLineOptions & options,
                                const std::vector<Arguments> & external_tables_arguments,
                                const std::vector<Arguments> & hosts_and_ports_arguments) = 0;
    virtual void processConfig() = 0;

    bool processQueryText(const String & text);

    virtual void readArguments(
        int argc,
        char ** argv,
        Arguments & common_arguments,
        std::vector<Arguments> & external_tables_arguments,
        std::vector<Arguments> & hosts_and_ports_arguments) = 0;


private:
    void receiveResult(ASTPtr parsed_query);
    bool receiveAndProcessPacket(ASTPtr parsed_query, bool cancelled_);
    void receiveLogsAndProfileEvents(ASTPtr parsed_query);
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query);
    bool receiveEndOfQuery();
    void cancelQuery();

    void onProgress(const Progress & value);
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
    void outputQueryInfo(bool echo_query_);
    void parseAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments);

    void updateSuggest(const ASTPtr & ast);

    void initQueryIdFormats();

protected:
    static bool isSyncInsertWithData(const ASTInsertQuery & insert_query, const ContextPtr & context);
    bool processMultiQueryFromFile(const String & file_name);

    bool is_interactive = false; /// Use either interactive line editing interface or batch mode.
    bool is_multiquery = false;
    bool delayed_interactive = false;

    bool echo_queries = false; /// Print queries before execution in batch mode.
    bool ignore_error = false; /// In case of errors, don't print error message, continue to next query. Only applicable for non-interactive mode.
    bool print_time_to_stderr = false; /// Output execution time to stderr in batch mode.

    std::optional<Suggest> suggest;
    bool load_suggestions = false;

    std::vector<String> queries_files; /// If not empty, queries will be read from these files
    std::vector<String> interleave_queries_files; /// If not empty, run queries from these files before processing every file from 'queries_files'.
    std::vector<String> cmd_options;

    bool stdin_is_a_tty = false; /// stdin is a terminal.
    bool stdout_is_a_tty = false; /// stdout is a terminal.
    uint64_t terminal_width = 0;

    ServerConnectionPtr connection;
    ConnectionParameters connection_parameters;

    String format; /// Query results output format.
    bool select_into_file = false; /// If writing result INTO OUTFILE. It affects progress rendering.
    bool is_default_format = true; /// false, if format is set in the config or command line.
    size_t format_max_block_size = 0; /// Max block size for console output.
    String insert_format; /// Format of INSERT data that is read from stdin in batch mode.
    size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.
    size_t max_client_network_bandwidth = 0; /// The maximum speed of data exchange over the network for the client in bytes per second.

    bool has_vertical_output_suffix = false; /// Is \G present at the end of the query string?

    /// We will format query_id in interactive mode in various ways, the default is just to print Query id: ...
    std::vector<std::pair<String, String>> query_id_formats;

    /// Settings specified via command line args
    Settings cmd_settings;

    SharedContextHolder shared_context;
    ContextMutablePtr global_context;

    /// Buffer that reads from stdin in batch mode.
    ReadBufferFromFileDescriptor std_in{STDIN_FILENO};
    /// Console output.
    WriteBufferFromFileDescriptor std_out{STDOUT_FILENO};
    std::unique_ptr<ShellCommand> pager_cmd;

    /// The user can specify to redirect query output to a file.
    std::unique_ptr<WriteBuffer> out_file_buf;
    std::shared_ptr<IOutputFormat> output_format;

    /// The user could specify special file for server logs (stderr by default)
    std::unique_ptr<WriteBuffer> out_logs_buf;
    String server_logs_file;
    std::unique_ptr<InternalTextLogs> logs_out_stream;

    String home_path;
    String history_file; /// Path to a file containing command history.

    String current_profile;

    UInt64 server_revision = 0;
    String server_version;
    String prompt_by_server_display_name;
    String server_display_name;

    ProgressIndication progress_indication;
    bool need_render_progress = true;
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

    bool fake_drop = false;

    struct HostAndPort
    {
        String host;
        std::optional<UInt16> port;
    };

    std::vector<HostAndPort> hosts_and_ports{};

    bool allow_repeated_settings = false;

    bool cancelled = false;

    bool logging_initialized = false;
};

}
