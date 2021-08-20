#pragma once

#include <boost/program_options.hpp>
#include <Poco/Util/Application.h>
#include <Interpreters/Context.h>
#include <Common/ProgressIndication.h>
#include <Client/Suggest.h>
#include <Client/QueryFuzzer.h>
#include <Common/ShellCommand.h>
#include <Core/ExternalTable.h>

namespace po = boost::program_options;


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class ClientBase : public Poco::Util::Application
{

public:
    using Arguments = std::vector<String>;

    void init(int argc, char ** argv);
    int main(const std::vector<String> & /*args*/) override;

protected:
    void runInteractive();
    void runNonInteractive();

    virtual bool processWithFuzzing(const String &)
    {
        throw Exception("Query processing with fuzzing is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }

    /// Process single query.
    virtual void executeSingleQuery(const String & query_to_execute, ASTPtr parsed_query) = 0;

    /// Methods, which can be called from executeSingleQuery.
    void processOrdinaryQuery(const String & query_to_execute, ASTPtr parsed_query);
    void processInsertQuery(const String & query_to_execute, ASTPtr parsed_query);

    void processParsedSingleQuery(const String & full_query, const String & query_to_execute,
        ASTPtr parsed_query, std::optional<bool> echo_query_ = {}, bool report_error = false);

    virtual void processError(const String & query) const = 0;
    virtual void loadSuggestionData(Suggest &) = 0;

    enum MultiQueryProcessingStage
    {
        QUERIES_END,
        PARSING_EXCEPTION,
        CONTINUE_PARSING,
        EXECUTE_QUERY,
        PARSING_FAILED,
    };

    virtual void connect() {}

    MultiQueryProcessingStage analyzeMultiQueryText(
        const char *& this_query_begin, const char *& this_query_end, const char * all_queries_end,
        String & query_to_execute, ASTPtr & parsed_query, const String & all_queries_text,
        std::optional<Exception> & current_exception);

    /// For non-interactive multi-query mode get queries text prefix.
    virtual String getQueryTextPrefix() { return ""; }


    void resetOutput();

    ASTPtr parseQuery(const char *& pos, const char * end, bool allow_multi_statements) const;

    /// Prepare for and call either runInteractive() or runNonInteractive().
    virtual int mainImpl() = 0;

    using ProgramOptionsDescription = boost::program_options::options_description;
    using CommandLineOptions = boost::program_options::variables_map;

    struct OptionsDescription
    {
        std::optional<ProgramOptionsDescription> main_description;
        std::optional<ProgramOptionsDescription> external_description;
    };

    virtual void printHelpMessage(const OptionsDescription & options_description) = 0;
    virtual void readArguments(int argc, char ** argv,
                               Arguments & common_arguments, std::vector<Arguments> &) = 0;
    virtual void addAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments) = 0;
    virtual void processOptions(const OptionsDescription & options_description,
                                const CommandLineOptions & options,
                                const std::vector<Arguments> & external_tables_arguments) = 0;
    virtual void processConfig() = 0;

private:
    bool processQueryText(const String & text);
    void processTextAsSingleQuery(const String & full_query);
    bool processMultiQuery(const String & all_queries_text);

    void receiveResult(ASTPtr parsed_query);
    bool receiveAndProcessPacket(ASTPtr parsed_query, bool cancelled);
    void receiveLogs(ASTPtr parsed_query);
    bool receiveSampleBlock(Block & out, ColumnsDescription & columns_description, ASTPtr parsed_query);
    bool receiveEndOfQuery();

    void onProgress(const Progress & value);
    void onData(Block & block, ASTPtr parsed_query);
    void onLogData(Block & block);
    void onTotals(Block & block, ASTPtr parsed_query);
    void onExtremes(Block & block, ASTPtr parsed_query);
    void onReceiveExceptionFromServer(std::unique_ptr<Exception> && e);
    void onProfileInfo(const BlockStreamProfileInfo & profile_info);
    void onEndOfStream();

    void sendData(Block & sample, const ColumnsDescription & columns_description, ASTPtr parsed_query);

    void sendDataFrom(ReadBuffer & buf, Block & sample,
                      const ColumnsDescription & columns_description, ASTPtr parsed_query);

    void initBlockOutputStream(const Block & block, ASTPtr parsed_query);
    void initLogsOutputStream();

    void sendExternalTables(ASTPtr parsed_query);

    inline String prompt() const
    {
        return boost::replace_all_copy(prompt_by_server_display_name, "{database}", config().getString("database", "default"));
    }

    void outputQueryInfo(bool echo_query_);

protected:
    bool is_interactive = false; /// Use either interactive line editing interface or batch mode.
    bool is_multiquery = false;

    bool echo_queries = false; /// Print queries before execution in batch mode.
    bool ignore_error = false; /// In case of errors, don't print error message, continue to next query. Only applicable for non-interactive mode.
    bool print_time_to_stderr = false; /// Output execution time to stderr in batch mode.

    String home_path;
    std::vector<String> queries_files; /// If not empty, queries will be read from these files
    String history_file; /// Path to a file containing command history.
    std::vector<String> interleave_queries_files; /// If not empty, run queries from these files before processing every file from 'queries_files'.

    bool has_vertical_output_suffix = false; /// Is \G present at the end of the query string?
    String prompt_by_server_display_name;
    String server_display_name;

    ProgressIndication progress_indication;
    bool need_render_progress = true;
    bool written_first_block = false;
    size_t processed_rows = 0; /// How many rows have been read or written.

    bool stdin_is_a_tty = false; /// stdin is a terminal.
    bool stdout_is_a_tty = false; /// stdout is a terminal.
    uint64_t terminal_width = 0;

    /// Settings specified via command line args
    Settings cmd_settings;

    SharedContextHolder shared_context;
    ContextMutablePtr global_context;

    QueryFuzzer fuzzer;
    int query_fuzzer_runs = 0;

    /// If the last query resulted in exception. `server_exception` or
    /// `client_exception` must be set.
    bool have_error = false;

    /// Buffer that reads from stdin in batch mode.
    ReadBufferFromFileDescriptor std_in{STDIN_FILENO};
    /// Console output.
    WriteBufferFromFileDescriptor std_out{STDOUT_FILENO};
    std::unique_ptr<ShellCommand> pager_cmd;

    /// The user can specify to redirect query output to a file.
    std::unique_ptr<WriteBuffer> out_file_buf;
    BlockOutputStreamPtr block_out_stream;

    /// The user could specify special file for server logs (stderr by default)
    std::unique_ptr<WriteBuffer> out_logs_buf;
    String server_logs_file;
    BlockOutputStreamPtr logs_out_stream;

    /// We will format query_id in interactive mode in various ways, the default is just to print Query id: ...
    std::vector<std::pair<String, String>> query_id_formats;

    /// Dictionary with query parameters for prepared statements.
    NameToNameMap query_parameters;

    std::unique_ptr<IServerConnection> connection;
    ConnectionParameters connection_parameters;

    String format; /// Query results output format.
    bool is_default_format = true; /// false, if format is set in the config or command line.

    /// The last exception that was received from the server. Is used for the
    /// return code in batch mode.
    std::unique_ptr<Exception> server_exception;
    /// Likewise, the last exception that occurred on the client.
    std::unique_ptr<Exception> client_exception;

    QueryProcessingStage::Enum query_processing_stage;

    /// External tables info.
    std::list<ExternalTable> external_tables;
    String current_profile;

    size_t format_max_block_size = 0; /// Max block size for console output.
    String insert_format; /// Format of INSERT data that is read from stdin in batch mode.
    size_t insert_format_max_block_size = 0; /// Max block size when reading INSERT data.
    size_t max_client_network_bandwidth = 0; /// The maximum speed of data exchange over the network for the client in bytes per second.

    UInt64 server_revision = 0;
    String server_version;

};

}
