#pragma once

#include <boost/program_options.hpp>
#include <Poco/Util/Application.h>
#include <Core/Names.h>
#include <Core/Types.h>
#include <Interpreters/Context.h>
#include <Common/ProgressIndication.h>
#include <Client/Suggest.h>
#include <Client/QueryFuzzer.h>
#include <Client/TestHint.h>
#include <Common/ShellCommand.h>

#if USE_REPLXX
#   include <common/ReplxxLineReader.h>
#elif defined(USE_READLINE) && USE_READLINE
#   include <common/ReadlineLineReader.h>
#else
#   include <common/LineReader.h>
#endif


namespace DB
{

class ClientBase : public Poco::Util::Application
{

public:
    using Arguments = std::vector<String>;

    void init(int argc, char ** argv);

    int main(const std::vector<String> & /*args*/) override;

protected:
    /*
     * Run query in interactive or non-interactive mode. Depends on:
     *  - processSingleQuery
     *  - processMultiQuery
     *  - processWithFuzzing
     */
    void runInteractive();

    void runNonInteractive();

    /*
     * full_query - current query as it was given to the client.
     * parsed_query - parsed query (used to determine some settings e.g. format, output file).
     * query_to_execute - current query as it will be executed by server.
     *                    (It may differ from the full query for INSERT queries, for which the
     *                    data that follows the query is stripped and sent separately.)
    **/


    /*
     * Process multiquery - several queries separated by ';'.
     * Also in case of clickhouse-server:
     * - INSERT data is ended by the end of line, not ';'.
     * - An exception is VALUES format where we also support semicolon in addition to end of line.
    **/
    bool processMultiQueryImpl(const String & all_queries_text,
                               std::function<void(const String & full_query, const String & query_to_execute, ASTPtr parsed_query)> execute_single_query,
                               std::function<void(const String &, Exception &)> process_parse_query_error = {});

    /// Process parsed single query.
    void processSingleQueryImpl(const String & query,
                                std::function<void()> execute_single_query,
                                std::optional<bool> echo_query_ = {}, bool report_error = false);

    /*
     * Method to implement multi-query processing.
     * Must make some preparation and then call processMultiQueryImpl. Afterwards it might execute some finishing code.
    **/
    virtual bool processMultiQuery(const String & all_queries_text) = 0;

    /*
     * Method to implement single-query processing.
     * Must make some preparation and then call processSingleQueryImpl. Afterwards it might execute some finishing code.
    **/
    virtual void processSingleQuery(const String & query) = 0;

    virtual bool processWithFuzzing(const String &)
    {
        throw Exception("Query processing with fuzzing is not implemented", ErrorCodes::NOT_IMPLEMENTED);
    }


    virtual void reportQueryError(const String & query) const = 0;

    /// For non-interactive multi-query mode get queries text prefix.
    virtual String getQueryTextPrefix() { return ""; }

    virtual void loadSuggestionData() {}


    /// Parse query text for multiquery mode.
    ASTPtr parseQuery(const char *& pos, const char * end, bool allow_multi_statements) const;
    void resetOutput();
    virtual void reconnectIfNeeded() {}
    virtual bool validateParsedOptions() { return false; }


    /// Prepare for and call either runInteractive() or runNonInteractive().
    virtual int mainImpl() = 0;

    virtual void readArguments(int argc, char ** argv,
                               Arguments & common_arguments, std::vector<Arguments> &) = 0;

    using ProgramOptionsDescription = boost::program_options::options_description;
    using CommandLineOptions = boost::program_options::variables_map;

    struct OptionsDescription
    {
        std::optional<ProgramOptionsDescription> main_description;
        std::optional<ProgramOptionsDescription> external_description;
    };

    virtual void printHelpMessage(const OptionsDescription & options_description) = 0;

    virtual void addOptions(OptionsDescription & options_description) = 0;

    virtual void processOptions(const OptionsDescription & options_description,
                                const CommandLineOptions & options,
                                const std::vector<Arguments> & external_tables_arguments) = 0;

    virtual void processConfig() = 0;

private:
    inline String prompt() const
    {
        return boost::replace_all_copy(prompt_by_server_display_name, "{database}", config().getString("database", "default"));
    }

    void outputQueryInfo(bool echo_query_);

    /// Process query text (multiquery or single query) accroding to options.
    bool processQueryText(const String & text);


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

    std::optional<Suggest> suggest;

    /// If the last query resulted in exception. `server_exception` or
    /// `client_exception` must be set.
    bool have_error = false;
    /// The last exception that was received from the server. Is used for the
    /// return code in batch mode.
    std::unique_ptr<Exception> server_exception;
    /// Likewise, the last exception that occurred on the client.
    std::unique_ptr<Exception> client_exception;

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

    /// We will format query_id in interactive mode in various ways, the default is just to print Query id: ...
    std::vector<std::pair<String, String>> query_id_formats;
    QueryProcessingStage::Enum query_processing_stage;

    String current_profile;

private:
    NameSet exit_strings{"exit", "quit", "logout", "учше", "йгше", "дщпщге", "exit;", "quit;", "logout;", "учшеж",
                         "йгшеж", "дщпщгеж", "q", "й", "\\q", "\\Q", "\\й", "\\Й", ":q", "Жй"};

};

}
