#pragma once

#include <Client/ClientBase.h>
#include <Client/LocalConnection.h>

#include <Common/ProgressIndication.h>
#include <Common/StatusFile.h>
#include <Common/InterruptListener.h>
#include <loggers/Loggers.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

#include <filesystem>
#include <memory>
#include <optional>


namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public ClientBase, public Loggers
{
public:
    LocalServer() = default;

    void initialize(Poco::Util::Application & self) override;

    ~LocalServer() override
    {
        if (global_context)
            global_context->shutdown(); /// required for properly exception handling
    }

protected:
    void connect() override
    {
        connection_parameters = ConnectionParameters(config());
        connection = std::make_unique<LocalConnection>(global_context);
    }

    void processSingleQuery(const String & full_query) override;

    bool processMultiQuery(const String & all_queries_text) override;


    void reportQueryError(const String & query) const override;

    void executeSingleQuery(const String & query_to_execute, ASTPtr parsed_query) override;

    String getQueryTextPrefix() override;

    void loadSuggestionData(Suggest &) override;


    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &) override;

    void printHelpMessage(const OptionsDescription & options_description) override;

    void addAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments) override;

    void processOptions(const OptionsDescription & options_description,
                        const CommandLineOptions & options,
                        const std::vector<Arguments> &) override;

    void processConfig() override;

    int mainImpl() override;

private:
    /** Composes CREATE subquery based on passed arguments (--structure --file --table and --input-format)
      * This query will be executed first, before queries passed through --query argument
      * Returns empty string if it cannot compose that query.
      */
    std::string getInitialCreateTableQuery();

    void tryInitPath();

    void applyCmdOptions(ContextMutablePtr context);

    void applyCmdSettings(ContextMutablePtr context);

    void processQueries();

    void setupUsers();

    void cleanup();

    void checkInterruptListener();

    ContextMutablePtr query_context;

    std::optional<StatusFile> status;

    std::unique_ptr<Exception> local_server_exception;

    void processQuery(const String & query, std::exception_ptr exception);

    std::optional<std::filesystem::path> temporary_directory_to_delete;

    /// Used to cancel query on ctrl-c. Lives for single query execution.
    std::optional<InterruptListener> interrupt_listener;

    std::mutex interrupt_listener_mutex;

    /// Was currently executed query cancelled by ctrl-c in interactive mode?
    bool cancelled = true;
};

}
