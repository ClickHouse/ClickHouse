#pragma once

#include <filesystem>
#include <memory>
#include <optional>

#include <Common/ProgressIndication.h>
#include <Common/StatusFile.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <loggers/Loggers.h>
#include <Client/ClientBase.h>
#include <Poco/Util/Application.h>
#include <IO/WriteBufferFromOStream.h>


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
    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &) override;

    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description,
                        const CommandLineOptions & options,
                        const std::vector<Arguments> &) override;

    void processConfig() override;

    int mainImpl() override;


    bool processMultiQuery(const String & all_queries_text) override
    {
        auto process_single_query = [&](const String & query_to_execute, const String &, ASTPtr)
        {
            processSingleQueryImpl(all_queries_text, query_to_execute, nullptr, echo_queries, false);
        };
        return processMultiQueryImpl(all_queries_text, process_single_query);
    }

    void processSingleQuery(const String & full_query) override
    {
        ASTPtr parsed_query;
        if (is_interactive)
        {
            auto this_query_begin = full_query.data();
            parsed_query = parseQuery(this_query_begin, full_query.data() + full_query.size(), false);
        }
        processSingleQueryImpl(full_query, full_query, parsed_query, echo_queries);
    }


    String getQueryTextPrefix() override
    {
        return getInitialCreateTableQuery();
    }

    void executeSingleQuery(const String & query_to_execute, ASTPtr parsed_query) override;

    void reportQueryError(const String &) const override {}

    void printHelpMessage(const OptionsDescription & options_description) override;

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

    ContextMutablePtr query_context;

    std::optional<StatusFile> status;

    std::exception_ptr local_server_exception;

    void processQuery(const String & query, std::exception_ptr exception);

    std::optional<std::filesystem::path> temporary_directory_to_delete;
};

}
