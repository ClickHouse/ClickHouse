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

    void shutdown() override
    {
        try
        {
            cleanup();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }

    String getQueryTextPrefix() override
    {
        return getInitialCreateTableQuery();
    }

    void executeParsedQueryImpl() override;

    void reportQueryError() const override {}

    void printHelpMessage(const OptionsDescription & options_description) override;

    bool supportPasswordOption() const override { return false; }

    bool processMultiQueryFromFile(const String & file) override
    {
        auto text = getInitialCreateTableQuery();
        String queries_from_file;
        ReadBufferFromFile in(file);
        readStringUntilEOF(queries_from_file, in);
        text += queries_from_file;
        return processMultiQuery(text);
    }

    void checkExceptions() override
    {
        if (exception)
            std::rethrow_exception(exception);
    }

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

    std::exception_ptr exception;

    void processQuery(const String & query, std::exception_ptr exception);

    std::optional<std::filesystem::path> temporary_directory_to_delete;
};

}
