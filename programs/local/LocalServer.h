#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <loggers/Loggers.h>
#include <Poco/Util/Application.h>
#include <Common/ProgressIndication.h>
#include <Client/ClientBase.h>

namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public ClientBase, public Loggers
{
public:
    LocalServer();

    ~LocalServer() override
    {
        if (global_context)
            global_context->shutdown(); /// required for properly exception handling
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

protected:
    void processMainImplException(const Exception & e) override;

    void initializeChild() override;

    int childMainImpl() override;

    bool isInteractive() override;

    bool processQueryFromInteractive(const String & input) override
    {
        return processQueryText(input);
    }

    void executeParsedQueryImpl() override;

    void reportQueryError() const override;

    void printHelpMessage(const OptionsDescription & options_description) override;

    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &) override;

    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description,
                        const CommandLineOptions & options,
                        const std::vector<Arguments> &) override;
    void processConfig() override;

    bool supportPasswordOption() const override { return false; }

    bool processFile(const String & file) override
    {
        auto text = getInitialCreateTableQuery();
        String queries_from_file;
        ReadBufferFromFile in(file);
        readStringUntilEOF(queries_from_file, in);
        text += queries_from_file;
        return processMultiQuery(text);
    }

private:
    ContextMutablePtr query_context;

    std::exception_ptr exception;

    void processQuery(const String & query, std::exception_ptr exception);

    std::optional<std::filesystem::path> temporary_directory_to_delete;
};

}
