#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <loggers/Loggers.h>
#include <Poco/Util/Application.h>
#include <Common/ProgressIndication.h>
#include <Client/IClient.h>

namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public IClient
{
public:
    LocalServer();

    void initialize(Poco::Util::Application & self) override;

    ~LocalServer() override;

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

    int childMainImpl() override;

    bool isInteractive() override;

    bool processQueryFromInteractive(const String & input) override
    {
        if (exit_strings.end() != exit_strings.find(trim(input, [](char c) { return isWhitespaceASCII(c) || c == ';'; })))
            return false;

        processQueryText(input);
        return true;
    }

    void processTextAsSingleQuery(const String & input) override
    {
        std::exception_ptr e;
        processQuery(input, e);
    }

    void processOrdinaryQuery() override
    {
        std::exception_ptr e;
        processQuery(query_to_send, e);
    }

    void reportQueryError() const override;

    void printHelpMessage(const OptionsDescription & options_description) override;

    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &) override;

    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description,
                        const CommandLineOptions & options,
                        const std::vector<Arguments> &) override;

    bool supportPasswordOption() const override { return false; }

    bool splitQueryIntoParts() const override { return false; }

    std::optional<std::filesystem::path> temporary_directory_to_delete;

private:
    ContextMutablePtr query_context;

    void processQuery(const String & query, std::exception_ptr exception);
};

}
