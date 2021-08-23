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

protected:
    void processSingleQuery(const String & query_to_execute, ASTPtr parsed_query) override;
    bool processMultiQuery(const String & all_queries_text) override;

    void processError(const String & query) const override;
    void loadSuggestionData(Suggest &) override;

    void connect() override;
    int mainImpl() override;

    String getQueryTextPrefix() override;
    void printHelpMessage(const OptionsDescription & options_description) override;

    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &) override;
    void addAndCheckOptions(OptionsDescription & options_description, po::variables_map & options, Arguments & arguments) override;
    void processOptions(const OptionsDescription & options_description, const CommandLineOptions & options,
                        const std::vector<Arguments> &) override;
    void processConfig() override;

private:
    /** Composes CREATE subquery based on passed arguments (--structure --file --table and --input-format)
      * This query will be executed first, before queries passed through --query argument
      * Returns empty string if it cannot compose that query.
      */
    std::string getInitialCreateTableQuery();

    void tryInitPath();
    void setupUsers();
    void cleanup();

    void applyCmdOptions(ContextMutablePtr context);
    void applyCmdSettings(ContextMutablePtr context);

    std::optional<StatusFile> status;
    std::optional<std::filesystem::path> temporary_directory_to_delete;
};

}
