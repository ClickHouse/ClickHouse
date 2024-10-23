#pragma once

#include <Client/ClientApplicationBase.h>
#include <Client/LocalConnection.h>

#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Loggers/Loggers.h>
#include <Common/InterruptListener.h>
#include <Common/StatusFile.h>

#include <filesystem>
#include <memory>
#include <optional>


namespace DB
{

/// Lightweight Application for clickhouse-local
/// No networking, no extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public ClientApplicationBase, public Loggers
{
public:
    LocalServer() = default;

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & /*args*/) override;

protected:
    Poco::Util::LayeredConfiguration & getClientConfiguration() override;

    void connect() override;

    void processError(const String & query) const override;

    String getName() const override { return "local"; }

    void printHelpMessage(const OptionsDescription & options_description, bool verbose) override;

    void addOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description, const CommandLineOptions & options,
                        const std::vector<Arguments> &, const std::vector<Arguments> &) override;

    void processConfig() override;
    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &) override;

    void updateLoggerLevel(const String & logs_level) override;

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

    void createClientContext();

    ServerSettings server_settings;

    std::optional<StatusFile> status;
    std::optional<std::filesystem::path> temporary_directory_to_delete;

    std::unique_ptr<ReadBufferFromFile> input;
};

}
