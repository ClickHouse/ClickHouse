#pragma once

#include <Client/ClientApplicationBase.h>
#include <Client/LocalConnection.h>

#include <Core/ServerSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Loggers/Loggers.h>
#include <Server/IServer.h>
#include <Server/ProtocolServerAdapter.h>
#include <Common/MemoryWorker.h>
#include <Common/StatusFile.h>

#include <filesystem>
#include <memory>
#include <mutex>
#include <optional>


namespace Poco { class ThreadPool; }

namespace DB
{

class AsynchronousMetrics;
class ServerType;

/// Lightweight Application for clickhouse-local.
/// No networking by default; TCP/HTTP listeners can be started explicitly via `SYSTEM START LISTEN`.
/// No extra configs and working directories, no pid and status files, no dictionaries, no logging.
/// Quiet mode by default
class LocalServer : public ClientApplicationBase, public Loggers, public IServer
{
public:
    LocalServer() = default;

    void initialize(Poco::Util::Application & self) override;

    int main(const std::vector<String> & /*args*/) override;
    bool supportsLocalMetaCommands() const override { return true; }

    /// IServer interface
    Poco::Util::LayeredConfiguration & config() const override { return ClientApplicationBase::config(); }
    Poco::Logger & logger() const override { return ClientApplicationBase::logger(); }
    ContextMutablePtr context() const override { return global_context; }
    bool isCancelled() const override { return is_cancelled; }

protected:
    Poco::Util::LayeredConfiguration & getClientConfiguration() override;

    void connect() override;

    void processError(std::string_view query) const override;

    String getName() const override { return "local"; }

    void printHelpMessage(const OptionsDescription & options_description) override;

    void addExtraOptions(OptionsDescription & options_description) override;

    void processOptions(const OptionsDescription & options_description, const CommandLineOptions & options,
                        const std::vector<Arguments> &, const std::vector<Arguments> &) override;

    void processConfig() override;
    void readArguments(int argc, char ** argv, Arguments & common_arguments, std::vector<Arguments> &, std::vector<Arguments> &) override;

    void updateLoggerLevel(const String & logs_level) override;

private:
    String getHelpHeader() const;
    String getHelpFooter() const;
    /** Composes CREATE subquery based on passed arguments (--structure --file --table and --input-format)
      * This query will be executed first, before queries passed through --query argument
      * Returns a pair of the table name and the corresponding create table statement.
      * Returns empty strings if it cannot compose that query.
      */
    std::pair<std::string, std::string> getInitialCreateTableQuery();

    void tryInitPath();
    void setupUsers();
    void cleanup();

    void applyCmdOptions(ContextMutablePtr context);
    void applyCmdSettings(ContextMutablePtr context);

    void createClientContext();

    void startServers(const ServerType & server_type);
    void stopServers(const ServerType & server_type);

    ServerSettings server_settings;

    /// Path of the config file actually loaded in `initialize`. Empty if no config file was loaded.
    /// Tracks loads from all sources: `--config-file` flag, `./config.xml`, and `getLocalConfigPath`
    /// (`./clickhouse-local.{xml,yaml,yml}`, `~/.clickhouse-local/config.{xml,yaml,yml}`,
    /// `/etc/clickhouse-local/config.{xml,yaml,yml}`). Needed by `setupUsers` to resolve relative
    /// paths in `user_directories.users_xml.path` against the config's own directory.
    String loaded_config_path;

    std::optional<StatusFile> status;
    std::optional<std::filesystem::path> temporary_directory_to_delete;

    std::unique_ptr<ReadBufferFromFile> input;

    /// MemoryWorker periodically updates RSS and resizes the userspace page cache.
    /// Without it the page cache stays stuck at `page_cache_min_size`.
    std::optional<MemoryWorker> memory_worker;

    std::atomic<bool> is_cancelled{false};
    std::vector<ProtocolServerAdapter> servers;
    std::mutex servers_lock;
    std::unique_ptr<Poco::ThreadPool> server_pool;
    std::unique_ptr<AsynchronousMetrics> async_metrics;
};

}
