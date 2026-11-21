#pragma once

#include <Common/Config/ConfigProcessor.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/ZooKeeper/ZooKeeperImpl.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <filesystem>
#include <optional>
#include <string>
#include <vector>

using Strings = std::vector<std::string>;

/// Returns the value of a benchmark option: the command line takes precedence
/// over the config, which takes precedence over the default.
template <typename T>
T getOption(const std::optional<T> & cli_value, const DB::ConfigurationPtr & config, const std::string & key, T default_value)
{
    if (cli_value)
        return *cli_value;

    if (!config || !config->has(key))
        return default_value;

    if constexpr (std::is_same_v<T, bool>)
        return config->getBool(key);
    else if constexpr (std::is_floating_point_v<T>)
        return static_cast<T>(config->getDouble(key));
    else
        return static_cast<T>(config->getUInt64(key));
}

struct ConnectionInfo
{
    std::string host;

    bool secure = false;
    int32_t session_timeout_ms = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
    int32_t connection_timeout_ms = Coordination::DEFAULT_CONNECTION_TIMEOUT_MS;
    int32_t operation_timeout_ms = Coordination::DEFAULT_OPERATION_TIMEOUT_MS;
    bool use_compression = false;
    bool use_xid_64 = false;

    size_t sessions = 1;
};

/// Holds per-host connection settings (from `--hosts` or the `connections`
/// config section) and creates ZooKeeper client connections from them.
class ConnectionFactory
{
public:
    /// `hosts_strings` (from the command line) takes precedence over the config.
    void initialize(const Strings & hosts_strings, const Poco::Util::AbstractConfiguration * config, bool enable_tracing_);

    const std::vector<ConnectionInfo> & connectionInfos() const { return connection_infos; }

    std::shared_ptr<Coordination::ZooKeeper> getConnection(const ConnectionInfo & connection_info, size_t connection_info_idx) const;

private:
    void parseHostsFromConfig(const Poco::Util::AbstractConfiguration & config);

    ConnectionInfo default_connection_info;
    std::vector<ConnectionInfo> connection_infos;
    bool enable_tracing = false;
};

/// Where to write the final JSON report, parsed from the `output` config section.
class BenchmarkOutput
{
public:
    void initializeFromConfig(const Poco::Util::AbstractConfiguration & config);
    void write(const std::string & output_string, int64_t start_timestamp_ms) const;

private:
    bool print_to_stdout = false;
    std::optional<std::filesystem::path> file_output;
    bool output_file_with_timestamp = false;
};
