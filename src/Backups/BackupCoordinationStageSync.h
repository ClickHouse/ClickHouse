#pragma once

#include <Backups/WithRetries.h>

namespace DB
{

/// Used to coordinate hosts so all hosts would come to a specific stage at around the same time.
class BackupCoordinationStageSync
{
public:
    BackupCoordinationStageSync(
        const String & root_zookeeper_path_,
        const String & current_host_,
        const WithRetries & with_retries_,
        LoggerPtr log_);

    /// Sets the stage of the current host and signal other hosts if there were other hosts waiting for that.
    void setStage(const String & stage, const String & stage_result);
    void setError(const Exception & exception);

    /// Sets the stage of the current host and waits until all hosts come to the same stage.
    /// The function returns the messages all hosts set when they come to the required stage.
    Strings wait(const Strings & all_hosts, const String & stage_to_wait);

    /// Almost the same as setAndWait() but this one stops waiting and throws an exception after a specific amount of time.
    Strings waitFor(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout);

private:
    void createRootNodes();
    void readState();

    Strings waitImpl(const Strings & all_hosts, const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) const;

    const String zookeeper_path;
    /// A reference to the field of parent object - BackupCoordinationRemote or RestoreCoordinationRemote
    const WithRetries & with_retries;
    const LoggerPtr log;

    struct HostInfo
    {
        String host;
        bool started = false;
        bool connected = false;
        std::optional<std::chrono::time_point<std::chrono::system_clock>> last_time_connected;
        std::unordered_map<String, String> stage_results;
    };

    struct HostAndError
    {
        String host;
        Exception exception;
    };

    struct State
    {
        std::map<String, HostInfo> hosts_info;
        std::optional<HostAndError> error;
    };

    State state TSA_GUARDED_BY(mutex);
    std::conditional_variable state_changed;
    std::mutex mutex;

    Poco::Event zk_nodes_changed;
};

}
