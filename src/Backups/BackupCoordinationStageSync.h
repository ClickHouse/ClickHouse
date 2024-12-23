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
        WithRetries & with_retries_,
        LoggerPtr log_);

    /// Sets the stage of the current host and signal other hosts if there were other hosts waiting for that.
    void set(const String & current_host, const String & new_stage, const String & message, const bool & all_hosts = false);
    void setError(const String & current_host, const Exception & exception);

    /// Sets the stage of the current host and waits until all hosts come to the same stage.
    /// The function returns the messages all hosts set when they come to the required stage.
    Strings wait(const Strings & all_hosts, const String & stage_to_wait);

    /// Almost the same as setAndWait() but this one stops waiting and throws an exception after a specific amount of time.
    Strings waitFor(const Strings & all_hosts, const String & stage_to_wait, std::chrono::milliseconds timeout);

private:
    void createRootNodes();

    struct State;
    State readCurrentState(WithRetries::RetriesControlHolder & retries_control_holder, const Strings & zk_nodes, const Strings & all_hosts, const String & stage_to_wait) const;

    Strings waitImpl(const Strings & all_hosts, const String & stage_to_wait, std::optional<std::chrono::milliseconds> timeout) const;

    String zookeeper_path;
    /// A reference to the field of parent object - BackupCoordinationRemote or RestoreCoordinationRemote
    WithRetries & with_retries;
    LoggerPtr log;
};

}
