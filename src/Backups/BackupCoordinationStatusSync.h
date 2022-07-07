#pragma once

#include <Common/ZooKeeper/Common.h>


namespace DB
{

/// Used to coordinate hosts so all hosts would come to a specific status at around the same time.
class BackupCoordinationStatusSync
{
public:
    BackupCoordinationStatusSync(const String & zookeeper_path_, zkutil::GetZooKeeper get_zookeeper_, Poco::Logger * log_);

    /// Sets the status of the current host and signal other hosts if there were other hosts waiting for that.
    void set(const String & current_host, const String & new_status, const String & message);

    /// Sets the status of the current host and waits until all hosts come to the same status.
    /// The function returns the messages all hosts set when they come to the required status.
    Strings setAndWait(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts);

    /// Almost the same as setAndWait() but this one stops waiting and throws an exception after a specific amount of time.
    Strings setAndWaitFor(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, UInt64 timeout_ms);

    static constexpr const char * kErrorStatus = "error";

private:
    void createRootNodes();
    Strings setImpl(const String & current_host, const String & new_status, const String & message, const Strings & all_hosts, const std::optional<UInt64> & timeout_ms);

    String zookeeper_path;
    zkutil::GetZooKeeper get_zookeeper;
    Poco::Logger * log;
};

}
