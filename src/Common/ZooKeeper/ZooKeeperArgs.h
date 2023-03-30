#pragma once
#include <Common/ZooKeeper/Types.h>
#include <Common/ZooKeeper/ZooKeeperConstants.h>
#include <Common/GetPriorityForLoadBalancing.h>

namespace Poco::Util
{
    class AbstractConfiguration;
}

namespace zkutil
{

struct ZooKeeperArgs
{
    ZooKeeperArgs(const Poco::Util::AbstractConfiguration & config, const String & config_name);

    /// hosts_string -- comma separated [secure://]host:port list
    ZooKeeperArgs(const String & hosts_string);
    ZooKeeperArgs() = default;
    bool operator == (const ZooKeeperArgs &) const = default;

    String implementation = "zookeeper";
    Strings hosts;
    String auth_scheme;
    String identity;
    String chroot;
    int32_t connection_timeout_ms = Coordination::DEFAULT_CONNECTION_TIMEOUT_MS;
    int32_t session_timeout_ms = Coordination::DEFAULT_SESSION_TIMEOUT_MS;
    int32_t operation_timeout_ms = Coordination::DEFAULT_OPERATION_TIMEOUT_MS;
    double send_fault_probability = 0.0;
    double recv_fault_probability = 0.0;

    DB::GetPriorityForLoadBalancing get_priority_load_balancing;
};

}
