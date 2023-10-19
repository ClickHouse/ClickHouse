#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <base/find_symbols.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/isLocalAddress.h>
#include <Poco/String.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace zkutil
{

ZooKeeperArgs::ZooKeeperArgs(const Poco::Util::AbstractConfiguration & config, const String & config_name)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    for (const auto & key : keys)
    {
        if (key.starts_with("node"))
        {
            hosts.push_back(
                (config.getBool(config_name + "." + key + ".secure", false) ? "secure://" : "")
                + config.getString(config_name + "." + key + ".host") + ":" + config.getString(config_name + "." + key + ".port", "2181"));
        }
        else if (key == "session_timeout_ms")
        {
            session_timeout_ms = config.getInt(config_name + "." + key);
        }
        else if (key == "operation_timeout_ms")
        {
            operation_timeout_ms = config.getInt(config_name + "." + key);
        }
        else if (key == "connection_timeout_ms")
        {
            connection_timeout_ms = config.getInt(config_name + "." + key);
        }
        else if (key == "enable_fault_injections_during_startup")
        {
            enable_fault_injections_during_startup = config.getBool(config_name + "." + key);
        }
        else if (key == "send_fault_probability")
        {
            send_fault_probability = config.getDouble(config_name + "." + key);
        }
        else if (key == "recv_fault_probability")
        {
            recv_fault_probability = config.getDouble(config_name + "." + key);
        }
        else if (key == "send_sleep_probability")
        {
            send_sleep_probability = config.getDouble(config_name + "." + key);
        }
        else if (key == "recv_sleep_probability")
        {
            recv_sleep_probability = config.getDouble(config_name + "." + key);
        }
        else if (key == "send_sleep_ms")
        {
            send_sleep_ms = config.getUInt64(config_name + "." + key);
        }
        else if (key == "recv_sleep_ms")
        {
            recv_sleep_ms = config.getUInt64(config_name + "." + key);
        }
        else if (key == "identity")
        {
            identity = config.getString(config_name + "." + key);
            if (!identity.empty())
                auth_scheme = "digest";
        }
        else if (key == "root")
        {
            chroot = config.getString(config_name + "." + key);
        }
        else if (key == "implementation")
        {
            implementation = config.getString(config_name + "." + key);
        }
        else if (key == "zookeeper_load_balancing")
        {
            String load_balancing_str = config.getString(config_name + "." + key);
            /// Use magic_enum to avoid dependency from dbms (`SettingFieldLoadBalancingTraits::fromString(...)`)
            auto load_balancing = magic_enum::enum_cast<DB::LoadBalancing>(Poco::toUpper(load_balancing_str));
            if (!load_balancing)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown load balancing: {}", load_balancing_str);
            get_priority_load_balancing.load_balancing = *load_balancing;
        }
        else
            throw KeeperException(std::string("Unknown key ") + key + " in config file", Coordination::Error::ZBADARGUMENTS);
    }

    if (!chroot.empty())
    {
        if (chroot.front() != '/')
            throw KeeperException(
                Coordination::Error::ZBADARGUMENTS,
                "Root path in config file should start with '/', but got {}", chroot);
        if (chroot.back() == '/')
            chroot.pop_back();
    }

    if (session_timeout_ms < 0 || operation_timeout_ms < 0 || connection_timeout_ms < 0)
        throw KeeperException("Timeout cannot be negative", Coordination::Error::ZBADARGUMENTS);

    /// init get_priority_load_balancing
    get_priority_load_balancing.hostname_differences.resize(hosts.size());
    const String & local_hostname = getFQDNOrHostName();
    for (size_t i = 0; i < hosts.size(); ++i)
    {
        const String & node_host = hosts[i].substr(0, hosts[i].find_last_of(':'));
        get_priority_load_balancing.hostname_differences[i] = DB::getHostNameDifference(local_hostname, node_host);
    }
}

ZooKeeperArgs::ZooKeeperArgs(const String & hosts_string)
{
    splitInto<','>(hosts, hosts_string);
}

}
