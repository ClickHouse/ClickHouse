#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <base/find_symbols.h>
#include <Poco/Util/AbstractConfiguration.h>

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
        else if (key == "send_fault_probability")
        {
            send_fault_probability = config.getDouble(config_name + "." + key);
        }
        else if (key == "recv_fault_probability")
        {
            recv_fault_probability = config.getDouble(config_name + "." + key);
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
        else
            throw KeeperException(std::string("Unknown key ") + key + " in config file", Coordination::Error::ZBADARGUMENTS);
    }

    if (!chroot.empty())
    {
        if (chroot.front() != '/')
            throw KeeperException(
                std::string("Root path in config file should start with '/', but got ") + chroot, Coordination::Error::ZBADARGUMENTS);
        if (chroot.back() == '/')
            chroot.pop_back();
    }
}

ZooKeeperArgs::ZooKeeperArgs(const String & hosts_string)
{
    splitInto<','>(hosts, hosts_string);
}

}
