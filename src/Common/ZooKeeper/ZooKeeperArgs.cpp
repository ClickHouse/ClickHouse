#include <Common/ZooKeeper/ZooKeeperArgs.h>
#include <Common/ZooKeeper/KeeperException.h>
#include <base/find_symbols.h>
#include <base/getFQDNOrHostName.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/isLocalAddress.h>
#include <Common/StringUtils.h>
#include <Common/thread_local_rng.h>
#include <Server/CloudPlacementInfo.h>
#include <IO/S3/Credentials.h>
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
    if (config_name == "keeper_server")
        initFromKeeperServerSection(config);
    else
        initFromKeeperSection(config, config_name);

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
        throw KeeperException::fromMessage(Coordination::Error::ZBADARGUMENTS, "Timeout cannot be negative");

    /// init get_priority_load_balancing
    get_priority_load_balancing.hostname_prefix_distance.resize(hosts.size());
    get_priority_load_balancing.hostname_levenshtein_distance.resize(hosts.size());
    const String & local_hostname = getFQDNOrHostName();
    for (size_t i = 0; i < hosts.size(); ++i)
    {
        const String & node_host = hosts[i].substr(0, hosts[i].find_last_of(':'));
        get_priority_load_balancing.hostname_prefix_distance[i] = DB::getHostNamePrefixDistance(local_hostname, node_host);
        get_priority_load_balancing.hostname_levenshtein_distance[i] = DB::getHostNameLevenshteinDistance(local_hostname, node_host);
    }
}

ZooKeeperArgs::ZooKeeperArgs(const String & hosts_string)
{
    splitInto<','>(hosts, hosts_string);
    availability_zones.resize(hosts.size());
}

void ZooKeeperArgs::initFromKeeperServerSection(const Poco::Util::AbstractConfiguration & config)
{
    static constexpr std::string_view config_name = "keeper_server";

    if (auto key = std::string{config_name} + ".tcp_port_secure";
        config.has(key))
    {
        auto tcp_port_secure = config.getString(key);

        if (tcp_port_secure.empty())
            throw KeeperException::fromMessage(Coordination::Error::ZBADARGUMENTS, "Empty tcp_port_secure in config file");
    }

    bool secure{false};
    std::string tcp_port;
    if (auto tcp_port_secure_key = std::string{config_name} + ".tcp_port_secure";
        config.has(tcp_port_secure_key))
    {
        secure = true;
        tcp_port = config.getString(tcp_port_secure_key);
    }
    else if (auto tcp_port_key = std::string{config_name} + ".tcp_port";
        config.has(tcp_port_key))
    {
        tcp_port = config.getString(tcp_port_key);
    }

    if (tcp_port.empty())
        throw KeeperException::fromMessage(Coordination::Error::ZBADARGUMENTS, "No tcp_port or tcp_port_secure in config file");

    if (auto coordination_key = std::string{config_name} + ".coordination_settings";
        config.has(coordination_key))
    {
        if (auto operation_timeout_key = coordination_key + ".operation_timeout_ms";
            config.has(operation_timeout_key))
            operation_timeout_ms = config.getInt(operation_timeout_key);

        if (auto session_timeout_key = coordination_key + ".session_timeout_ms";
            config.has(session_timeout_key))
            session_timeout_ms = config.getInt(session_timeout_key);
    }

    use_xid_64 = config.getBool(std::string{config_name} + ".use_xid_64", false);

    Poco::Util::AbstractConfiguration::Keys keys;
    std::string raft_configuration_key = std::string{config_name} + ".raft_configuration";
    config.keys(raft_configuration_key, keys);
    for (const auto & key : keys)
    {
        if (startsWith(key, "server"))
        {
            hosts.push_back(
                (secure ? "secure://" : "") + config.getString(raft_configuration_key + "." + key + ".hostname") + ":" + tcp_port);
            availability_zones.push_back(config.getString(raft_configuration_key + "." + key + ".availability_zone", ""));
        }
    }

    static constexpr std::array load_balancing_keys
    {
        ".zookeeper_load_balancing",
        ".keeper_load_balancing"
    };

    for (const auto * load_balancing_key : load_balancing_keys)
    {
        if (auto load_balancing_config = std::string{config_name} + load_balancing_key;
            config.has(load_balancing_config))
        {
            String load_balancing_str = config.getString(load_balancing_config);
            /// Use magic_enum to avoid dependency from dbms (`SettingFieldLoadBalancingTraits::fromString(...)`)
            auto load_balancing = magic_enum::enum_cast<DB::LoadBalancing>(Poco::toUpper(load_balancing_str));
            if (!load_balancing)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown load balancing: {}", load_balancing_str);
            get_priority_load_balancing = DB::GetPriorityForLoadBalancing(*load_balancing, thread_local_rng() % hosts.size());
            break;
        }
    }

    availability_zone_autodetect = config.getBool(std::string{config_name} + ".availability_zone_autodetect", false);
    prefer_local_availability_zone = config.getBool(std::string{config_name} + ".prefer_local_availability_zone", false);
    if (prefer_local_availability_zone)
        client_availability_zone = DB::PlacementInfo::PlacementInfo::instance().getAvailabilityZone();
}

void ZooKeeperArgs::initFromKeeperSection(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    zookeeper_name = config_name;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    std::optional<DB::LoadBalancing> load_balancing;

    for (const auto & key : keys)
    {
        if (key.starts_with("node"))
        {
            hosts.push_back(
                (config.getBool(config_name + "." + key + ".secure", false) ? "secure://" : "")
                + config.getString(config_name + "." + key + ".host") + ":" + config.getString(config_name + "." + key + ".port", "2181"));
            availability_zones.push_back(config.getString(config_name + "." + key + ".availability_zone", ""));
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
        else if (key == "sessions_path")
        {
            sessions_path = config.getString(config_name + "." + key);
        }
        else if (key == "prefer_local_availability_zone")
        {
            prefer_local_availability_zone = config.getBool(config_name + "." + key);
        }
        else if (key == "implementation")
        {
            implementation = config.getString(config_name + "." + key);
        }
        else if (key == "zookeeper_load_balancing" || key == "keeper_load_balancing")
        {
            String load_balancing_str = config.getString(config_name + "." + key);
            /// Use magic_enum to avoid dependency from dbms (`SettingFieldLoadBalancingTraits::fromString(...)`)
            load_balancing = magic_enum::enum_cast<DB::LoadBalancing>(Poco::toUpper(load_balancing_str));
            if (!load_balancing)
                throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "Unknown load balancing: {}", load_balancing_str);
        }
        else if (key == "fallback_session_lifetime")
        {
            fallback_session_lifetime = SessionLifetimeConfiguration
            {
                .min_sec = config.getUInt(config_name + "." + key + ".min"),
                .max_sec = config.getUInt(config_name + "." + key + ".max"),
            };
        }
        else if (key == "use_compression")
        {
            use_compression = config.getBool(config_name + "." + key);
        }
        else if (key == "use_xid_64")
        {
            use_xid_64 = config.getBool(config_name + "." + key);
        }
        else if (key == "availability_zone_autodetect")
        {
            availability_zone_autodetect = config.getBool(config_name + "." + key);
        }
        else
            throw KeeperException(Coordination::Error::ZBADARGUMENTS, "Unknown key {} in config file", key);
    }

    if (load_balancing)
        get_priority_load_balancing = DB::GetPriorityForLoadBalancing(*load_balancing, thread_local_rng() % hosts.size());

    if (prefer_local_availability_zone)
        client_availability_zone = DB::PlacementInfo::PlacementInfo::instance().getAvailabilityZone();
}

}
