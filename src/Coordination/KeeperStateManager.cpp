#include <Coordination/KeeperStateManager.h>

#include <filesystem>
#include <Coordination/Defines.h>
#include <Common/DNSResolver.h>
#include <Common/Exception.h>
#include <Common/isLocalAddress.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

namespace
{

bool isLocalhost(const std::string & hostname)
{
    try
    {
        return isLocalAddress(DNSResolver::instance().resolveHost(hostname));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    return false;
}

std::unordered_map<UInt64, std::string> getClientPorts(const Poco::Util::AbstractConfiguration & config)
{
    using namespace std::string_literals;
    static const std::array config_port_names = {
        "keeper_server.tcp_port"s,
        "keeper_server.tcp_port_secure"s,
        "interserver_http_port"s,
        "interserver_https_port"s,
        "tcp_port"s,
        "tcp_with_proxy_port"s,
        "tcp_port_secure"s,
        "mysql_port"s,
        "postgresql_port"s,
        "grpc_port"s,
        "prometheus.port"s,
    };

    std::unordered_map<UInt64, std::string> ports;
    for (const auto & config_port_name : config_port_names)
    {
        if (config.has(config_port_name))
            ports[config.getUInt64(config_port_name)] = config_port_name;
    }
    return ports;
}

}

/// this function quite long because contains a lot of sanity checks in config:
/// 1. No duplicate endpoints
/// 2. No "localhost" or "127.0.0.1" or another local addresses mixed with normal addresses
/// 3. Raft internal port is not equal to any other port for client
/// 4. No duplicate IDs
/// 5. Our ID present in hostnames list
KeeperStateManager::KeeperConfigurationWrapper
KeeperStateManager::parseServersConfiguration(const Poco::Util::AbstractConfiguration & config, bool allow_without_us) const
{
    const bool hostname_checks_enabled = config.getBool(config_prefix + ".hostname_checks_enabled", true);

    KeeperConfigurationWrapper result;
    result.cluster_config = std::make_shared<nuraft::cluster_config>();
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix + ".raft_configuration", keys);

    auto client_ports = getClientPorts(config);

    /// Sometimes (especially in cloud envs) users can provide incorrect
    /// configuration with duplicated raft ids or endpoints. We check them
    /// on config parsing stage and never commit to quorum.
    std::unordered_map<std::string, int> check_duplicated_hostnames;

    size_t total_servers = 0;
    bool localhost_present = false;
    std::string non_local_hostname;
    size_t local_address_counter = 0;
    for (const auto & server_key : keys)
    {
        if (!startsWith(server_key, "server"))
            continue;

        std::string full_prefix = config_prefix + ".raft_configuration." + server_key;
        int new_server_id = config.getInt(full_prefix + ".id");
        std::string hostname = config.getString(full_prefix + ".hostname");
        int port = config.getInt(full_prefix + ".port");
        bool can_become_leader = config.getBool(full_prefix + ".can_become_leader", true);
        int32_t priority = config.getInt(full_prefix + ".priority", 1);
        bool start_as_follower = config.getBool(full_prefix + ".start_as_follower", false);

        if (client_ports.contains(port))
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Raft configuration contains hostname '{}' with port '{}' which is equal to '{}' in server configuration",
                hostname,
                port,
                client_ports[port]);
        }

        if (hostname_checks_enabled)
        {
            if (hostname == "localhost")
            {
                localhost_present = true;
                local_address_counter++;
            }
            else if (isLocalhost(hostname))
            {
                local_address_counter++;
            }
            else
            {
                non_local_hostname = hostname;
            }
        }

        if (start_as_follower)
            result.servers_start_as_followers.insert(new_server_id);

        auto endpoint = hostname + ":" + std::to_string(port);
        if (check_duplicated_hostnames.contains(endpoint))
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Raft config contains duplicate endpoints: "
                "endpoint {} has been already added with id {}, but going to add it one more time with id {}",
                endpoint,
                check_duplicated_hostnames[endpoint],
                new_server_id);
        }
        else
        {
            /// Fullscan to check duplicated ids
            for (const auto & [id_endpoint, id] : check_duplicated_hostnames)
            {
                if (new_server_id == id)
                    throw Exception(
                        ErrorCodes::RAFT_ERROR,
                        "Raft config contains duplicate ids: id {} has been already added with endpoint {}, "
                        "but going to add it one more time with endpoint {}",
                        id,
                        id_endpoint,
                        endpoint);
            }
            check_duplicated_hostnames.emplace(endpoint, new_server_id);
        }

        auto peer_config = nuraft::cs_new<nuraft::srv_config>(new_server_id, 0, endpoint, "", !can_become_leader, priority);
        if (my_server_id == new_server_id)
        {
            result.config = peer_config;
            result.port = port;
        }

        result.cluster_config->get_servers().push_back(peer_config);
        total_servers++;
    }

    if (!result.config && !allow_without_us)
        throw Exception(ErrorCodes::RAFT_ERROR, "Our server id {} not found in raft_configuration section", my_server_id);

    if (result.servers_start_as_followers.size() == total_servers)
        throw Exception(ErrorCodes::RAFT_ERROR, "At least one of servers should be able to start as leader (without <start_as_follower>)");

    if (hostname_checks_enabled)
    {
        if (localhost_present && !non_local_hostname.empty())
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Mixing 'localhost' and non-local hostnames ('{}') in raft_configuration is not allowed. "
                "Different hosts can resolve 'localhost' to themselves so it's not allowed.",
                non_local_hostname);
        }

        if (!non_local_hostname.empty() && local_address_counter > 1)
        {
            throw Exception(
                ErrorCodes::RAFT_ERROR,
                "Local address specified more than once ({} times) and non-local hostnames also exists ('{}') in raft_configuration. "
                "Such configuration is not allowed because single host can vote multiple times.",
                local_address_counter,
                non_local_hostname);
        }
    }

    return result;
}

KeeperStateManager::KeeperStateManager(int server_id_, const std::string & host, int port, const std::string & logs_path)
    : my_server_id(server_id_), secure(false), log_store(nuraft::cs_new<KeeperLogStore>(logs_path, 5000, false, false))
{
    auto peer_config = nuraft::cs_new<nuraft::srv_config>(my_server_id, host + ":" + std::to_string(port));
    configuration_wrapper.cluster_config = nuraft::cs_new<nuraft::cluster_config>();
    configuration_wrapper.port = port;
    configuration_wrapper.config = peer_config;
    configuration_wrapper.cluster_config->get_servers().push_back(peer_config);
}

KeeperStateManager::KeeperStateManager(
    int my_server_id_,
    const std::string & config_prefix_,
    const std::string & log_storage_path,
    const Poco::Util::AbstractConfiguration & config,
    const CoordinationSettingsPtr & coordination_settings)
    : my_server_id(my_server_id_)
    , secure(config.getBool(config_prefix_ + ".raft_configuration.secure", false))
    , config_prefix(config_prefix_)
    , configuration_wrapper(parseServersConfiguration(config, false))
    , log_store(nuraft::cs_new<KeeperLogStore>(
          log_storage_path,
          coordination_settings->rotate_log_storage_interval,
          coordination_settings->force_sync,
          coordination_settings->compress_logs))
{
}

void KeeperStateManager::loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep)
{
    log_store->init(last_commited_index, logs_to_keep);
}

void KeeperStateManager::system_exit(const int /* exit_code */)
{
    /// NuRaft itself calls exit() which will call atexit handlers
    /// and this may lead to an issues in multi-threaded program.
    ///
    /// Override this with abort().
    abort();
}

ClusterConfigPtr KeeperStateManager::getLatestConfigFromLogStore() const
{
    auto entry_with_change = log_store->getLatestConfigChange();
    if (entry_with_change)
        return ClusterConfig::deserialize(entry_with_change->get_buf());
    return nullptr;
}

void KeeperStateManager::flushAndShutDownLogStore()
{
    log_store->flushChangelogAndShutdown();
}

void KeeperStateManager::save_config(const nuraft::cluster_config & config)
{
    std::lock_guard lock(configuration_wrapper_mutex);
    nuraft::ptr<nuraft::buffer> buf = config.serialize();
    configuration_wrapper.cluster_config = nuraft::cluster_config::deserialize(*buf);
}

void KeeperStateManager::save_state(const nuraft::srv_state & state)
{
    nuraft::ptr<nuraft::buffer> buf = state.serialize();
    server_state = nuraft::srv_state::deserialize(*buf);
}

ConfigUpdateActions KeeperStateManager::getConfigurationDiff(const Poco::Util::AbstractConfiguration & config) const
{
    auto new_configuration_wrapper = parseServersConfiguration(config, true);

    std::unordered_map<int, KeeperServerConfigPtr> new_ids, old_ids;
    for (const auto & new_server : new_configuration_wrapper.cluster_config->get_servers())
        new_ids[new_server->get_id()] = new_server;

    {
        std::lock_guard lock(configuration_wrapper_mutex);
        for (const auto & old_server : configuration_wrapper.cluster_config->get_servers())
            old_ids[old_server->get_id()] = old_server;
    }

    ConfigUpdateActions result;

    /// First of all add new servers
    for (const auto & [new_id, server_config] : new_ids)
    {
        auto old_server_it = old_ids.find(new_id);
        if (old_server_it == old_ids.end())
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::AddServer, server_config});
        else
        {
            const auto & old_endpoint = old_server_it->second->get_endpoint();
            if (old_endpoint != server_config->get_endpoint())
            {
                LOG_WARNING(
                    &Poco::Logger::get("RaftConfiguration"),
                    "Config will be ignored because a server with ID {} is already present in the cluster on a different endpoint ({}). "
                    "The endpoint of the current servers should not be changed. For servers on a new endpoint, please use a new ID.",
                    new_id,
                    old_endpoint);
                return {};
            }
        }
    }

    /// After that remove old ones
    for (auto [old_id, server_config] : old_ids)
    {
        if (!new_ids.contains(old_id))
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::RemoveServer, server_config});
    }

    {
        std::lock_guard lock(configuration_wrapper_mutex);
        /// And update priority if required
        for (const auto & old_server : configuration_wrapper.cluster_config->get_servers())
        {
            for (const auto & new_server : new_configuration_wrapper.cluster_config->get_servers())
            {
                if (old_server->get_id() == new_server->get_id())
                {
                    if (old_server->get_priority() != new_server->get_priority())
                    {
                        result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::UpdatePriority, new_server});
                    }
                    break;
                }
            }
        }
    }

    return result;
}

}
