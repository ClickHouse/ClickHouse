#include <Coordination/KeeperStateManager.h>
#include <Coordination/Defines.h>
#include <Common/Exception.h>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

namespace
{
    std::string getLogsPathFromConfig(
        const std::string & config_prefix, const Poco::Util::AbstractConfiguration & config, bool standalone_keeper)
{
    /// the most specialized path
    if (config.has(config_prefix + ".log_storage_path"))
        return config.getString(config_prefix + ".log_storage_path");

    if (config.has(config_prefix + ".storage_path"))
        return std::filesystem::path{config.getString(config_prefix + ".storage_path")} / "logs";

    if (standalone_keeper)
        return std::filesystem::path{config.getString("path", KEEPER_DEFAULT_PATH)} / "logs";
    else
        return std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination/logs";
}

}


KeeperConfigurationWrapper KeeperStateManager::parseServersConfiguration(const Poco::Util::AbstractConfiguration & config, bool allow_without_us) const
{
    KeeperConfigurationWrapper result;
    result.cluster_config = std::make_shared<nuraft::cluster_config>();
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix + ".raft_configuration", keys);

    size_t total_servers = 0;
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

        if (start_as_follower)
            result.servers_start_as_followers.insert(new_server_id);

        auto endpoint = hostname + ":" + std::to_string(port);
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

    return result;
}

KeeperStateManager::KeeperStateManager(int server_id_, const std::string & host, int port, const std::string & logs_path)
    : my_server_id(server_id_)
    , secure(false)
    , log_store(nuraft::cs_new<KeeperLogStore>(logs_path, 5000, false, false))
{
    auto peer_config = nuraft::cs_new<nuraft::srv_config>(my_server_id, host + ":" + std::to_string(port));
    configuration_wrapper.cluster_config = nuraft::cs_new<nuraft::cluster_config>();
    configuration_wrapper.port = port;
    configuration_wrapper.config = peer_config;
    configuration_wrapper.cluster_config->get_servers().push_back(peer_config);
}

KeeperStateManager::KeeperStateManager(
    int server_id_,
    const std::string & config_prefix_,
    const Poco::Util::AbstractConfiguration & config,
    const CoordinationSettingsPtr & coordination_settings,
    bool standalone_keeper)
    : my_server_id(server_id_)
    , secure(config.getBool(config_prefix_ + ".raft_configuration.secure", false))
    , config_prefix(config_prefix_)
    , configuration_wrapper(parseServersConfiguration(config, false))
    , log_store(nuraft::cs_new<KeeperLogStore>(
                    getLogsPathFromConfig(config_prefix_, config, standalone_keeper),
                    coordination_settings->rotate_log_storage_interval, coordination_settings->force_sync, coordination_settings->compress_logs))
{
}

void KeeperStateManager::loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep)
{
    log_store->init(last_commited_index, logs_to_keep);
}

ClusterConfigPtr KeeperStateManager::getLatestConfigFromLogStore() const
{
    auto entry_with_change = log_store->getLatestConfigChange();
    if (entry_with_change)
        return ClusterConfig::deserialize(entry_with_change->get_buf());
    return nullptr;
}

void KeeperStateManager::flushLogStore()
{
    log_store->flush();
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
    for (auto [new_id, server_config] : new_ids)
    {
        if (!old_ids.count(new_id))
            result.emplace_back(ConfigUpdateAction{ConfigUpdateActionType::AddServer, server_config});
    }

    /// After that remove old ones
    for (auto [old_id, server_config] : old_ids)
    {
        if (!new_ids.count(old_id))
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
