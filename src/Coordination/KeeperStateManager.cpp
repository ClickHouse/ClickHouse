#include <Coordination/KeeperStateManager.h>
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

KeeperStateManager::KeeperStateManager(int server_id_, const std::string & host, int port, const std::string & logs_path)
    : my_server_id(server_id_)
    , my_port(port)
    , secure(false)
    , log_store(nuraft::cs_new<KeeperLogStore>(logs_path, 5000, false))
    , cluster_config(nuraft::cs_new<nuraft::cluster_config>())
{
    auto peer_config = nuraft::cs_new<nuraft::srv_config>(my_server_id, host + ":" + std::to_string(port));
    cluster_config->get_servers().push_back(peer_config);
}

KeeperStateManager::KeeperStateManager(
    int my_server_id_,
    const std::string & config_prefix,
    const Poco::Util::AbstractConfiguration & config,
    const CoordinationSettingsPtr & coordination_settings,
    bool standalone_keeper)
    : my_server_id(my_server_id_)
    , secure(config.getBool(config_prefix + ".raft_configuration.secure", false))
    , log_store(nuraft::cs_new<KeeperLogStore>(
                    getLogsPathFromConfig(config_prefix, config, standalone_keeper),
                    coordination_settings->rotate_log_storage_interval, coordination_settings->force_sync))
    , cluster_config(nuraft::cs_new<nuraft::cluster_config>())
{

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix + ".raft_configuration", keys);
    total_servers = keys.size();

    for (const auto & server_key : keys)
    {
        if (!startsWith(server_key, "server"))
            continue;

        std::string full_prefix = config_prefix + ".raft_configuration." + server_key;
        int server_id = config.getInt(full_prefix + ".id");
        std::string hostname = config.getString(full_prefix + ".hostname");
        int port = config.getInt(full_prefix + ".port");
        bool can_become_leader = config.getBool(full_prefix + ".can_become_leader", true);
        int32_t priority = config.getInt(full_prefix + ".priority", 1);
        bool start_as_follower = config.getBool(full_prefix + ".start_as_follower", false);

        if (start_as_follower)
            start_as_follower_servers.insert(server_id);

        auto endpoint = hostname + ":" + std::to_string(port);
        auto peer_config = nuraft::cs_new<nuraft::srv_config>(server_id, 0, endpoint, "", !can_become_leader, priority);
        if (server_id == my_server_id)
        {
            my_server_config = peer_config;
            my_port = port;
        }

        cluster_config->get_servers().push_back(peer_config);
    }

    if (!my_server_config)
        throw Exception(ErrorCodes::RAFT_ERROR, "Our server id {} not found in raft_configuration section", my_server_id);

    if (start_as_follower_servers.size() == cluster_config->get_servers().size())
        throw Exception(ErrorCodes::RAFT_ERROR, "At least one of servers should be able to start as leader (without <start_as_follower>)");
}

void KeeperStateManager::loadLogStore(uint64_t last_commited_index, uint64_t logs_to_keep)
{
    log_store->init(last_commited_index, logs_to_keep);
}

void KeeperStateManager::flushLogStore()
{
    log_store->flush();
}

void KeeperStateManager::save_config(const nuraft::cluster_config & config)
{
    // Just keep in memory in this example.
    // Need to write to disk here, if want to make it durable.
    nuraft::ptr<nuraft::buffer> buf = config.serialize();
    cluster_config = nuraft::cluster_config::deserialize(*buf);
}

void KeeperStateManager::save_state(const nuraft::srv_state & state)
{
     // Just keep in memory in this example.
     // Need to write to disk here, if want to make it durable.
     nuraft::ptr<nuraft::buffer> buf = state.serialize();
     server_state = nuraft::srv_state::deserialize(*buf);
 }

}
