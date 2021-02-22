#include <Coordination/InMemoryStateManager.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
}

InMemoryStateManager::InMemoryStateManager(int server_id_, const std::string & host, int port)
    : my_server_id(server_id_)
    , my_port(port)
    , log_store(nuraft::cs_new<InMemoryLogStore>())
    , cluster_config(nuraft::cs_new<nuraft::cluster_config>())
{
    auto peer_config = nuraft::cs_new<nuraft::srv_config>(my_server_id, host + ":" + std::to_string(port));
    cluster_config->get_servers().push_back(peer_config);
}

InMemoryStateManager::InMemoryStateManager(
    int my_server_id_,
    const std::string & config_prefix,
    const Poco::Util::AbstractConfiguration & config)
    : my_server_id(my_server_id_)
    , log_store(nuraft::cs_new<InMemoryLogStore>())
    , cluster_config(nuraft::cs_new<nuraft::cluster_config>())
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_prefix, keys);

    for (const auto & server_key : keys)
    {
        std::string full_prefix = config_prefix + "." + server_key;
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
        throw Exception(ErrorCodes::RAFT_ERROR, "Our server id {} not found in raft_configuration section");

    if (start_as_follower_servers.size() == cluster_config->get_servers().size())
        throw Exception(ErrorCodes::RAFT_ERROR, "At least one of servers should be able to start as leader (without <start_as_follower>)");
}

void InMemoryStateManager::save_config(const nuraft::cluster_config & config)
{
    // Just keep in memory in this example.
    // Need to write to disk here, if want to make it durable.
    nuraft::ptr<nuraft::buffer> buf = config.serialize();
    cluster_config = nuraft::cluster_config::deserialize(*buf);
}

void InMemoryStateManager::save_state(const nuraft::srv_state & state)
{
     // Just keep in memory in this example.
     // Need to write to disk here, if want to make it durable.
     nuraft::ptr<nuraft::buffer> buf = state.serialize();
     server_state = nuraft::srv_state::deserialize(*buf);
 }

}
