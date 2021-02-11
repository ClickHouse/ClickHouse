#include <Coordination/InMemoryStateManager.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RAFT_ERROR;
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
