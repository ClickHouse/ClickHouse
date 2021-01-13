#include <Coordination/InMemoryStateManager.h>

namespace DB
{

InMemoryStateManager::InMemoryStateManager(int my_server_id_, const std::string & endpoint_)
    : my_server_id(my_server_id_)
    , endpoint(endpoint_)
    , log_store(nuraft::cs_new<InMemoryLogStore>())
    , server_config(nuraft::cs_new<nuraft::srv_config>(my_server_id, endpoint))
    , cluster_config(nuraft::cs_new<nuraft::cluster_config>())
{
    cluster_config->get_servers().push_back(server_config);
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
