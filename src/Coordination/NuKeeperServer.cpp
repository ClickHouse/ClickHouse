#include <Coordination/NuKeeperServer.h>

namespace DB
{

void NuKeeperServer::addServer(int server_id_, const std::string & server_uri)
{
    if (raft_instance->is_leader())
    {
        nuraft::srv_config first_config(server_id, server_uri);
    }

}
