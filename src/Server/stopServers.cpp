#include <Server/stopServers.h>

#include <Server/ProtocolServerAdapter.h>
#include <Server/ServerType.h>

#include <functional>

namespace DB
{

void stopServers(std::vector<ProtocolServerAdapter> & servers, const ServerType & server_type, LoggerRawPtr log)
{
    /// Remove servers once all their connections are closed
    auto check_server = [&log](const char prefix[], auto & server)
    {
        if (!server.isStopping())
            return false;
        size_t current_connections = server.currentConnections();
        LOG_DEBUG(log, "Server {}{}: {} ({} connections)",
            server.getDescription(),
            prefix,
            !current_connections ? "finished" : "waiting",
            current_connections);
        return !current_connections;
    };

    std::erase_if(servers, std::bind_front(check_server, " (from one of previous remove)"));

    for (auto & server : servers)
    {
        if (!server.isStopping())
        {
            const std::string server_port_name = server.getPortName();
            if (server_type.shouldStop(server_port_name))
                server.stop();
        }
    }

    std::erase_if(servers, std::bind_front(check_server, ""));
}

}
