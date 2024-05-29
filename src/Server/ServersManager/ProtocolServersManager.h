#pragma once

#include <Server/ServersManager/IServersManager.h>
#include <Server/TCPProtocolStackFactory.h>
#include <Poco/Net/HTTPServerParams.h>

namespace DB
{

class ProtocolServersManager : public IServersManager
{
public:
    using IServersManager::IServersManager;

    void createServers(
        const Poco::Util::AbstractConfiguration & config,
        IServer & server,
        std::mutex & servers_lock,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        bool start_servers,
        const ServerType & server_type) override;

    using IServersManager::stopServers;
    size_t stopServers(const ServerSettings & server_settings, std::mutex & servers_lock) override;

private:
    std::unique_ptr<TCPProtocolStackFactory> buildProtocolStackFromConfig(
        const Poco::Util::AbstractConfiguration & config,
        IServer & server,
        const std::string & protocol,
        Poco::Net::HTTPServerParams::Ptr http_params,
        AsynchronousMetrics & async_metrics,
        bool & is_secure) const;
};

}
