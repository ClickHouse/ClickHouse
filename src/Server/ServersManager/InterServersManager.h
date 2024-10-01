#pragma once

#include <Server/ServersManager/IServersManager.h>

namespace DB
{

class InterServersManager : public IServersManager
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

    size_t stopServers(const ServerSettings & server_settings, std::mutex & servers_lock) override;

    void updateServers(
        const Poco::Util::AbstractConfiguration & config,
        IServer & iserver,
        std::mutex & servers_lock,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        ConfigurationPtr latest_config) override;

private:
    Strings getInterserverListenHosts(const Poco::Util::AbstractConfiguration & config) const;

    void createInterserverServers(
        const Poco::Util::AbstractConfiguration & config,
        IServer & server,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        bool start_servers,
        const ServerType & server_type);
};

}
