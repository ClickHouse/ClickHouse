#pragma once

#include <mutex>
#include <Core/ServerSettings.h>
#include <Interpreters/Context_fwd.h>
#include <Server/IServer.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/ServerType.h>
#include <Poco/Logger.h>
#include <Poco/Net/ServerSocket.h>
#include <Poco/ThreadPool.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Common/AsynchronousMetrics.h>
#include <Common/Config/ConfigProcessor.h>

namespace DB
{

class IServersManager
{
public:
    IServersManager(ContextMutablePtr global_context_, Poco::Logger * logger_);
    virtual ~IServersManager() = default;

    bool empty() const;
    std::vector<ProtocolServerMetrics> getMetrics() const;

    virtual void createServers(
        const Poco::Util::AbstractConfiguration & config,
        IServer & server,
        std::mutex & servers_lock,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        bool start_servers,
        const ServerType & server_type)
        = 0;

    void startServers();

    void stopServers(const ServerType & server_type);
    virtual size_t stopServers(const ServerSettings & server_settings, std::mutex & servers_lock) = 0;

    virtual void updateServers(
        const Poco::Util::AbstractConfiguration & config,
        IServer & server,
        std::mutex & servers_lock,
        Poco::ThreadPool & server_pool,
        AsynchronousMetrics & async_metrics,
        ConfigurationPtr latest_config);

protected:
    ContextMutablePtr global_context;
    Poco::Logger * logger;

    std::vector<ProtocolServerAdapter> servers;

    Poco::Net::SocketAddress socketBindListen(
        const Poco::Util::AbstractConfiguration & config, Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port) const;

    using CreateServerFunc = std::function<ProtocolServerAdapter(UInt16)>;
    void createServer(
        const Poco::Util::AbstractConfiguration & config,
        const std::string & listen_host,
        const char * port_name,
        bool start_server,
        CreateServerFunc && func);

    void stopServersForUpdate(const Poco::Util::AbstractConfiguration & config, ConfigurationPtr latest_config);

    Strings getListenHosts(const Poco::Util::AbstractConfiguration & config) const;
    bool getListenTry(const Poco::Util::AbstractConfiguration & config) const;
};

}
