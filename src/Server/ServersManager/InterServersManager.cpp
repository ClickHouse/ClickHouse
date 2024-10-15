#include <Server/ServersManager/InterServersManager.h>

#include <Interpreters/Context.h>
#include <Server/CloudPlacementInfo.h>
#include <Server/HTTP/HTTPServer.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/KeeperReadinessHandler.h>
#include <Server/waitServersToFinish.h>
#include <Poco/Net/HTTPServerParams.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/ProfileEvents.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>

#if USE_SSL
#    include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_NURAFT
#    include <Coordination/FourLetterCommand.h>
#    include <Server/KeeperTCPHandlerFactory.h>
#endif

namespace ProfileEvents
{
extern const Event InterfaceInterserverSendBytes;
extern const Event InterfaceInterserverReceiveBytes;
}

namespace DB
{

namespace Setting
{
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsSeconds http_send_timeout;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsSeconds send_timeout;
}

namespace ErrorCodes
{
extern const int SUPPORT_IS_DISABLED;
}

void InterServersManager::createServers(
    const Poco::Util::AbstractConfiguration & config,
    IServer & server,
    std::mutex & servers_lock,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    bool start_servers,
    const ServerType & server_type)
{
    if (config.has("keeper_server.server_id"))
    {
#if USE_NURAFT
        //// If we don't have configured connection probably someone trying to use clickhouse-server instead
        //// of clickhouse-keeper, so start synchronously.
        bool can_initialize_keeper_async = false;

        if (zkutil::hasZooKeeperConfig(config)) /// We have configured connection to some zookeeper cluster
        {
            /// If we cannot connect to some other node from our cluster then we have to wait our Keeper start
            /// synchronously.
            can_initialize_keeper_async = global_context->tryCheckClientConnectionToMyKeeperCluster();
        }
        /// Initialize keeper RAFT.
        global_context->initializeKeeperDispatcher(can_initialize_keeper_async);
        FourLetterCommandFactory::registerCommands(*global_context->getKeeperDispatcher());

        auto config_getter = [this]() -> const Poco::Util::AbstractConfiguration & { return global_context->getConfigRef(); };

        for (const auto & listen_host : getListenHosts(config))
        {
            /// TCP Keeper
            constexpr auto port_name = "keeper_server.tcp_port";
            createServer(
                config,
                listen_host,
                port_name,
                /* start_server = */ false,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(
                        Poco::Timespan(config.getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0));
                    socket.setSendTimeout(
                        Poco::Timespan(config.getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0));
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "Keeper (tcp): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new KeeperTCPHandlerFactory(
                                config_getter,
                                global_context->getKeeperDispatcher(),
                                global_context->getSettingsRef()[Setting::receive_timeout].totalSeconds(),
                                global_context->getSettingsRef()[Setting::send_timeout].totalSeconds(),
                                false),
                            server_pool,
                            socket));
                });

            constexpr auto secure_port_name = "keeper_server.tcp_port_secure";
            createServer(
                config,
                listen_host,
                secure_port_name,
                /* start_server = */ false,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
#    if USE_SSL
                    Poco::Net::SecureServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(
                        Poco::Timespan(config.getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0));
                    socket.setSendTimeout(
                        Poco::Timespan(config.getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0));
                    return ProtocolServerAdapter(
                        listen_host,
                        secure_port_name,
                        "Keeper with secure protocol (tcp_secure): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new KeeperTCPHandlerFactory(
                                config_getter,
                                global_context->getKeeperDispatcher(),
                                global_context->getSettingsRef()[Setting::receive_timeout].totalSeconds(),
                                global_context->getSettingsRef()[Setting::send_timeout].totalSeconds(),
                                true),
                            server_pool,
                            socket));
#    else
                    UNUSED(port);
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#    endif
                });

            /// HTTP control endpoints
            createServer(
                config,
                listen_host,
                /* port_name = */ "keeper_server.http_control.port",
                /* start_server = */ false,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    auto http_context = std::make_shared<HTTPContext>(global_context);
                    Poco::Timespan keep_alive_timeout(config.getUInt("keep_alive_timeout", 10), 0);
                    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
                    http_params->setTimeout(http_context->getReceiveTimeout());
                    http_params->setKeepAliveTimeout(keep_alive_timeout);

                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(http_context->getReceiveTimeout());
                    socket.setSendTimeout(http_context->getSendTimeout());
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "HTTP Control: http://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            std::move(http_context),
                            createKeeperHTTPControlMainHandlerFactory(
                                config_getter(), global_context->getKeeperDispatcher(), "KeeperHTTPControlHandler-factory"),
                            server_pool,
                            socket,
                            http_params));
                });
        }
#else
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse server built without NuRaft library. Cannot use internal coordination.");
#endif
    }

    if (config.has(DB::PlacementInfo::PLACEMENT_CONFIG_PREFIX))
    {
        PlacementInfo::PlacementInfo::instance().initialize(config);
    }

    {
        std::lock_guard lock(servers_lock);
        /// We should start interserver communications before (and more important shutdown after) tables.
        /// Because server can wait for a long-running queries (for example in tcp_handler) after interserver handler was already shut down.
        /// In this case we will have replicated tables which are unable to send any parts to other replicas, but still can
        /// communicate with zookeeper, execute merges, etc.
        createInterserverServers(config, server, server_pool, async_metrics, start_servers, server_type);
        startServers();
    }
}

size_t InterServersManager::stopServers(const ServerSettings & server_settings, std::mutex & servers_lock)
{
    if (servers.empty())
    {
        return 0;
    }

    LOG_DEBUG(logger, "Waiting for current connections to servers for tables to finish.");

    size_t current_connections = 0;
    {
        std::lock_guard lock(servers_lock);
        for (auto & server : servers)
        {
            server.stop();
            current_connections += server.currentConnections();
        }
    }

    if (current_connections)
        LOG_INFO(logger, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
    else
        LOG_INFO(logger, "Closed all listening sockets.");

    if (current_connections > 0)
        current_connections = waitServersToFinish(servers, servers_lock, server_settings.shutdown_wait_unfinished);

    if (current_connections)
        LOG_INFO(
            logger,
            "Closed connections to servers for tables. But {} remain. Probably some tables of other users cannot finish their connections "
            "after context shutdown.",
            current_connections);
    else
        LOG_INFO(logger, "Closed connections to servers for tables.");
    return current_connections;
}

void InterServersManager::updateServers(
    const Poco::Util::AbstractConfiguration & config,
    IServer & iserver,
    std::mutex & /*servers_lock*/,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    ConfigurationPtr latest_config)
{
    stopServersForUpdate(config, latest_config);
    createInterserverServers(config, iserver, server_pool, async_metrics, true, ServerType(ServerType::Type::QUERIES_ALL));
}

Strings InterServersManager::getInterserverListenHosts(const Poco::Util::AbstractConfiguration & config) const
{
    auto interserver_listen_hosts = DB::getMultipleValuesFromConfig(config, "", "interserver_listen_host");
    if (!interserver_listen_hosts.empty())
        return interserver_listen_hosts;

    /// Use more general restriction in case of emptiness
    return getListenHosts(config);
}

void InterServersManager::createInterserverServers(
    const Poco::Util::AbstractConfiguration & config,
    IServer & server,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    bool start_servers,
    const ServerType & server_type)
{
    const Settings & settings = global_context->getSettingsRef();

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings[Setting::http_receive_timeout]);
    http_params->setKeepAliveTimeout(global_context->getServerSettings().keep_alive_timeout);

    /// Now iterate over interserver_listen_hosts
    for (const auto & interserver_listen_host : getInterserverListenHosts(config))
    {
        if (server_type.shouldStart(ServerType::Type::INTERSERVER_HTTP))
        {
            /// Interserver IO HTTP
            constexpr auto port_name = "interserver_http_port";
            createServer(
                config,
                interserver_listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, interserver_listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                    socket.setSendTimeout(settings[Setting::http_send_timeout]);
                    return ProtocolServerAdapter(
                        interserver_listen_host,
                        port_name,
                        "replica communication (interserver): http://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            std::make_shared<HTTPContext>(global_context),
                            createHandlerFactory(server, config, async_metrics, "InterserverIOHTTPHandler-factory"),
                            server_pool,
                            socket,
                            http_params,
                            ProfileEvents::InterfaceInterserverReceiveBytes,
                            ProfileEvents::InterfaceInterserverSendBytes));
                });
        }

        if (server_type.shouldStart(ServerType::Type::INTERSERVER_HTTPS))
        {
            constexpr auto port_name = "interserver_https_port";
            createServer(
                config,
                interserver_listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
#if USE_SSL
                    Poco::Net::SecureServerSocket socket;
                    auto address = socketBindListen(config, socket, interserver_listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                    socket.setSendTimeout(settings[Setting::http_send_timeout]);
                    return ProtocolServerAdapter(
                        interserver_listen_host,
                        port_name,
                        "secure replica communication (interserver): https://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            std::make_shared<HTTPContext>(global_context),
                            createHandlerFactory(server, config, async_metrics, "InterserverIOHTTPSHandler-factory"),
                            server_pool,
                            socket,
                            http_params,
                            ProfileEvents::InterfaceInterserverReceiveBytes,
                            ProfileEvents::InterfaceInterserverSendBytes));
#else
                    UNUSED(port);
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
                });
        }
    }
}

}
