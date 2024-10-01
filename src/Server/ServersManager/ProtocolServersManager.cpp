#include <Server/ServersManager/ProtocolServersManager.h>

#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Server/HTTP/HTTPServer.h>
#include <Server/HTTP/HTTPServerConnectionFactory.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/MySQLHandlerFactory.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <Server/ProxyV1HandlerFactory.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/TLSHandlerFactory.h>
#include <Server/waitServersToFinish.h>
#include <Common/ProfileEvents.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/makeSocketAddress.h>

#if USE_SSL
#    include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_GRPC
#    include <Server/GRPCServer.h>
#endif

namespace ProfileEvents
{
extern const Event InterfaceNativeSendBytes;
extern const Event InterfaceNativeReceiveBytes;
extern const Event InterfaceHTTPSendBytes;
extern const Event InterfaceHTTPReceiveBytes;
extern const Event InterfacePrometheusSendBytes;
extern const Event InterfacePrometheusReceiveBytes;
extern const Event InterfaceMySQLSendBytes;
extern const Event InterfaceMySQLReceiveBytes;
extern const Event InterfacePostgreSQLSendBytes;
extern const Event InterfacePostgreSQLReceiveBytes;
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
extern const int INVALID_CONFIG_PARAMETER;
}

void ProtocolServersManager::createServers(
    const Poco::Util::AbstractConfiguration & config,
    IServer & server,
    std::mutex & /*servers_lock*/,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    bool start_servers,
    const ServerType & server_type)
{
    auto listen_hosts = getListenHosts(config);
    const Settings & settings = global_context->getSettingsRef();

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings[Setting::http_receive_timeout]);
    http_params->setKeepAliveTimeout(global_context->getServerSettings().keep_alive_timeout);

    Poco::Util::AbstractConfiguration::Keys protocols;
    config.keys("protocols", protocols);

    for (const auto & protocol : protocols)
    {
        if (!server_type.shouldStart(ServerType::Type::CUSTOM, protocol))
            continue;

        std::string prefix = "protocols." + protocol + ".";
        std::string port_name = prefix + "port";
        std::string description{"<undefined> protocol"};
        if (config.has(prefix + "description"))
            description = config.getString(prefix + "description");

        if (!config.has(prefix + "port"))
            continue;

        std::vector<std::string> hosts;
        if (config.has(prefix + "host"))
            hosts.push_back(config.getString(prefix + "host"));
        else
            hosts = listen_hosts;

        for (const auto & host : hosts)
        {
            bool is_secure = false;
            auto stack = buildProtocolStackFromConfig(config, server, protocol, http_params, async_metrics, is_secure);

            if (stack->empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' stack empty", protocol);

            createServer(
                config,
                host,
                port_name.c_str(),
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, host, port);
                    socket.setReceiveTimeout(settings[Setting::receive_timeout]);
                    socket.setSendTimeout(settings[Setting::send_timeout]);
                    return ProtocolServerAdapter(
                        host,
                        port_name.c_str(),
                        description + ": " + address.toString(),
                        std::make_unique<TCPServer>(stack.release(), server_pool, socket, new Poco::Net::TCPServerParams));
                });
        }
    }

    for (const auto & listen_host : listen_hosts)
    {
        if (server_type.shouldStart(ServerType::Type::HTTP))
        {
            /// HTTP
            constexpr auto port_name = "http_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                    socket.setSendTimeout(settings[Setting::http_send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "http://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            std::make_shared<HTTPContext>(global_context),
                            createHandlerFactory(server, config, async_metrics, "HTTPHandler-factory"),
                            server_pool,
                            socket,
                            http_params,
                            ProfileEvents::InterfaceHTTPReceiveBytes,
                            ProfileEvents::InterfaceHTTPSendBytes));
                });
        }

        if (server_type.shouldStart(ServerType::Type::HTTPS))
        {
            /// HTTPS
            constexpr auto port_name = "https_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
#if USE_SSL
                    Poco::Net::SecureServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                    socket.setSendTimeout(settings[Setting::http_send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "https://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            std::make_shared<HTTPContext>(global_context),
                            createHandlerFactory(server, config, async_metrics, "HTTPSHandler-factory"),
                            server_pool,
                            socket,
                            http_params,
                            ProfileEvents::InterfaceHTTPReceiveBytes,
                            ProfileEvents::InterfaceHTTPSendBytes));
#else
                    UNUSED(port);
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "HTTPS protocol is disabled because Poco library was built without NetSSL support.");
#endif
                });
        }

        if (server_type.shouldStart(ServerType::Type::TCP))
        {
            /// TCP
            constexpr auto port_name = "tcp_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::receive_timeout]);
                    socket.setSendTimeout(settings[Setting::send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "native protocol (tcp): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new TCPHandlerFactory(
                                server, false, false, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }

        if (server_type.shouldStart(ServerType::Type::TCP_WITH_PROXY))
        {
            /// TCP with PROXY protocol, see https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
            constexpr auto port_name = "tcp_with_proxy_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::receive_timeout]);
                    socket.setSendTimeout(settings[Setting::send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "native protocol (tcp) with PROXY: " + address.toString(),
                        std::make_unique<TCPServer>(
                            new TCPHandlerFactory(
                                server, false, true, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }

        if (server_type.shouldStart(ServerType::Type::TCP_SECURE))
        {
            /// TCP with SSL
            constexpr auto port_name = "tcp_port_secure";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
#if USE_SSL
                    Poco::Net::SecureServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::receive_timeout]);
                    socket.setSendTimeout(settings[Setting::send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "secure native protocol (tcp_secure): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new TCPHandlerFactory(
                                server, true, false, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
#else
                    UNUSED(port);
                    throw Exception(
                        ErrorCodes::SUPPORT_IS_DISABLED,
                        "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
                });
        }

        if (server_type.shouldStart(ServerType::Type::MYSQL))
        {
            constexpr auto port_name = "mysql_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(Poco::Timespan());
                    socket.setSendTimeout(settings[Setting::send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "MySQL compatibility protocol: " + address.toString(),
                        std::make_unique<TCPServer>(
                            new MySQLHandlerFactory(
                                server, ProfileEvents::InterfaceMySQLReceiveBytes, ProfileEvents::InterfaceMySQLSendBytes),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }

        if (server_type.shouldStart(ServerType::Type::POSTGRESQL))
        {
            constexpr auto port_name = "postgresql_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(Poco::Timespan());
                    socket.setSendTimeout(settings[Setting::send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "PostgreSQL compatibility protocol: " + address.toString(),
                        std::make_unique<TCPServer>(
                            new PostgreSQLHandlerFactory(
                                server, ProfileEvents::InterfacePostgreSQLReceiveBytes, ProfileEvents::InterfacePostgreSQLSendBytes),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }

#if USE_GRPC
        if (server_type.shouldStart(ServerType::Type::GRPC))
        {
            constexpr auto port_name = "grpc_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::SocketAddress server_address(listen_host, port);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "gRPC protocol: " + server_address.toString(),
                        std::make_unique<GRPCServer>(server, makeSocketAddress(listen_host, port, logger)));
                });
        }
#endif
        if (server_type.shouldStart(ServerType::Type::PROMETHEUS))
        {
            /// Prometheus (if defined and not setup yet with http_port)
            constexpr auto port_name = "prometheus.port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                    socket.setSendTimeout(settings[Setting::http_send_timeout]);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "Prometheus: http://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            std::make_shared<HTTPContext>(global_context),
                            createHandlerFactory(server, config, async_metrics, "PrometheusHandler-factory"),
                            server_pool,
                            socket,
                            http_params,
                            ProfileEvents::InterfacePrometheusReceiveBytes,
                            ProfileEvents::InterfacePrometheusSendBytes));
                });
        }
    }
}

size_t ProtocolServersManager::stopServers(const ServerSettings & server_settings, std::mutex & servers_lock)
{
    if (servers.empty())
    {
        return 0;
    }

    LOG_DEBUG(logger, "Waiting for current connections to close.");

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
        LOG_WARNING(logger, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
    else
        LOG_INFO(logger, "Closed all listening sockets.");

    /// Wait for unfinished backups and restores.
    /// This must be done after closing listening sockets (no more backups/restores) but before ProcessList::killAllQueries
    /// (because killAllQueries() will cancel all running backups/restores).
    if (server_settings.shutdown_wait_backups_and_restores)
        global_context->waitAllBackupsAndRestores();
    /// Killing remaining queries.
    if (!server_settings.shutdown_wait_unfinished_queries)
        global_context->getProcessList().killAllQueries();

    if (current_connections)
        current_connections = waitServersToFinish(servers, servers_lock, server_settings.shutdown_wait_unfinished);

    if (current_connections)
        LOG_WARNING(
            logger,
            "Closed connections. But {} remain."
            " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>",
            current_connections);
    else
        LOG_INFO(logger, "Closed connections.");
    return current_connections;
}

std::unique_ptr<TCPProtocolStackFactory> ProtocolServersManager::buildProtocolStackFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    IServer & server,
    const std::string & protocol,
    Poco::Net::HTTPServerParams::Ptr http_params,
    AsynchronousMetrics & async_metrics,
    bool & is_secure) const
{
    auto create_factory = [&](const std::string & type, const std::string & conf_name) -> TCPServerConnectionFactory::Ptr
    {
        if (type == "tcp")
            return TCPServerConnectionFactory::Ptr(new TCPHandlerFactory(
                server, false, false, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes));

        if (type == "tls")
#if USE_SSL
            return TCPServerConnectionFactory::Ptr(new TLSHandlerFactory(server, conf_name));
#else
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif

        if (type == "proxy1")
            return TCPServerConnectionFactory::Ptr(new ProxyV1HandlerFactory(server, conf_name));
        if (type == "mysql")
            return TCPServerConnectionFactory::Ptr(
                new MySQLHandlerFactory(server, ProfileEvents::InterfaceMySQLReceiveBytes, ProfileEvents::InterfaceMySQLSendBytes));
        if (type == "postgres")
            return TCPServerConnectionFactory::Ptr(new PostgreSQLHandlerFactory(
                server, ProfileEvents::InterfacePostgreSQLReceiveBytes, ProfileEvents::InterfacePostgreSQLSendBytes));
        if (type == "http")
            return TCPServerConnectionFactory::Ptr(new HTTPServerConnectionFactory(
                std::make_shared<HTTPContext>(global_context),
                http_params,
                createHandlerFactory(server, config, async_metrics, "HTTPHandler-factory"),
                ProfileEvents::InterfaceHTTPReceiveBytes,
                ProfileEvents::InterfaceHTTPSendBytes));
        if (type == "prometheus")
            return TCPServerConnectionFactory::Ptr(new HTTPServerConnectionFactory(
                std::make_shared<HTTPContext>(global_context),
                http_params,
                createHandlerFactory(server, config, async_metrics, "PrometheusHandler-factory"),
                ProfileEvents::InterfacePrometheusReceiveBytes,
                ProfileEvents::InterfacePrometheusSendBytes));
        if (type == "interserver")
            return TCPServerConnectionFactory::Ptr(new HTTPServerConnectionFactory(
                std::make_shared<HTTPContext>(global_context),
                http_params,
                createHandlerFactory(server, config, async_metrics, "InterserverIOHTTPHandler-factory"),
                ProfileEvents::InterfaceInterserverReceiveBytes,
                ProfileEvents::InterfaceInterserverSendBytes));

        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol configuration error, unknown protocol name '{}'", type);
    };

    std::string conf_name = "protocols." + protocol;
    std::string prefix = conf_name + ".";
    std::unordered_set<std::string> pset{conf_name};

    auto stack = std::make_unique<TCPProtocolStackFactory>(server, conf_name);

    while (true)
    {
        // if there is no "type" - it's a reference to another protocol and this is just an endpoint
        if (config.has(prefix + "type"))
        {
            std::string type = config.getString(prefix + "type");
            if (type == "tls")
            {
                if (is_secure)
                    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' contains more than one TLS layer", protocol);
                is_secure = true;
            }

            stack->append(create_factory(type, conf_name));
        }

        if (!config.has(prefix + "impl"))
            break;

        conf_name = "protocols." + config.getString(prefix + "impl");
        prefix = conf_name + ".";

        if (!pset.insert(conf_name).second)
            throw Exception(
                ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' configuration contains a loop on '{}'", protocol, conf_name);
    }

    return stack;
}

}
