#include "ProxyServer.h"

#include <iostream>
#include <string>

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/waitServersToFinish.h>
#include <base/safeExit.h>
#include <Poco/Environment.h>
#include <Poco/Util/HelpFormatter.h>
#include <Common/ErrorHandlers.h>
#include <Common/Exception.h>
#include <Common/Jemalloc.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/logger_useful.h>
#include <Common/makeSocketAddress.h>
#include <Common/scope_guard_safe.h>

#include <ProxyServer/TCPHandlerFactory.h>

#include <incbin.h>
/// A minimal file used when the server is run without installation
INCBIN(proxy_resource_embedded_xml, SOURCE_DIR "/programs/proxy-server/embedded.xml");

int mainEntryClickHouseProxyServer(int argc, char ** argv)
{
    Proxy::ProxyServer app;

    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return static_cast<UInt8>(code) ? code : 1;
    }
}


namespace DB
{
namespace ErrorCodes
{
extern const int NO_ELEMENTS_IN_CONFIG;
extern const int NETWORK_ERROR;
}
}

namespace Proxy
{

namespace
{
std::vector<std::string> getListenHosts(const Poco::Util::AbstractConfiguration & config)
{
    auto listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
    }
    return listen_hosts;
}
}

void ProxyServer::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(Poco::Util::Option("help", "h", "show help and exit").required(false).repeatable(false).binding("help"));
    BaseDaemon::defineOptions(options);
}

int ProxyServer::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(ProxyServer::options());
        auto header_str = fmt::format(
            "{} [OPTION] [-- [ARG]...]\n"
            "positional arguments can be used to rewrite config.xml properties, for example, --tcp_port=9001",
            commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    return Application::run(); // NOLINT
}

void ProxyServer::initialize(Poco::Util::Application & self)
{
    DB::ConfigProcessor::registerEmbeddedConfig(
        "config.xml", std::string_view(reinterpret_cast<const char *>(gproxy_resource_embedded_xmlData), gproxy_resource_embedded_xmlSize));
    BaseDaemon::initialize(self);
    logger().information("starting up");

    LOG_INFO(
        &logger(),
        "OS name: {}, version: {}, architecture: {}",
        Poco::Environment::osName(),
        Poco::Environment::osVersion(),
        Poco::Environment::osArchitecture());
}

void ProxyServer::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

int ProxyServer::main(const std::vector<std::string> & /*args*/)
try
{
#if USE_JEMALLOC
    DB::setJemallocBackgroundThreads(true);
#endif

    Poco::Logger * log = &logger();

    DB::MainThreadStatus::getInstance();

    DB::ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config());

    // StackTrace::setShowAddresses(false); // TODO: move to config

    Poco::ThreadPool server_pool(
        /* minCapacity */ 3,
        /* maxCapacity */ 4096, // TODO: move to config (max_connections)
        /* idleTime */ 60,
        /* stackSize */ POCO_THREAD_STACK_SIZE);

    std::mutex servers_lock;
    std::vector<DB::ProtocolServerAdapter> servers;

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    auto router = std::make_shared<Router>(config());

    // TODO: register config reloader

    const auto listen_hosts = getListenHosts(config());

    {
        {
            std::lock_guard lock(servers_lock);
            createServers(config(), router, listen_hosts, server_pool, servers);
            if (servers.empty())
                throw DB::Exception(
                    DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                    "No servers started (add valid listen_host and 'tcp_port' "
                    "to configuration file.)");
        }

        if (servers.empty())
            throw DB::Exception(
                DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "No servers started (add valid listen_host and 'tcp_port' "
                "to configuration file.)");

        // main_config_reloader->start(); // TODO: support config reloading

        {
            std::lock_guard lock(servers_lock);
            for (auto & server : servers)
            {
                server.start();
                LOG_INFO(log, "Listening for {}", server.getDescription());
            }

            LOG_INFO(log, "Ready for connections.");
            LOG_WARNING(log, "ATTENTION! The proxy server mode is under development. Use it at your own risk");
        }

        // TODO: support systemd

        // TODO: graceful shutdown

        waitForTerminationRequest();
    }

    return Application::EXIT_OK;
}
catch (...)
{
    /// Poco does not provide stacktrace.
    DB::tryLogCurrentException("Application");
    auto code = DB::getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : -1;
}

std::string ProxyServer::getDefaultCorePath() const
{
    return "/var/dumps/clickhouse-proxy";
}

Poco::Net::SocketAddress ProxyServer::socketBindListen(
    const Poco::Util::AbstractConfiguration & config,
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    [[maybe_unused]] bool secure) const
{
    auto address = DB::makeSocketAddress(host, port, &logger());
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ false);
    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(&logger(), "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config.getUInt("listen_backlog", 4096));

    return address;
}

void ProxyServer::createServer(
    Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    bool start_server,
    std::vector<DB::ProtocolServerAdapter> & servers,
    CreateServerFunc && func) const
{
    /// For testing purposes, user may omit tcp_port in configuration file.
    if (config.getString(port_name, "").empty())
        return;

    /// If we already have an active server for this listen_host/port_name, don't create it again
    for (const auto & server : servers)
    {
        if (!server.isStopping() && server.getListenHost() == listen_host && server.getPortName() == port_name)
            return;
    }

    auto port = config.getInt(port_name);
    try
    {
        servers.push_back(func(port));
        if (start_server)
        {
            servers.back().start();
            LOG_INFO(&logger(), "Listening for {}", servers.back().getDescription());
        }
    }
    catch (const Poco::Exception &)
    {
        throw DB::Exception(
            DB::ErrorCodes::NETWORK_ERROR, "Listen [{}]:{} failed: {}", listen_host, port, DB::getCurrentExceptionMessage(false));
    }
}

void ProxyServer::createServers(
    Poco::Util::AbstractConfiguration & config,
    RouterPtr router,
    const std::vector<std::string> & listen_hosts,
    Poco::ThreadPool & server_pool,
    std::vector<DB::ProtocolServerAdapter> & servers,
    bool start_servers,
    const DB::ServerType & server_type)
{
    DB::ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config);

    for (const auto & listen_host : listen_hosts)
    {
        const char * port_name;

        // TODO: support TCP Secure, HTTP(S) and the rest

        if (server_type.shouldStart(DB::ServerType::Type::TCP))
        {
            LOG_INFO(&logger(), "Creating TCP");
            /// TCP
            port_name = "tcp_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                servers,
                [&](UInt16 port) -> DB::ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(300); // TODO: move to config (receive_timeout)
                    socket.setSendTimeout(300); // TODO: move to config (send_timeout)
                    return DB::ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "native protocol (tcp): " + address.toString(),
                        std::make_unique<DB::TCPServer>(
                            new TCPHandlerFactory(
                                *this,
                                /* secure */ false,
                                false,
                                router),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }

        if (server_type.shouldStart(DB::ServerType::Type::TCP_WITH_PROXY))
        {
            /// TCP with PROXY protocol, see https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
            port_name = "tcp_with_proxy_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                servers,
                [&](UInt16 port) -> DB::ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(300); // TODO: move to config (receive_timeout)
                    socket.setSendTimeout(300); // TODO: move to config (send_timeout)
                    return DB::ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "native protocol (tcp) with PROXY: " + address.toString(),
                        std::make_unique<DB::TCPServer>(
                            new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ true, router),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }
    }
}

}
