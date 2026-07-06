#include <ProxyServer.h>

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

#include <Common/Config/ConfigProcessor.h>

/// A minimal file used when the proxy is run without installation
constexpr unsigned char proxy_resource_embedded_xml[] =
{
#embed "embedded.xml"
};

int mainEntryClickHouseProxy(int argc, char ** argv);
int mainEntryClickHouseProxy(int argc, char ** argv)
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
extern const int INVALID_CONFIG_PARAMETER;
}
}

namespace Proxy
{

namespace
{
std::vector<std::string> getListenHosts(const Poco::Util::AbstractConfiguration & config, bool & listen_try)
{
    auto listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    if (listen_hosts.empty())
    {
        /// The implicit defaults include `::1`, which fails to bind on hosts with IPv6 disabled.
        /// As `clickhouse-server` does, treat a bind failure on an implicit listen host as non-fatal.
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
        listen_try = true;
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
        "config.xml",
        std::string_view(reinterpret_cast<const char *>(proxy_resource_embedded_xml), std::size(proxy_resource_embedded_xml)));
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
    DB::Jemalloc::setBackgroundThreads(true);
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

    bool listen_try = config().getBool("listen_try", false);
    const auto listen_hosts = getListenHosts(config(), listen_try);

    {
        {
            std::lock_guard lock(servers_lock);
            createServers(config(), router, listen_hosts, server_pool, servers, listen_try);
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

        waitForTerminationRequest();

        /// Stop the listeners so that in-flight relay loops observe the shutdown and exit instead of
        /// staying alive until their peers disconnect.
        LOG_DEBUG(log, "Shutting down.");
        {
            std::lock_guard lock(servers_lock);
            for (auto & server : servers)
                server.stop();
        }
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
    bool listen_try,
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

    /// Validate the port before narrowing it to `UInt16`, so an out-of-range value in the config
    /// fails loudly instead of silently binding a wrapped-around port. Port 0 means "any free port".
    const int port = config.getInt(port_name);
    if (port < 0 || port > 65535)
        throw DB::Exception(
            DB::ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Invalid port {} configured for '{}' (must be in the range 0..65535)",
            port,
            port_name);

    try
    {
        servers.push_back(func(static_cast<UInt16>(port)));
        if (start_server)
        {
            servers.back().start();
            LOG_INFO(&logger(), "Listening for {}", servers.back().getDescription());
        }
    }
    catch (const Poco::Exception &)
    {
        if (listen_try)
        {
            LOG_WARNING(
                &logger(),
                "Listen [{}]:{} failed: {}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, "
                "then consider specifying a not disabled IP version in the <listen_host> element of the configuration file.",
                listen_host,
                port,
                DB::getCurrentExceptionMessage(false));
            return;
        }
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
    bool listen_try,
    bool start_servers,
    const DB::ServerType & server_type)
{
    DB::ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config);

    for (const auto & listen_host : listen_hosts)
    {
        const char * port_name = nullptr;

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
                listen_try,
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
                listen_try,
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
