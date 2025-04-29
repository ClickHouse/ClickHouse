#include "ProxyServer.h"

#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <base/safeExit.h>
#include <Common/ErrorHandlers.h>
#include <Common/Exception.h>
#include <Common/ThreadStatus.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/logger_useful.h>
#include <Common/makeSocketAddress.h>
#include <Common/scope_guard_safe.h>
#include <Common/ProfileEvents.h>
#include <Poco/Environment.h>
#include <Poco/Util/HelpFormatter.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/waitServersToFinish.h>
#include <ProxyServer/TCPHandlerFactory.h>

#include <iostream>
#include <string>

#include <Common/Jemalloc.h>

#include <incbin.h>
/// A minimal file used when the server is run without installation
INCBIN(proxy_resource_embedded_xml, SOURCE_DIR "/programs/proxy-server/embedded.xml");

namespace ProfileEvents
{
    extern const Event InterfaceNativeSendBytes;
    extern const Event InterfaceNativeReceiveBytes;
}

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


namespace DB {
namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int NETWORK_ERROR;
}
}

namespace Proxy
{

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

void ProxyServer::createServer(
    Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    bool start_server,
    std::vector<DB::ProtocolServerAdapter> & servers,
    CreateServerFunc && func) const
{
    /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
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
        throw DB::Exception(DB::ErrorCodes::NETWORK_ERROR, "Listen [{}]:{} failed: {}", listen_host, port, DB::getCurrentExceptionMessage(false));
    }
}

void ProxyServer::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

int ProxyServer::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(ProxyServer::options());
        auto header_str = fmt::format(
            "{} [OPTION] [-- [ARG]...]\n"
            "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
            commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    return Application::run(); // NOLINT
}

void ProxyServer::initialize(Poco::Util::Application & self)
{
    DB::ConfigProcessor::registerEmbeddedConfig("config.xml", std::string_view(reinterpret_cast<const char *>(gproxy_resource_embedded_xmlData), gproxy_resource_embedded_xmlSize));
    BaseDaemon::initialize(self);
    logger().information("starting up");

    LOG_INFO(
        &logger(),
        "OS name: {}, version: {}, architecture: {}",
        Poco::Environment::osName(),
        Poco::Environment::osVersion(),
        Poco::Environment::osArchitecture());
}

std::string ProxyServer::getDefaultCorePath() const
{
    return "/var/dumps/clickhouse-proxy";
}

void ProxyServer::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(Poco::Util::Option("help", "h", "show help and exit").required(false).repeatable(false).binding("help"));
    BaseDaemon::defineOptions(options);
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

    // StackTrace::setShowAddresses(false); // TODO move to config

    Poco::ThreadPool server_pool(
        /* minCapacity */ 3,
        /* maxCapacity */ 4096, // TODO move to config (max_connections)
        /* idleTime */ 60,
        /* stackSize */ POCO_THREAD_STACK_SIZE);

    std::mutex servers_lock;
    std::vector<DB::ProtocolServerAdapter> servers;

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler); // ???? TODO: needed?

    auto router = std::make_shared<Router>(config());

    // TODO register config reloader

    const auto listen_hosts = getListenHosts(config());

    {
        {
            std::lock_guard lock(servers_lock);
            createServers(config(), router, listen_hosts, server_pool, servers);
            if (servers.empty())
                throw DB::Exception(
                    DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                    "No servers started (add valid listen_host and 'tcp_port' or 'http_port' "
                    "to configuration file.)");
        }

        if (servers.empty())
            throw DB::Exception(
                DB::ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "No servers started (add valid listen_host and 'tcp_port' or 'http_port' "
                "to configuration file.)");

        // main_config_reloader->start(); // TODO

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

        // TODO: systemdNotify?

        /*
        SCOPE_EXIT_SAFE({
            LOG_DEBUG(log, "Received termination signal.");

            /// Stop reloading of the main config. This must be done before everything else because it
            /// can try to access/modify already deleted objects.
            /// E.g. it can recreate new servers or it may pass a changed config to some destroyed parts of ContextSharedPart.
            // main_config_reloader.reset();

            is_cancelled = true;

            LOG_DEBUG(log, "Waiting for current connections to close.");

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
                LOG_WARNING(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            size_t wait_limit_seconds = server_settings[DB::ServerSetting::shutdown_wait_unfinished];

            if (current_connections)
                current_connections = waitServersToFinish(servers, servers_lock, wait_limit_seconds);

            if (current_connections)
                LOG_WARNING(
                    log,
                    "Closed connections. But {} remain."
                    " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>",
                    current_connections);
            else
                LOG_INFO(log, "Closed connections.");

            if (current_connections)
            {
                /// There is no better way to force connections to close in Poco.
                /// Otherwise connection handlers will continue to live
                /// (they are effectively dangling objects, but they use global thread pool
                ///  and global thread pool destructor will wait for threads, preventing server shutdown).

                LOG_WARNING(log, "Will shutdown forcefully.");
                safeExit(0);
            }
        });
        */

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

void ProxyServer::createServers(
    Poco::Util::AbstractConfiguration & config,
    RouterPtr router,
    const std::vector<std::string> & listen_hosts,
    Poco::ThreadPool &server_pool,
    std::vector<DB::ProtocolServerAdapter> & servers,
    bool start_servers,
    const DB::ServerType & server_type)
{
    DB::ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config);

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(30); // TODO move to config (http_receive_timeout)
    http_params->setKeepAliveTimeout(30); // TODO move to config (keep_alive_timeout)
    http_params->setMaxKeepAliveRequests(10000); // TODO move to config (max_keep_alive_requests)

    for (const auto & listen_host : listen_hosts)
    {
        const char * port_name;

        /* TODO HTTP
        if (server_type.shouldStart(ServerType::Type::HTTP))
        {
            /// HTTP
            port_name = "http_port";
            createServer(
                config,
                listen_host,
                port_name,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port);
                    socket.setReceiveTimeout(30); // TODO move to config (http_receive_timeout)
                    socket.setSendTimeout(30); // TODO move to config (http_send_timeout)

                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "http://" + address.toString(),
                        std::make_unique<HTTPServer>(
                            httpContext(),
                            createHandlerFactory(*this, config, "HTTPHandler-factory"),
                            server_pool,
                            socket,
                            http_params,
                            ProfileEvents::InterfaceHTTPReceiveBytes,
                            ProfileEvents::InterfaceHTTPSendBytes));
                });
        }
        */

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
                    socket.setReceiveTimeout(300); // TODO move to config (receive_timeout)
                    socket.setSendTimeout(300); // TODO move to config (send_timeout)
                    return DB::ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "native protocol (tcp): " + address.toString(),
                        std::make_unique<DB::TCPServer>(
                            new TCPHandlerFactory(
                                *this,
                                /* secure */ false,
                                router
                            ),
                            server_pool,
                            socket,
                            new Poco::Net::TCPServerParams));
                });
        }
    }
}
}
