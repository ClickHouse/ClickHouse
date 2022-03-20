#include "Keeper.h"

#include <Common/ClickHouseRevision.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/DNSResolver.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Coordination/Defines.h>
#include <Common/Config/ConfigReloader.h>
#include <Server/TCPServer.h>
#include <filesystem>
#include <IO/UseSSL.h>
#include <Core/ServerUUID.h>
#include <base/logger_useful.h>
#include <base/ErrorHandlers.h>
#include <base/scope_guard.h>
#include <base/safeExit.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerParams.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Version.h>
#include <Poco/Environment.h>
#include <sys/stat.h>
#include <pwd.h>
#include <Coordination/FourLetterCommand.h>

#include "config_core.h"
#include "Common/config_version.h"

#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SecureServerSocket.h>
#endif

#include <Server/ProtocolServerAdapter.h>
#include <Server/KeeperTCPHandlerFactory.h>


int mainEntryClickHouseKeeper(int argc, char ** argv)
{
    DB::Keeper app;

    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NETWORK_ERROR;
    extern const int MISMATCHING_USERS_FOR_PROCESS_AND_DATA;
    extern const int FAILED_TO_GETPWUID;
}

namespace
{

int waitServersToFinish(std::vector<DB::ProtocolServerAdapter> & servers, size_t seconds_to_wait)
{
    const int sleep_max_ms = 1000 * seconds_to_wait;
    const int sleep_one_ms = 100;
    int sleep_current_ms = 0;
    int current_connections = 0;
    for (;;)
    {
        current_connections = 0;

        for (auto & server : servers)
        {
            server.stop();
            current_connections += server.currentConnections();
        }

        if (!current_connections)
            break;

        sleep_current_ms += sleep_one_ms;
        if (sleep_current_ms < sleep_max_ms)
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_one_ms));
        else
            break;
    }
    return current_connections;
}

Poco::Net::SocketAddress makeSocketAddress(const std::string & host, UInt16 port, Poco::Logger * log)
{
    Poco::Net::SocketAddress socket_address;
    try
    {
        socket_address = Poco::Net::SocketAddress(host, port);
    }
    catch (const Poco::Net::DNSException & e)
    {
        const auto code = e.code();
        if (code == EAI_FAMILY
#if defined(EAI_ADDRFAMILY)
                    || code == EAI_ADDRFAMILY
#endif
           )
        {
            LOG_ERROR(log, "Cannot resolve listen_host ({}), error {}: {}. "
                "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                "specify IPv4 address to listen in <listen_host> element of configuration "
                "file. Example: <listen_host>0.0.0.0</listen_host>",
                host, e.code(), e.message());
        }

        throw;
    }
    return socket_address;
}

std::string getUserName(uid_t user_id)
{
    /// Try to convert user id into user name.
    auto buffer_size = sysconf(_SC_GETPW_R_SIZE_MAX);
    if (buffer_size <= 0)
        buffer_size = 1024;
    std::string buffer;
    buffer.reserve(buffer_size);

    struct passwd passwd_entry;
    struct passwd * result = nullptr;
    const auto error = getpwuid_r(user_id, &passwd_entry, buffer.data(), buffer_size, &result);

    if (error)
        throwFromErrno("Failed to find user name for " + toString(user_id), ErrorCodes::FAILED_TO_GETPWUID, error);
    else if (result)
        return result->pw_name;
    return toString(user_id);
}

}

Poco::Net::SocketAddress Keeper::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
{
    auto address = makeSocketAddress(host, port, &logger());
#if !defined(POCO_CLICKHOUSE_PATCH) || POCO_VERSION < 0x01090100
    if (secure)
        /// Bug in old (<1.9.1) poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
        /// https://github.com/pocoproject/poco/pull/2257
        socket.bind(address, /* reuseAddress = */ true);
    else
#endif
#if POCO_VERSION < 0x01080000
    socket.bind(address, /* reuseAddress = */ true);
#else
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config().getBool("listen_reuse_port", false));
#endif

    socket.listen(/* backlog = */ config().getUInt("listen_backlog", 64));

    return address;
}

void Keeper::createServer(const std::string & listen_host, const char * port_name, bool listen_try, CreateServerFunc && func) const
{
    /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
    if (!config().has(port_name))
        return;

    auto port = config().getInt(port_name);
    try
    {
        func(port);
    }
    catch (const Poco::Exception &)
    {
        std::string message = "Listen [" + listen_host + "]:" + std::to_string(port) + " failed: " + getCurrentExceptionMessage(false);

        if (listen_try)
        {
            LOG_WARNING(&logger(), "{}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                message);
        }
        else
        {
            throw Exception{message, ErrorCodes::NETWORK_ERROR};
        }
    }
}

void Keeper::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

int Keeper::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(Keeper::options());
        auto header_str = fmt::format("{} [OPTION] [-- [ARG]...]\n"
                                      "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
                                      commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    if (config().hasOption("version"))
    {
        std::cout << DBMS_NAME << " keeper version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
        return 0;
    }

    return Application::run(); // NOLINT
}

void Keeper::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("starting up");

    LOG_INFO(&logger(), "OS Name = {}, OS Version = {}, OS Architecture = {}",
        Poco::Environment::osName(),
        Poco::Environment::osVersion(),
        Poco::Environment::osArchitecture());
}

std::string Keeper::getDefaultConfigFileName() const
{
    return "keeper_config.xml";
}

void Keeper::defineOptions(Poco::Util::OptionSet & options)
{
    options.addOption(
        Poco::Util::Option("help", "h", "show help and exit")
            .required(false)
            .repeatable(false)
            .binding("help"));
    options.addOption(
        Poco::Util::Option("version", "V", "show version and exit")
            .required(false)
            .repeatable(false)
            .binding("version"));
    BaseDaemon::defineOptions(options);
}

int Keeper::main(const std::vector<std::string> & /*args*/)
{
    Poco::Logger * log = &logger();

    UseSSL use_ssl;

    MainThreadStatus::getInstance();

#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    LOG_WARNING(log, "Keeper was built in debug mode. It will work slowly.");
#endif

#if defined(SANITIZER)
    LOG_WARNING(log, "Keeper was built with sanitizer. It will work slowly.");
#endif

    if (!config().has("keeper_server"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Keeper configuration (<keeper_server> section) not found in config");

    std::string path;

    if (config().has("keeper_server.storage_path"))
        path = config().getString("keeper_server.storage_path");
    else if (config().has("keeper_server.log_storage_path"))
        path = std::filesystem::path(config().getString("keeper_server.log_storage_path")).parent_path();
    else if (config().has("keeper_server.snapshot_storage_path"))
        path = std::filesystem::path(config().getString("keeper_server.snapshot_storage_path")).parent_path();
    else
        path = std::filesystem::path{KEEPER_DEFAULT_PATH};


    /// Check that the process user id matches the owner of the data.
    const auto effective_user_id = geteuid();
    struct stat statbuf;
    if (stat(path.c_str(), &statbuf) == 0 && effective_user_id != statbuf.st_uid)
    {
        const auto effective_user = getUserName(effective_user_id);
        const auto data_owner = getUserName(statbuf.st_uid);
        std::string message = "Effective user of the process (" + effective_user +
            ") does not match the owner of the data (" + data_owner + ").";
        if (effective_user_id == 0)
        {
            message += " Run under 'sudo -u " + data_owner + "'.";
            throw Exception(message, ErrorCodes::MISMATCHING_USERS_FOR_PROCESS_AND_DATA);
        }
        else
        {
            LOG_WARNING(log, fmt::runtime(message));
        }
    }

    DB::ServerUUID::load(path + "/uuid", log);

    std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");

    GlobalThreadPool::initialize(
        config().getUInt("max_thread_pool_size", 100),
        config().getUInt("max_thread_pool_free_size", 1000),
        config().getUInt("thread_pool_queue_size", 10000)
    );

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", DateLUT::instance().getTimeZone());

    /// Don't want to use DNS cache
    DNSResolver::instance().setDisableCacheFlag();

    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));

    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config(), "", "listen_host");

    bool listen_try = config().getBool("listen_try", false);
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
        listen_try = true;
    }

    auto servers = std::make_shared<std::vector<ProtocolServerAdapter>>();

    /// Initialize keeper RAFT. Do nothing if no keeper_server in config.
    tiny_context.initializeKeeperDispatcher(/* start_async = */false);
    FourLetterCommandFactory::registerCommands(*tiny_context.getKeeperDispatcher());

    auto config_getter = [this] () -> const Poco::Util::AbstractConfiguration &
    {
        return tiny_context.getConfigRef();
    };

    for (const auto & listen_host : listen_hosts)
    {
        /// TCP Keeper
        const char * port_name = "keeper_server.tcp_port";
        createServer(listen_host, port_name, listen_try, [&](UInt16 port)
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(socket, listen_host, port);
            socket.setReceiveTimeout(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC));
            socket.setSendTimeout(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC));
            servers->emplace_back(
                listen_host,
                port_name,
                "Keeper (tcp): " + address.toString(),
                std::make_unique<TCPServer>(
                    new KeeperTCPHandlerFactory(
                        config_getter, tiny_context.getKeeperDispatcher(),
                            config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC),
                            config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC), false), server_pool, socket));
        });

        const char * secure_port_name = "keeper_server.tcp_port_secure";
        createServer(listen_host, secure_port_name, listen_try, [&](UInt16 port)
        {
#if USE_SSL
            Poco::Net::SecureServerSocket socket;
            auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC));
            socket.setSendTimeout(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC));
            servers->emplace_back(
                listen_host,
                secure_port_name,
                "Keeper with secure protocol (tcp_secure): " + address.toString(),
                std::make_unique<TCPServer>(
                    new KeeperTCPHandlerFactory(
                        config_getter, tiny_context.getKeeperDispatcher(),
                        config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC),
                        config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC), true), server_pool, socket));
#else
            UNUSED(port);
            throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        });
    }

    for (auto & server : *servers)
    {
        server.start();
        LOG_INFO(log, "Listening for {}", server.getDescription());
    }

    zkutil::EventPtr unused_event = std::make_shared<Poco::Event>();
    zkutil::ZooKeeperNodeCache unused_cache([] { return nullptr; });
    /// ConfigReloader have to strict parameters which are redundant in our case
    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        include_from_path,
        config().getString("path", ""),
        std::move(unused_cache),
        unused_event,
        [&](ConfigurationPtr config, bool /* initial_loading */)
        {
            if (config->has("keeper_server"))
                tiny_context.updateKeeperConfiguration(*config);
        },
        /* already_loaded = */ false);  /// Reload it right now (initial loading)

    SCOPE_EXIT({
        LOG_INFO(log, "Shutting down.");
        main_config_reloader.reset();

        LOG_DEBUG(log, "Waiting for current connections to Keeper to finish.");
        int current_connections = 0;
        for (auto & server : *servers)
        {
            server.stop();
            current_connections += server.currentConnections();
        }

        if (current_connections)
            LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
        else
            LOG_INFO(log, "Closed all listening sockets.");

        if (current_connections > 0)
            current_connections = waitServersToFinish(*servers, config().getInt("shutdown_wait_unfinished", 5));

        if (current_connections)
            LOG_INFO(log, "Closed connections to Keeper. But {} remain. Probably some users cannot finish their connections after context shutdown.", current_connections);
        else
            LOG_INFO(log, "Closed connections to Keeper.");

        tiny_context.shutdownKeeperDispatcher();

        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();

        LOG_DEBUG(log, "Destroyed global context.");

        if (current_connections)
        {
            LOG_INFO(log, "Will shutdown forcefully.");
            safeExit(0);
        }
    });


    buildLoggers(config(), logger());
    main_config_reloader->start();

    LOG_INFO(log, "Ready for connections.");

    waitForTerminationRequest();

    return Application::EXIT_OK;
}


void Keeper::logRevision() const
{
    Poco::Logger::root().information("Starting ClickHouse Keeper " + std::string{VERSION_STRING}
        + " with revision " + std::to_string(ClickHouseRevision::getVersionRevision())
        + ", " + build_id_info
        + ", PID " + std::to_string(getpid()));
}


}
