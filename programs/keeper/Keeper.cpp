#include "Keeper.h"

#include <Common/ClickHouseRevision.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/DNSResolver.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Coordination/Defines.h>
#include <filesystem>
#include <IO/UseSSL.h>
#include <Core/ServerUUID.h>
#include <base/logger_useful.h>
#include <base/ErrorHandlers.h>
#include <base/scope_guard.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerParams.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Version.h>
#include <Poco/Environment.h>
#include <sys/stat.h>
#include <pwd.h>

#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#   include "Common/config_version.h"
#endif

#include <Server/ProtocolServerAdapter.h>
#include <Server/InterfaceConfig.h>
#include <Server/InterfaceConfigUtil.h>
#include <Server/KeeperTCPHandlerFactory.h>

#if defined(OS_LINUX)
#   include <unistd.h>
#   include <sys/syscall.h>
#endif

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

[[noreturn]] void forceShutdown()
{
#if defined(THREAD_SANITIZER) && defined(OS_LINUX)
    /// Thread sanitizer tries to do something on exit that we don't need if we want to exit immediately,
    /// while connection handling threads are still run.
    (void)syscall(SYS_exit_group, 0);
    __builtin_unreachable();
#else
    _exit(0);
#endif
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

    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::KEEPER);

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
            LOG_WARNING(log, message);
        }
    }

    DB::ServerUUID::load(path + "/uuid", log);

    const Settings & settings = global_context->getSettingsRef();

    GlobalThreadPool::initialize(config().getUInt("max_thread_pool_size", 100));

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", DateLUT::instance().getTimeZone());

    /// Don't want to use DNS cache
    DNSResolver::instance().setDisableCacheFlag();

    const auto proxies = Util::parseProxies(config());
    const auto interfaces = Util::parseInterfaces(config(), settings, proxies);

    /// Initialize keeper RAFT. Do nothing if no keeper_server in config.
    global_context->initializeKeeperDispatcher(/* start_async = */false);

    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));

    auto servers = std::make_shared<std::vector<DB::ProtocolServerAdapter>>();
    *servers = Util::createServers(
        {"keeper_tcp"},
        interfaces, *this, server_pool, /*async_metrics = */ nullptr
    );

    for (auto & server : *servers)
        server.start();

    SCOPE_EXIT({
        LOG_INFO(log, "Shutting down.");

        global_context->shutdown();

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

        global_context->shutdownKeeperDispatcher();

        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();
        shared_context.reset();

        LOG_DEBUG(log, "Destroyed global context.");

        if (current_connections)
        {
            LOG_INFO(log, "Will shutdown forcefully.");
            forceShutdown();
        }
    });

    buildLoggers(config(), logger());

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
