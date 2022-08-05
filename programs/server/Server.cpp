#include "Server.h"

#include <memory>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <cerrno>
#include <pwd.h>
#include <unistd.h>
#include <Poco/Version.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Environment.h>
#include <Common/scope_guard_safe.h>
#include <base/defines.h>
#include <Common/logger_useful.h>
#include <base/phdr_cache.h>
#include <Common/ErrorHandlers.h>
#include <base/getMemoryAmount.h>
#include <base/getAvailableMemoryAmount.h>
#include <base/errnoToString.h>
#include <base/coverage.h>
#include <base/getFQDNOrHostName.h>
#include <base/safeExit.h>
#include <Common/MemoryTracker.h>
#include <Common/ClickHouseRevision.h>
#include <Common/DNSResolver.h>
#include <Common/CurrentMetrics.h>
#include <Common/ConcurrencyControl.h>
#include <Common/Macros.h>
#include <Common/ShellCommand.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/getExecutablePath.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/getMappedArea.h>
#include <Common/remapExecutable.h>
#include <Common/TLDListsHolder.h>
#include <Core/ServerUUID.h>
#include <IO/HTTPCommon.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/IOThreadPool.h>
#include <IO/UseSSL.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/UserDefinedSQLObjectsLoader.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Access/AccessControl.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/Cache/ExternalDataSourceCache.h>
#include <Storages/Cache/registerRemoteFileMetadatas.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Formats/registerFormats.h>
#include <Storages/registerStorages.h>
#include <QueryPipeline/ConnectionCollector.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Common/Config/ConfigReloader.h>
#include <Server/HTTPHandlerFactory.h>
#include "MetricsTransmitter.h"
#include <Common/StatusFile.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/TCPServer.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ThreadFuzzer.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/filesystemHelpers.h>
#include <Common/Elf.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <Server/MySQLHandlerFactory.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <Server/CertificateReloader.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/HTTP/HTTPServer.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <filesystem>

#include "config_core.h"
#include "Common/config_version.h"

#if defined(OS_LINUX)
#    include <sys/mman.h>
#    include <sys/ptrace.h>
#    include <Common/hasLinuxCapability.h>
#endif

#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

#if USE_NURAFT
#    include <Coordination/FourLetterCommand.h>
#    include <Server/KeeperTCPHandlerFactory.h>
#endif

#if USE_JEMALLOC
#    include <jemalloc/jemalloc.h>
#endif

namespace CurrentMetrics
{
    extern const Metric Revision;
    extern const Metric VersionInteger;
    extern const Metric MemoryTracking;
    extern const Metric MaxDDLEntryID;
    extern const Metric MaxPushedDDLEntryID;
}

namespace ProfileEvents
{
    extern const Event MainConfigLoads;
}

namespace fs = std::filesystem;

#if USE_JEMALLOC
static bool jemallocOptionEnabled(const char *name)
{
    bool value;
    size_t size = sizeof(value);

    if (mallctl(name, reinterpret_cast<void *>(&value), &size, /* newp= */ nullptr, /* newlen= */ 0))
        throw Poco::SystemException("mallctl() failed");

    return value;
}
#else
static bool jemallocOptionEnabled(const char *) { return 0; }
#endif

int mainEntryClickHouseServer(int argc, char ** argv)
{
    DB::Server app;

    if (jemallocOptionEnabled("opt.background_thread"))
    {
        LOG_ERROR(&app.logger(),
            "jemalloc.background_thread was requested, "
            "however ClickHouse uses percpu_arena and background_thread most likely will not give any benefits, "
            "and also background_thread is not compatible with ClickHouse watchdog "
            "(that can be disabled with CLICKHOUSE_WATCHDOG_ENABLE=0)");
    }

    /// Do not fork separate process from watchdog if we attached to terminal.
    /// Otherwise it breaks gdb usage.
    /// Can be overridden by environment variable (cannot use server config at this moment).
    if (argc > 0)
    {
        const char * env_watchdog = getenv("CLICKHOUSE_WATCHDOG_ENABLE");
        if (env_watchdog)
        {
            if (0 == strcmp(env_watchdog, "1"))
                app.shouldSetupWatchdog(argv[0]);

            /// Other values disable watchdog explicitly.
        }
        else if (!isatty(STDIN_FILENO) && !isatty(STDOUT_FILENO) && !isatty(STDERR_FILENO))
            app.shouldSetupWatchdog(argv[0]);
    }

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


namespace
{

void setupTmpPath(Poco::Logger * log, const std::string & path)
try
{
    LOG_DEBUG(log, "Setting up {} to store temporary data in it", path);

    fs::create_directories(path);

    /// Clearing old temporary files.
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_regular_file() && startsWith(it->path().filename(), "tmp"))
        {
            LOG_DEBUG(log, "Removing old temporary file {}", it->path().string());
            fs::remove(it->path());
        }
        else
            LOG_DEBUG(log, "Skipped file in temporary path {}", it->path().string());
    }
}
catch (...)
{
    DB::tryLogCurrentException(
        log,
        fmt::format(
            "Caught exception while setup temporary path: {}. It is ok to skip this exception as cleaning old temporary files is not "
            "necessary",
            path));
}

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

}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
    extern const int SYSTEM_ERROR;
    extern const int FAILED_TO_GETPWUID;
    extern const int MISMATCHING_USERS_FOR_PROCESS_AND_DATA;
    extern const int NETWORK_ERROR;
    extern const int CORRUPTED_DATA;
}


static std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty", ErrorCodes::INVALID_CONFIG_PARAMETER);
    if (path.back() != '/')
        path += '/';
    return std::move(path);
}

static std::string getUserName(uid_t user_id)
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

Poco::Net::SocketAddress Server::socketBindListen(
    const Poco::Util::AbstractConfiguration & config,
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    [[maybe_unused]] bool secure) const
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
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config.getBool("listen_reuse_port", false));
#endif

    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(&logger(), "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config.getUInt("listen_backlog", 4096));

    return address;
}

Strings getListenHosts(const Poco::Util::AbstractConfiguration & config)
{
    auto listen_hosts = DB::getMultipleValuesFromConfig(config, "", "listen_host");
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
    }
    return listen_hosts;
}

Strings getInterserverListenHosts(const Poco::Util::AbstractConfiguration & config)
{
    auto interserver_listen_hosts = DB::getMultipleValuesFromConfig(config, "", "interserver_listen_host");
    if (!interserver_listen_hosts.empty())
      return interserver_listen_hosts;

    /// Use more general restriction in case of emptiness
    return getListenHosts(config);
}

bool getListenTry(const Poco::Util::AbstractConfiguration & config)
{
    bool listen_try = config.getBool("listen_try", false);
    if (!listen_try)
        listen_try = DB::getMultipleValuesFromConfig(config, "", "listen_host").empty();
    return listen_try;
}


void Server::createServer(
    Poco::Util::AbstractConfiguration & config,
    const std::string & listen_host,
    const char * port_name,
    bool listen_try,
    bool start_server,
    std::vector<ProtocolServerAdapter> & servers,
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
        global_context->registerServerPort(port_name, port);
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

void Server::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

int Server::run()
{
    if (config().hasOption("help"))
    {
        Poco::Util::HelpFormatter help_formatter(Server::options());
        auto header_str = fmt::format("{} [OPTION] [-- [ARG]...]\n"
                                      "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
                                      commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    if (config().hasOption("version"))
    {
        std::cout << DBMS_NAME << " server version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
        return 0;
    }
    return Application::run(); // NOLINT
}

void Server::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("starting up");

    LOG_INFO(&logger(), "OS name: {}, version: {}, architecture: {}",
        Poco::Environment::osName(),
        Poco::Environment::osVersion(),
        Poco::Environment::osArchitecture());
}

std::string Server::getDefaultCorePath() const
{
    return getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH)) + "cores";
}

void Server::defineOptions(Poco::Util::OptionSet & options)
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


void checkForUsersNotInMainConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & config_path,
    const std::string & users_config_path,
    Poco::Logger * log)
{
    if (config.getBool("skip_check_for_incorrect_settings", false))
        return;

    if (config.has("users") || config.has("profiles") || config.has("quotas"))
    {
        /// We cannot throw exception here, because we have support for obsolete 'conf.d' directory
        /// (that does not correspond to config.d or users.d) but substitute configuration to both of them.

        LOG_ERROR(log, "The <users>, <profiles> and <quotas> elements should be located in users config file: {} not in main config {}."
            " Also note that you should place configuration changes to the appropriate *.d directory like 'users.d'.",
            users_config_path, config_path);
    }
}

/// Unused in other builds
#if defined(OS_LINUX)
static String readString(const String & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilEOF(contents, in);
    return contents;
}

static int readNumber(const String & path)
{
    ReadBufferFromFile in(path);
    int result;
    readText(result, in);
    return result;
}

#endif

static void sanityChecks(Server & server)
{
    std::string data_path = getCanonicalPath(server.config().getString("path", DBMS_DEFAULT_PATH));
    std::string logs_path = server.config().getString("logger.log", "");

    if (server.logger().is(Poco::Message::PRIO_TEST))
        server.context()->addWarningMessage("Server logging level is set to 'test' and performance is degraded. This cannot be used in production.");

#if defined(OS_LINUX)
    try
    {
        const char * filename = "/sys/devices/system/clocksource/clocksource0/current_clocksource";
        if (readString(filename).find("tsc") == std::string::npos)
            server.context()->addWarningMessage("Linux is not using a fast TSC clock source. Performance can be degraded. Check " + String(filename));
    }
    catch (...)
    {
    }

    try
    {
        const char * filename = "/proc/sys/vm/overcommit_memory";
        if (readNumber(filename) == 2)
            server.context()->addWarningMessage("Linux memory overcommit is disabled. Check " + String(filename));
    }
    catch (...)
    {
    }

    try
    {
        const char * filename = "/sys/kernel/mm/transparent_hugepage/enabled";
        if (readString(filename).find("[always]") != std::string::npos)
            server.context()->addWarningMessage("Linux transparent hugepages are set to \"always\". Check " + String(filename));
    }
    catch (...)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/pid_max";
        if (readNumber(filename) < 30000)
            server.context()->addWarningMessage("Linux max PID is too low. Check " + String(filename));
    }
    catch (...)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/threads-max";
        if (readNumber(filename) < 30000)
            server.context()->addWarningMessage("Linux threads max count is too low. Check " + String(filename));
    }
    catch (...)
    {
    }

    std::string dev_id = getBlockDeviceId(data_path);
    if (getBlockDeviceType(dev_id) == BlockDeviceType::ROT && getBlockDeviceReadAheadBytes(dev_id) == 0)
        server.context()->addWarningMessage("Rotational disk with disabled readahead is in use. Performance can be degraded. Used for data: " + String(data_path));
#endif

    try
    {
        if (getAvailableMemoryAmount() < (2l << 30))
            server.context()->addWarningMessage("Available memory at server startup is too low (2GiB).");

        if (!enoughSpaceInDirectory(data_path, 1ull << 30))
            server.context()->addWarningMessage("Available disk space for data at server startup is too low (1GiB): " + String(data_path));

        if (!logs_path.empty())
        {
            auto logs_parent = fs::path(logs_path).parent_path();
            if (!enoughSpaceInDirectory(logs_parent, 1ull << 30))
                server.context()->addWarningMessage("Available disk space for logs at server startup is too low (1GiB): " + String(logs_parent));
        }
    }
    catch (...)
    {
    }
}

int Server::main(const std::vector<std::string> & /*args*/)
{
    Poco::Logger * log = &logger();

    UseSSL use_ssl;

    MainThreadStatus::getInstance();

    StackTrace::setShowAddresses(config().getBool("show_addresses_in_stack_traces", true));

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();
    registerDictionaries();
    registerDisks();
    registerFormats();
    registerRemoteFileMetadatas();

    CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::getVersionRevision());
    CurrentMetrics::set(CurrentMetrics::VersionInteger, ClickHouseRevision::getVersionInteger());

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases, ...
      */
    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);

#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    global_context->addWarningMessage("Server was built in debug mode. It will work slowly.");
#endif

    if (ThreadFuzzer::instance().isEffective())
        global_context->addWarningMessage("ThreadFuzzer is enabled. Application will run slowly and unstable.");

#if defined(SANITIZER)
    global_context->addWarningMessage("Server was built with sanitizer. It will work slowly.");
#endif

    sanityChecks(*this);

    // Initialize global thread pool. Do it before we fetch configs from zookeeper
    // nodes (`from_zk`), because ZooKeeper interface uses the pool. We will
    // ignore `max_thread_pool_size` in configs we fetch from ZK, but oh well.
    GlobalThreadPool::initialize(
        config().getUInt("max_thread_pool_size", 10000),
        config().getUInt("max_thread_pool_free_size", 1000),
        config().getUInt("thread_pool_queue_size", 10000)
    );

    IOThreadPool::initialize(
        config().getUInt("max_io_thread_pool_size", 100),
        config().getUInt("max_io_thread_pool_free_size", 0),
        config().getUInt("io_thread_pool_queue_size", 10000));

    /// Initialize global local cache for remote filesystem.
    if (config().has("local_cache_for_remote_fs"))
    {
        bool enable = config().getBool("local_cache_for_remote_fs.enable", false);
        if (enable)
        {
            String root_dir = config().getString("local_cache_for_remote_fs.root_dir");
            UInt64 limit_size = config().getUInt64("local_cache_for_remote_fs.limit_size");
            UInt64 bytes_read_before_flush
                = config().getUInt64("local_cache_for_remote_fs.bytes_read_before_flush", DBMS_DEFAULT_BUFFER_SIZE);
            ExternalDataSourceCache::instance().initOnce(global_context, root_dir, limit_size, bytes_read_before_flush);
        }
    }

    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
    std::mutex servers_lock;
    std::vector<ProtocolServerAdapter> servers;
    std::vector<ProtocolServerAdapter> servers_to_start_before_tables;
    /// This object will periodically calculate some metrics.
    AsynchronousMetrics async_metrics(
        global_context, config().getUInt("asynchronous_metrics_update_period_s", 1),
        [&]() -> std::vector<ProtocolServerMetrics>
        {
            std::vector<ProtocolServerMetrics> metrics;
            metrics.reserve(servers_to_start_before_tables.size());
            for (const auto & server : servers_to_start_before_tables)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads()});

            std::lock_guard lock(servers_lock);
            for (const auto & server : servers)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads()});
            return metrics;
        }
    );

    ConnectionCollector::init(global_context, config().getUInt("max_threads_for_connection_collector", 10));

    bool has_zookeeper = config().has("zookeeper");

    zkutil::ZooKeeperNodeCache main_config_zk_node_cache([&] { return global_context->getZooKeeper(); });
    zkutil::EventPtr main_config_zk_changed_event = std::make_shared<Poco::Event>();
    if (loaded_config.has_zk_includes)
    {
        auto old_configuration = loaded_config.configuration;
        ConfigProcessor config_processor(config_path);
        loaded_config = config_processor.loadConfigWithZooKeeperIncludes(
            main_config_zk_node_cache, main_config_zk_changed_event, /* fallback_to_preprocessed = */ true);
        config_processor.savePreprocessedConfig(loaded_config, config().getString("path", DBMS_DEFAULT_PATH));
        config().removeConfiguration(old_configuration.get());
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    Settings::checkNoSettingNamesAtTopLevel(config(), config_path);

    const auto memory_amount = getMemoryAmount();

#if defined(OS_LINUX)
    std::string executable_path = getExecutablePath();

    if (!executable_path.empty())
    {
        /// Integrity check based on checksum of the executable code.
        /// Note: it is not intended to protect from malicious party,
        /// because the reference checksum can be easily modified as well.
        /// And we don't involve asymmetric encryption with PKI yet.
        /// It's only intended to protect from faulty hardware.
        /// Note: it is only based on machine code.
        /// But there are other sections of the binary (e.g. exception handling tables)
        /// that are interpreted (not executed) but can alter the behaviour of the program as well.

        /// Please keep the below log messages in-sync with the ones in daemon/BaseDaemon.cpp

        String calculated_binary_hash = getHashOfLoadedBinaryHex();

        if (stored_binary_hash.empty())
        {
            LOG_WARNING(log, "Integrity check of the executable skipped because the reference checksum could not be read."
                " (calculated checksum: {})", calculated_binary_hash);
        }
        else if (calculated_binary_hash == stored_binary_hash)
        {
            LOG_INFO(log, "Integrity check of the executable successfully passed (checksum: {})", calculated_binary_hash);
        }
        else
        {
            /// If program is run under debugger, ptrace will fail.
            if (ptrace(PTRACE_TRACEME, 0, nullptr, nullptr) == -1)
            {
                /// Program is run under debugger. Modification of it's binary image is ok for breakpoints.
                global_context->addWarningMessage(
                    fmt::format("Server is run under debugger and its binary image is modified (most likely with breakpoints).",
                    calculated_binary_hash)
                );
            }
            else
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Calculated checksum of the executable ({0}) does not correspond"
                    " to the reference checksum stored in the executable ({1})."
                    " This may indicate one of the following:"
                    " - the executable {2} was changed just after startup;"
                    " - the executable {2} was corrupted on disk due to faulty hardware;"
                    " - the loaded executable was corrupted in memory due to faulty hardware;"
                    " - the file {2} was intentionally modified;"
                    " - a logical error in the code."
                    , calculated_binary_hash, stored_binary_hash, executable_path);
            }
        }
    }
    else
        executable_path = "/usr/bin/clickhouse";    /// It is used for information messages.

    /// After full config loaded
    {
        if (config().getBool("remap_executable", false))
        {
            LOG_DEBUG(log, "Will remap executable in memory.");
            size_t size = remapExecutable();
            LOG_DEBUG(log, "The code ({}) in memory has been successfully remapped.", ReadableSize(size));
        }

        if (config().getBool("mlock_executable", false))
        {
            if (hasLinuxCapability(CAP_IPC_LOCK))
            {
                try
                {
                    /// Get the memory area with (current) code segment.
                    /// It's better to lock only the code segment instead of calling "mlockall",
                    /// because otherwise debug info will be also locked in memory, and it can be huge.
                    auto [addr, len] = getMappedArea(reinterpret_cast<void *>(mainEntryClickHouseServer));

                    LOG_TRACE(log, "Will do mlock to prevent executable memory from being paged out. It may take a few seconds.");
                    if (0 != mlock(addr, len))
                        LOG_WARNING(log, "Failed mlock: {}", errnoToString(ErrorCodes::SYSTEM_ERROR));
                    else
                        LOG_TRACE(log, "The memory map of clickhouse executable has been mlock'ed, total {}", ReadableSize(len));
                }
                catch (...)
                {
                    LOG_WARNING(log, "Cannot mlock: {}", getCurrentExceptionMessage(false));
                }
            }
            else
            {
                LOG_INFO(log, "It looks like the process has no CAP_IPC_LOCK capability, binary mlock will be disabled."
                    " It could happen due to incorrect ClickHouse package installation."
                    " You could resolve the problem manually with 'sudo setcap cap_ipc_lock=+ep {}'."
                    " Note that it will not work on 'nosuid' mounted filesystems.", executable_path);
            }
        }
    }
#endif

    global_context->setRemoteHostFilter(config());

    std::string path_str = getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH));
    fs::path path = path_str;
    std::string default_database = config().getString("default_database", "default");

    /// Check that the process user id matches the owner of the data.
    const auto effective_user_id = geteuid();
    struct stat statbuf;
    if (stat(path_str.c_str(), &statbuf) == 0 && effective_user_id != statbuf.st_uid)
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
            global_context->addWarningMessage(message);
        }
    }

    global_context->setPath(path_str);

    StatusFile status{path / "status", StatusFile::write_full_info};

    DB::ServerUUID::load(path / "uuid", log);

    /// Try to increase limit on number of open files.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of file descriptors is {}", rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = config().getUInt("max_open_files", rlim.rlim_max);
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                LOG_WARNING(log, "Cannot set max number of file descriptors to {}. Try to specify max_open_files according to your system limits. error: {}", rlim.rlim_cur, strerror(errno));
            else
                LOG_DEBUG(log, "Set max number of file descriptors to {} (was {}).", rlim.rlim_cur, old);
        }
    }

    /// Try to increase limit on number of threads.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NPROC, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of threads is {}", rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = rlim.rlim_max;
            int rc = setrlimit(RLIMIT_NPROC, &rlim);
            if (rc != 0)
            {
                LOG_WARNING(log, "Cannot set max number of threads to {}. error: {}", rlim.rlim_cur, strerror(errno));
                rlim.rlim_cur = old;
            }
            else
            {
                LOG_DEBUG(log, "Set max number of threads to {} (was {}).", rlim.rlim_cur, old);
            }
        }

        if (rlim.rlim_cur < 30000)
        {
            global_context->addWarningMessage("Maximum number of threads is lower than 30000. There could be problems with handling a lot of simultaneous queries.");
        }
    }

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", DateLUT::instance().getTimeZone());

    /// Storage with temporary data for processing of heavy queries.
    {
        std::string tmp_path = config().getString("tmp_path", path / "tmp/");
        std::string tmp_policy = config().getString("tmp_policy", "");
        const VolumePtr & volume = global_context->setTemporaryStorage(tmp_path, tmp_policy);
        for (const DiskPtr & disk : volume->getDisks())
            setupTmpPath(log, disk->getPath());
    }

    /** Directory with 'flags': files indicating temporary settings for the server set by system administrator.
      * Flags may be cleared automatically after being applied by the server.
      * Examples: do repair of local data; clone all replicated tables from replica.
      */
    {
        auto flags_path = path / "flags/";
        fs::create_directories(flags_path);
        global_context->setFlagsPath(flags_path);
    }

    /** Directory with user provided files that are usable by 'file' table function.
      */
    {

        std::string user_files_path = config().getString("user_files_path", path / "user_files/");
        global_context->setUserFilesPath(user_files_path);
        fs::create_directories(user_files_path);
    }

    {
        std::string dictionaries_lib_path = config().getString("dictionaries_lib_path", path / "dictionaries_lib/");
        global_context->setDictionariesLibPath(dictionaries_lib_path);
        fs::create_directories(dictionaries_lib_path);
    }

    {
        std::string user_scripts_path = config().getString("user_scripts_path", path / "user_scripts/");
        global_context->setUserScriptsPath(user_scripts_path);
        fs::create_directories(user_scripts_path);
    }

    {
        std::string user_defined_path = config().getString("user_defined_path", path / "user_defined/");
        global_context->setUserDefinedPath(user_defined_path);
        fs::create_directories(user_defined_path);
    }

    /// top_level_domains_lists
    {
        const std::string & top_level_domains_path = config().getString("top_level_domains_path", path / "top_level_domains/");
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", config());
    }

    {
        fs::create_directories(path / "data/");
        fs::create_directories(path / "metadata/");

        /// Directory with metadata of tables, which was marked as dropped by Atomic database
        fs::create_directories(path / "metadata_dropped/");
    }

#if USE_ROCKSDB
    /// Initialize merge tree metadata cache
    if (config().has("merge_tree_metadata_cache"))
    {
        fs::create_directories(path / "rocksdb/");
        size_t size = config().getUInt64("merge_tree_metadata_cache.lru_cache_size", 256 << 20);
        bool continue_if_corrupted = config().getBool("merge_tree_metadata_cache.continue_if_corrupted", false);
        try
        {
            LOG_DEBUG(
                log, "Initiailizing merge tree metadata cache lru_cache_size:{} continue_if_corrupted:{}", size, continue_if_corrupted);
            global_context->initializeMergeTreeMetadataCache(path_str + "/" + "rocksdb", size);
        }
        catch (...)
        {
            if (continue_if_corrupted)
            {
                /// Rename rocksdb directory and reinitialize merge tree metadata cache
                time_t now = time(nullptr);
                fs::rename(path / "rocksdb", path / ("rocksdb.old." + std::to_string(now)));
                global_context->initializeMergeTreeMetadataCache(path_str + "/" + "rocksdb", size);
            }
            else
            {
                throw;
            }
        }
    }
#endif

    if (config().has("interserver_http_port") && config().has("interserver_https_port"))
        throw Exception("Both http and https interserver ports are specified", ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG);

    static const auto interserver_tags =
    {
        std::make_tuple("interserver_http_host", "interserver_http_port", "http"),
        std::make_tuple("interserver_https_host", "interserver_https_port", "https")
    };

    for (auto [host_tag, port_tag, scheme] : interserver_tags)
    {
        if (config().has(port_tag))
        {
            String this_host = config().getString(host_tag, "");

            if (this_host.empty())
            {
                this_host = getFQDNOrHostName();
                LOG_DEBUG(log, "Configuration parameter '{}' doesn't exist or exists and empty. Will use '{}' as replica host.",
                    host_tag, this_host);
            }

            String port_str = config().getString(port_tag);
            int port = parse<int>(port_str);

            if (port < 0 || port > 0xFFFF)
                throw Exception("Out of range '" + String(port_tag) + "': " + toString(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            global_context->setInterserverIOAddress(this_host, port);
            global_context->setInterserverScheme(scheme);
        }
    }

    LOG_DEBUG(log, "Initiailizing interserver credentials.");
    global_context->updateInterserverCredentials(config());

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    /// Initialize main config reloader.
    std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");

    if (config().has("query_masking_rules"))
    {
        SensitiveDataMasker::setInstance(std::make_unique<SensitiveDataMasker>(config(), "query_masking_rules"));
    }

    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        include_from_path,
        config().getString("path", ""),
        std::move(main_config_zk_node_cache),
        main_config_zk_changed_event,
        [&](ConfigurationPtr config, bool initial_loading)
        {
            Settings::checkNoSettingNamesAtTopLevel(*config, config_path);

            /// Limit on total memory usage
            size_t max_server_memory_usage = config->getUInt64("max_server_memory_usage", 0);

            double max_server_memory_usage_to_ram_ratio = config->getDouble("max_server_memory_usage_to_ram_ratio", 0.9);
            size_t default_max_server_memory_usage = memory_amount * max_server_memory_usage_to_ram_ratio;

            if (max_server_memory_usage == 0)
            {
                max_server_memory_usage = default_max_server_memory_usage;
                LOG_INFO(log, "Setting max_server_memory_usage was set to {}"
                    " ({} available * {:.2f} max_server_memory_usage_to_ram_ratio)",
                    formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                    formatReadableSizeWithBinarySuffix(memory_amount),
                    max_server_memory_usage_to_ram_ratio);
            }
            else if (max_server_memory_usage > default_max_server_memory_usage)
            {
                max_server_memory_usage = default_max_server_memory_usage;
                LOG_INFO(log, "Setting max_server_memory_usage was lowered to {}"
                    " because the system has low amount of memory. The amount was"
                    " calculated as {} available"
                    " * {:.2f} max_server_memory_usage_to_ram_ratio",
                    formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                    formatReadableSizeWithBinarySuffix(memory_amount),
                    max_server_memory_usage_to_ram_ratio);
            }

            total_memory_tracker.setHardLimit(max_server_memory_usage);
            total_memory_tracker.setDescription("(total)");
            total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);

            auto * global_overcommit_tracker = global_context->getGlobalOvercommitTracker();
            total_memory_tracker.setOvercommitTracker(global_overcommit_tracker);

            // FIXME logging-related things need synchronization -- see the 'Logger * log' saved
            // in a lot of places. For now, disable updating log configuration without server restart.
            //setTextLog(global_context->getTextLog());
            updateLevels(*config, logger());
            global_context->setClustersConfig(config, has_zookeeper);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros", log));
            global_context->setExternalAuthenticatorsConfig(*config);

            global_context->loadOrReloadDictionaries(*config);
            global_context->loadOrReloadModels(*config);
            global_context->loadOrReloadUserDefinedExecutableFunctions(*config);

            global_context->setRemoteHostFilter(*config);

            /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
            if (config->has("max_table_size_to_drop"))
                global_context->setMaxTableSizeToDrop(config->getUInt64("max_table_size_to_drop"));

            if (config->has("max_partition_size_to_drop"))
                global_context->setMaxPartitionSizeToDrop(config->getUInt64("max_partition_size_to_drop"));

            if (config->has("concurrent_threads_soft_limit"))
            {
                auto concurrent_threads_soft_limit = config->getInt("concurrent_threads_soft_limit", 0);
                if (concurrent_threads_soft_limit == -1)
                {
                    // Based on tests concurrent_threads_soft_limit has an optimal value when it's about 3 times of logical CPU cores
                    constexpr size_t thread_factor = 3;
                    concurrent_threads_soft_limit = std::thread::hardware_concurrency() * thread_factor;
                }
                if (concurrent_threads_soft_limit)
                    ConcurrencyControl::instance().setMaxConcurrency(concurrent_threads_soft_limit);
                else
                    ConcurrencyControl::instance().setMaxConcurrency(ConcurrencyControl::Unlimited);
            }
            else
                ConcurrencyControl::instance().setMaxConcurrency(ConcurrencyControl::Unlimited);

            if (config->has("max_concurrent_queries"))
                global_context->getProcessList().setMaxSize(config->getInt("max_concurrent_queries", 0));

            if (config->has("max_concurrent_insert_queries"))
                global_context->getProcessList().setMaxInsertQueriesAmount(config->getInt("max_concurrent_insert_queries", 0));

            if (config->has("max_concurrent_select_queries"))
                global_context->getProcessList().setMaxSelectQueriesAmount(config->getInt("max_concurrent_select_queries", 0));

            if (config->has("keeper_server"))
                global_context->updateKeeperConfiguration(*config);

            /// Reload the number of threads for global pools.
            /// Note: If you specified it in the top level config (not it config of default profile)
            /// then ClickHouse will use it exactly.
            /// This is done for backward compatibility.
            if (global_context->areBackgroundExecutorsInitialized() && (config->has("background_pool_size") || config->has("background_merges_mutations_concurrency_ratio")))
            {
                auto new_pool_size = config->getUInt64("background_pool_size", 16);
                auto new_ratio = config->getUInt64("background_merges_mutations_concurrency_ratio", 2);
                global_context->getMergeMutateExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size * new_ratio);
            }

            if (global_context->areBackgroundExecutorsInitialized() && config->has("background_move_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_move_pool_size");
                global_context->getMovesExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size);
            }

            if (global_context->areBackgroundExecutorsInitialized() && config->has("background_fetches_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_fetches_pool_size");
                global_context->getFetchesExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size);
            }

            if (global_context->areBackgroundExecutorsInitialized() && config->has("background_common_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_common_pool_size");
                global_context->getCommonExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size);
            }

            if (config->has("background_buffer_flush_schedule_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_buffer_flush_schedule_pool_size");
                global_context->getBufferFlushSchedulePool().increaseThreadsCount(new_pool_size);
            }

            if (config->has("background_schedule_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_schedule_pool_size");
                global_context->getSchedulePool().increaseThreadsCount(new_pool_size);
            }

            if (config->has("background_message_broker_schedule_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_message_broker_schedule_pool_size");
                global_context->getMessageBrokerSchedulePool().increaseThreadsCount(new_pool_size);
            }

            if (config->has("background_distributed_schedule_pool_size"))
            {
                auto new_pool_size = config->getUInt64("background_distributed_schedule_pool_size");
                global_context->getDistributedSchedulePool().increaseThreadsCount(new_pool_size);
            }

            if (!initial_loading)
            {
                /// We do not load ZooKeeper configuration on the first config loading
                /// because TestKeeper server is not started yet.
                if (config->has("zookeeper"))
                    global_context->reloadZooKeeperIfChanged(config);

                global_context->reloadAuxiliaryZooKeepersConfigIfChanged(config);

                std::lock_guard lock(servers_lock);
                updateServers(*config, server_pool, async_metrics, servers);
            }

            global_context->updateStorageConfiguration(*config);
            global_context->updateInterserverCredentials(*config);

            CompressionCodecEncrypted::Configuration::instance().tryLoad(*config, "encryption_codecs");
#if USE_SSL
            CertificateReloader::instance().tryLoad(*config);
#endif
            ProfileEvents::increment(ProfileEvents::MainConfigLoads);
        },
        /* already_loaded = */ false);  /// Reload it right now (initial loading)

    const auto listen_hosts = getListenHosts(config());
    const auto interserver_listen_hosts = getInterserverListenHosts(config());
    const auto listen_try = getListenTry(config());

    if (config().has("keeper_server"))
    {
#if USE_NURAFT
        //// If we don't have configured connection probably someone trying to use clickhouse-server instead
        //// of clickhouse-keeper, so start synchronously.
        bool can_initialize_keeper_async = false;

        if (has_zookeeper) /// We have configured connection to some zookeeper cluster
        {
            /// If we cannot connect to some other node from our cluster then we have to wait our Keeper start
            /// synchronously.
            can_initialize_keeper_async = global_context->tryCheckClientConnectionToMyKeeperCluster();
        }
        /// Initialize keeper RAFT.
        global_context->initializeKeeperDispatcher(can_initialize_keeper_async);
        FourLetterCommandFactory::registerCommands(*global_context->getKeeperDispatcher());

        auto config_getter = [this] () -> const Poco::Util::AbstractConfiguration &
        {
            return global_context->getConfigRef();
        };

        for (const auto & listen_host : listen_hosts)
        {
            /// TCP Keeper
            const char * port_name = "keeper_server.tcp_port";
            createServer(
                config(), listen_host, port_name, listen_try, /* start_server: */ false,
                servers_to_start_before_tables,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config(), socket, listen_host, port);
                    socket.setReceiveTimeout(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC));
                    socket.setSendTimeout(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC));
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "Keeper (tcp): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new KeeperTCPHandlerFactory(
                                config_getter, global_context->getKeeperDispatcher(),
                                global_context->getSettingsRef().receive_timeout.totalSeconds(),
                                global_context->getSettingsRef().send_timeout.totalSeconds(),
                                false), server_pool, socket));
                });

            const char * secure_port_name = "keeper_server.tcp_port_secure";
            createServer(
                config(), listen_host, secure_port_name, listen_try, /* start_server: */ false,
                servers_to_start_before_tables,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
#if USE_SSL
                    Poco::Net::SecureServerSocket socket;
                    auto address = socketBindListen(config(), socket, listen_host, port, /* secure = */ true);
                    socket.setReceiveTimeout(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC));
                    socket.setSendTimeout(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC));
                    return ProtocolServerAdapter(
                        listen_host,
                        secure_port_name,
                        "Keeper with secure protocol (tcp_secure): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new KeeperTCPHandlerFactory(
                                config_getter, global_context->getKeeperDispatcher(),
                                global_context->getSettingsRef().receive_timeout.totalSeconds(),
                                global_context->getSettingsRef().send_timeout.totalSeconds(), true), server_pool, socket));
#else
                    UNUSED(port);
                    throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                });
        }
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse server built without NuRaft library. Cannot use internal coordination.");
#endif

    }

    for (auto & server : servers_to_start_before_tables)
    {
        server.start();
        LOG_INFO(log, "Listening for {}", server.getDescription());
    }

    /// Initialize access storages.
    auto & access_control = global_context->getAccessControl();
    try
    {
        access_control.setUpFromMainConfig(config(), config_path, [&] { return global_context->getZooKeeper(); });
    }
    catch (...)
    {
        tryLogCurrentException(log);
        throw;
    }

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]()
    {
        main_config_reloader->reload();
        access_control.reload();
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

    /// Set up caches.

    /// Lower cache size on low-memory systems.
    double cache_size_to_ram_max_ratio = config().getDouble("cache_size_to_ram_max_ratio", 0.5);
    size_t max_cache_size = memory_amount * cache_size_to_ram_max_ratio;

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
    if (uncompressed_cache_size > max_cache_size)
    {
        uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Uncompressed cache size was lowered to {} because the system has low amount of memory",
            formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setUncompressedCache(uncompressed_cache_size);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());
    const Settings & settings = global_context->getSettingsRef();

    /// Initialize background executors after we load default_profile config.
    /// This is needed to load proper values of background_pool_size etc.
    global_context->initializeBackgroundExecutorsIfNeeded();

    if (settings.async_insert_threads)
        global_context->setAsynchronousInsertQueue(std::make_shared<AsynchronousInsertQueue>(
            global_context,
            settings.async_insert_threads,
            settings.async_insert_max_data_size,
            AsynchronousInsertQueue::Timeout{.busy = settings.async_insert_busy_timeout_ms, .stale = settings.async_insert_stale_timeout_ms}));

    /// Size of cache for marks (index of MergeTree family of tables).
    size_t mark_cache_size = config().getUInt64("mark_cache_size", 5368709120);
    if (!mark_cache_size)
        LOG_ERROR(log, "Too low mark cache size will lead to severe performance degradation.");
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_INFO(log, "Mark cache size was lowered to {} because the system has low amount of memory",
            formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_size);

    /// Size of cache for uncompressed blocks of MergeTree indices. Zero means disabled.
    size_t index_uncompressed_cache_size = config().getUInt64("index_uncompressed_cache_size", 0);
    if (index_uncompressed_cache_size)
        global_context->setIndexUncompressedCache(index_uncompressed_cache_size);

    /// Size of cache for index marks (index of MergeTree skip indices).
    size_t index_mark_cache_size = config().getUInt64("index_mark_cache_size", 0);
    if (index_mark_cache_size)
        global_context->setIndexMarkCache(index_mark_cache_size);

    /// A cache for mmapped files.
    size_t mmap_cache_size = config().getUInt64("mmap_cache_size", 1000);   /// The choice of default is arbitrary.
    if (mmap_cache_size)
        global_context->setMMappedFileCache(mmap_cache_size);

#if USE_EMBEDDED_COMPILER
    /// 128 MB
    constexpr size_t compiled_expression_cache_size_default = 1024 * 1024 * 128;
    size_t compiled_expression_cache_size = config().getUInt64("compiled_expression_cache_size", compiled_expression_cache_size_default);

    constexpr size_t compiled_expression_cache_elements_size_default = 10000;
    size_t compiled_expression_cache_elements_size = config().getUInt64("compiled_expression_cache_elements_size", compiled_expression_cache_elements_size_default);

    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_size, compiled_expression_cache_elements_size);
#endif

    /// Set path for format schema files
    fs::path format_schema_path(config().getString("format_schema_path", path / "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path);
    fs::create_directories(format_schema_path);

    /// Check sanity of MergeTreeSettings on server startup
    {
        size_t background_pool_tasks = global_context->getMergeMutateExecutor()->getMaxTasksCount();
        global_context->getMergeTreeSettings().sanityCheck(background_pool_tasks);
        global_context->getReplicatedMergeTreeSettings().sanityCheck(background_pool_tasks);
    }

    /// try set up encryption. There are some errors in config, error will be printed and server wouldn't start.
    CompressionCodecEncrypted::Configuration::instance().load(config(), "encryption_codecs");

    SCOPE_EXIT({
        /// Stop reloading of the main config. This must be done before `global_context->shutdown()` because
        /// otherwise the reloading may pass a changed config to some destroyed parts of ContextSharedPart.
        main_config_reloader.reset();
        access_control.stopPeriodicReloading();

        async_metrics.stop();

        /** Ask to cancel background jobs all table engines,
          *  and also query_log.
          * It is important to do early, not in destructor of Context, because
          *  table engines could use Context on destroy.
          */
        LOG_INFO(log, "Shutting down storages.");

        global_context->shutdown();

        LOG_DEBUG(log, "Shut down storages.");

        if (!servers_to_start_before_tables.empty())
        {
            LOG_DEBUG(log, "Waiting for current connections to servers for tables to finish.");
            int current_connections = 0;
            for (auto & server : servers_to_start_before_tables)
            {
                server.stop();
                current_connections += server.currentConnections();
            }

            if (current_connections)
                LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            if (current_connections > 0)
                current_connections = waitServersToFinish(servers_to_start_before_tables, config().getInt("shutdown_wait_unfinished", 5));

            if (current_connections)
                LOG_INFO(log, "Closed connections to servers for tables. But {} remain. Probably some tables of other users cannot finish their connections after context shutdown.", current_connections);
            else
                LOG_INFO(log, "Closed connections to servers for tables.");

            global_context->shutdownKeeperDispatcher();
        }

        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();
        shared_context.reset();
        LOG_DEBUG(log, "Destroyed global context.");
    });

    /// Set current database name before loading tables and databases because
    /// system logs may copy global context.
    global_context->setCurrentDatabaseNameInGlobalContext(default_database);

    LOG_INFO(log, "Loading user defined objects from {}", path_str);
    try
    {
        UserDefinedSQLObjectsLoader::instance().loadObjects(global_context);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception while loading user defined objects");
        throw;
    }
    LOG_DEBUG(log, "Loaded user defined objects");

    LOG_INFO(log, "Loading metadata from {}", path_str);

    try
    {
        auto & database_catalog = DatabaseCatalog::instance();
        /// We load temporary database first, because projections need it.
        database_catalog.initializeAndLoadTemporaryDatabase();
        loadMetadataSystem(global_context);
        maybeConvertSystemDatabase(global_context);
        /// After attaching system databases we can initialize system log.
        global_context->initializeSystemLogs();
        global_context->setSystemZooKeeperLogAfterInitializationIfNeeded();
        /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
        attachSystemTablesServer(global_context, *database_catalog.getSystemDatabase(), has_zookeeper);
        attachInformationSchema(global_context, *database_catalog.getDatabase(DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *database_catalog.getDatabase(DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        /// Firstly remove partially dropped databases, to avoid race with MaterializedMySQLSyncThread,
        /// that may execute DROP before loadMarkedAsDroppedTables() in background,
        /// and so loadMarkedAsDroppedTables() will find it and try to add, and UUID will overlap.
        database_catalog.loadMarkedAsDroppedTables();
        /// Then, load remaining databases
        loadMetadata(global_context, default_database);
        convertDatabasesEnginesIfNeed(global_context);
        startupSystemTables();
        database_catalog.loadDatabases();
        /// After loading validate that default database exists
        database_catalog.assertDatabaseExists(default_database);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception while loading metadata");
        throw;
    }
    LOG_DEBUG(log, "Loaded metadata.");

    /// Init trace collector only after trace_log system table was created
    /// Disable it if we collect test coverage information, because it will work extremely slow.
#if USE_UNWIND && !WITH_COVERAGE
    /// Profilers cannot work reliably with any other libunwind or without PHDR cache.
    if (hasPHDRCache())
    {
        global_context->initializeTraceCollector();

        /// Set up server-wide memory profiler (for total memory tracker).
        UInt64 total_memory_profiler_step = config().getUInt64("total_memory_profiler_step", 0);
        if (total_memory_profiler_step)
        {
            total_memory_tracker.setProfilerStep(total_memory_profiler_step);
        }

        double total_memory_tracker_sample_probability = config().getDouble("total_memory_tracker_sample_probability", 0);
        if (total_memory_tracker_sample_probability)
        {
            total_memory_tracker.setSampleProbability(total_memory_tracker_sample_probability);
        }
    }
#endif

    /// Describe multiple reasons when query profiler cannot work.

#if !USE_UNWIND
    LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they cannot work without bundled unwind (stack unwinding) library.");
#endif

#if WITH_COVERAGE
    LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they work extremely slow with test coverage.");
#endif

#if defined(SANITIZER)
    LOG_INFO(log, "Query Profiler disabled because they cannot work under sanitizers"
        " when two different stack unwinding methods will interfere with each other.");
#endif

#if !defined(__x86_64__)
    LOG_INFO(log, "Query Profiler and TraceCollector is only tested on x86_64. It also known to not work under qemu-user.");
#endif

    if (!hasPHDRCache())
        LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they require PHDR cache to be created"
            " (otherwise the function 'dl_iterate_phdr' is not lock free and not async-signal safe).");

    std::unique_ptr<DNSCacheUpdater> dns_cache_updater;
    if (config().has("disable_internal_dns_cache") && config().getInt("disable_internal_dns_cache"))
    {
        /// Disable DNS caching at all
        DNSResolver::instance().setDisableCacheFlag();
        LOG_DEBUG(log, "DNS caching disabled");
    }
    else
    {
        /// Initialize a watcher periodically updating DNS cache
        dns_cache_updater = std::make_unique<DNSCacheUpdater>(
            global_context, config().getInt("dns_cache_update_period", 15), config().getUInt("dns_max_consecutive_failures", 5));
    }

#if defined(OS_LINUX)
    auto tasks_stats_provider = TasksStatsCounters::findBestAvailableProvider();
    if (tasks_stats_provider == TasksStatsCounters::MetricsProvider::None)
    {
        LOG_INFO(log, "It looks like this system does not have procfs mounted at /proc location,"
            " neither clickhouse-server process has CAP_NET_ADMIN capability."
            " 'taskstats' performance statistics will be disabled."
            " It could happen due to incorrect ClickHouse package installation."
            " You can try to resolve the problem manually with 'sudo setcap cap_net_admin=+ep {}'."
            " Note that it will not work on 'nosuid' mounted filesystems."
            " It also doesn't work if you run clickhouse-server inside network namespace as it happens in some containers.",
            executable_path);
    }
    else
    {
        LOG_INFO(log, "Tasks stats provider: {}", TasksStatsCounters::metricsProviderString(tasks_stats_provider));
    }

    if (!hasLinuxCapability(CAP_SYS_NICE))
    {
        LOG_INFO(log, "It looks like the process has no CAP_SYS_NICE capability, the setting 'os_thread_priority' will have no effect."
            " It could happen due to incorrect ClickHouse package installation."
            " You could resolve the problem manually with 'sudo setcap cap_sys_nice=+ep {}'."
            " Note that it will not work on 'nosuid' mounted filesystems.",
            executable_path);
    }
#else
    LOG_INFO(log, "TaskStats is not implemented for this OS. IO accounting will be disabled.");
#endif

    {
        attachSystemTablesAsync(global_context, *DatabaseCatalog::instance().getSystemDatabase(), async_metrics);

        {
            std::lock_guard lock(servers_lock);
            createServers(config(), listen_hosts, interserver_listen_hosts, listen_try, server_pool, async_metrics, servers);
            if (servers.empty())
                throw Exception(
                    "No servers started (add valid listen_host and 'tcp_port' or 'http_port' to configuration file.)",
                    ErrorCodes::NO_ELEMENTS_IN_CONFIG);
        }

        if (servers.empty())
             throw Exception("No servers started (add valid listen_host and 'tcp_port' or 'http_port' to configuration file.)",
                ErrorCodes::NO_ELEMENTS_IN_CONFIG);

#if USE_SSL
        CertificateReloader::instance().tryLoad(config());
#endif

        /// Must be done after initialization of `servers`, because async_metrics will access `servers` variable from its thread.

        async_metrics.start();

        {
            String level_str = config().getString("text_log.level", "");
            int level = level_str.empty() ? INT_MAX : Poco::Logger::parseLevel(level_str);
            setTextLog(global_context->getTextLog(), level);
        }

        buildLoggers(config(), logger());

        main_config_reloader->start();
        access_control.startPeriodicReloading();
        if (dns_cache_updater)
            dns_cache_updater->start();

        {
            LOG_INFO(log, "Available RAM: {}; physical cores: {}; logical cores: {}.",
                formatReadableSizeWithBinarySuffix(memory_amount),
                getNumberOfPhysicalCPUCores(),  // on ARM processors it can show only enabled at current moment cores
                std::thread::hardware_concurrency());
        }

        /// try to load dictionaries immediately, throw on error and die
        try
        {
            global_context->loadOrReloadDictionaries(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading dictionaries.");
            throw;
        }

        /// try to load embedded dictionaries immediately, throw on error and die
        try
        {
            global_context->tryCreateEmbeddedDictionaries(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading embedded dictionaries.");
            throw;
        }

        /// try to load models immediately, throw on error and die
        try
        {
            global_context->loadOrReloadModels(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading dictionaries.");
            throw;
        }

        /// try to load user defined executable functions, throw on error and die
        try
        {
            global_context->loadOrReloadUserDefinedExecutableFunctions(config());
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading user defined executable functions.");
            throw;
        }

        if (has_zookeeper && config().has("distributed_ddl"))
        {
            /// DDL worker should be started after all tables were loaded
            String ddl_zookeeper_path = config().getString("distributed_ddl.path", "/clickhouse/task_queue/ddl/");
            int pool_size = config().getInt("distributed_ddl.pool_size", 1);
            if (pool_size < 1)
                throw Exception("distributed_ddl.pool_size should be greater then 0", ErrorCodes::ARGUMENT_OUT_OF_BOUND);
            global_context->setDDLWorker(std::make_unique<DDLWorker>(pool_size, ddl_zookeeper_path, global_context, &config(),
                                                                     "distributed_ddl", "DDLWorker",
                                                                     &CurrentMetrics::MaxDDLEntryID, &CurrentMetrics::MaxPushedDDLEntryID));
        }

        {
            std::lock_guard lock(servers_lock);
            for (auto & server : servers)
            {
                server.start();
                LOG_INFO(log, "Listening for {}", server.getDescription());
            }

            global_context->setServerCompletelyStarted();
            LOG_INFO(log, "Ready for connections.");
        }

        try
        {
            global_context->startClusterDiscovery();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while starting cluster discovery");
        }

        SCOPE_EXIT_SAFE({
            LOG_DEBUG(log, "Received termination signal.");
            LOG_DEBUG(log, "Waiting for current connections to close.");

            is_cancelled = true;

            int current_connections = 0;
            {
                std::lock_guard lock(servers_lock);
                for (auto & server : servers)
                {
                    server.stop();
                    current_connections += server.currentConnections();
                }
            }

            if (current_connections)
                LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            /// Killing remaining queries.
            if (!config().getBool("shutdown_wait_unfinished_queries", false))
                global_context->getProcessList().killAllQueries();

            if (current_connections)
                current_connections = waitServersToFinish(servers, config().getInt("shutdown_wait_unfinished", 5));

            if (current_connections)
                LOG_INFO(log, "Closed connections. But {} remain."
                    " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>", current_connections);
            else
                LOG_INFO(log, "Closed connections.");

            dns_cache_updater.reset();

            if (current_connections)
            {
                /// There is no better way to force connections to close in Poco.
                /// Otherwise connection handlers will continue to live
                /// (they are effectively dangling objects, but they use global thread pool
                ///  and global thread pool destructor will wait for threads, preventing server shutdown).

                /// Dump coverage here, because std::atexit callback would not be called.
                dumpCoverageReportIfPossible();
                LOG_INFO(log, "Will shutdown forcefully.");
                safeExit(0);
            }
        });

        std::vector<std::unique_ptr<MetricsTransmitter>> metrics_transmitters;
        for (const auto & graphite_key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
        {
            metrics_transmitters.emplace_back(std::make_unique<MetricsTransmitter>(
                global_context->getConfigRef(), graphite_key, async_metrics));
        }

        waitForTerminationRequest();
    }

    return Application::EXIT_OK;
}


void Server::createServers(
    Poco::Util::AbstractConfiguration & config,
    const Strings & listen_hosts,
    const Strings & interserver_listen_hosts,
    bool listen_try,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    std::vector<ProtocolServerAdapter> & servers,
    bool start_servers)
{
    const Settings & settings = global_context->getSettingsRef();

    Poco::Timespan keep_alive_timeout(config.getUInt("keep_alive_timeout", 10), 0);
    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings.http_receive_timeout);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    for (const auto & listen_host : listen_hosts)
    {
        /// HTTP
        const char * port_name = "http_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port);
            socket.setReceiveTimeout(settings.http_receive_timeout);
            socket.setSendTimeout(settings.http_send_timeout);

            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "http://" + address.toString(),
                std::make_unique<HTTPServer>(
                    context(), createHandlerFactory(*this, async_metrics, "HTTPHandler-factory"), server_pool, socket, http_params));
        });

        /// HTTPS
        port_name = "https_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
#if USE_SSL
            Poco::Net::SecureServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(settings.http_receive_timeout);
            socket.setSendTimeout(settings.http_send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "https://" + address.toString(),
                std::make_unique<HTTPServer>(
                    context(), createHandlerFactory(*this, async_metrics, "HTTPSHandler-factory"), server_pool, socket, http_params));
#else
            UNUSED(port);
            throw Exception{"HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        });

        /// TCP
        port_name = "tcp_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port);
            socket.setReceiveTimeout(settings.receive_timeout);
            socket.setSendTimeout(settings.send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "native protocol (tcp): " + address.toString(),
                std::make_unique<TCPServer>(
                    new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ false),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));
        });

        /// TCP with PROXY protocol, see https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
        port_name = "tcp_with_proxy_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port);
            socket.setReceiveTimeout(settings.receive_timeout);
            socket.setSendTimeout(settings.send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "native protocol (tcp) with PROXY: " + address.toString(),
                std::make_unique<TCPServer>(
                    new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ true),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));
        });

        /// TCP with SSL
        port_name = "tcp_port_secure";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
#if USE_SSL
            Poco::Net::SecureServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(settings.receive_timeout);
            socket.setSendTimeout(settings.send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "secure native protocol (tcp_secure): " + address.toString(),
                std::make_unique<TCPServer>(
                    new TCPHandlerFactory(*this, /* secure */ true, /* proxy protocol */ false),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));
#else
            UNUSED(port);
            throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        });

        port_name = "mysql_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(Poco::Timespan());
            socket.setSendTimeout(settings.send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "MySQL compatibility protocol: " + address.toString(),
                std::make_unique<TCPServer>(new MySQLHandlerFactory(*this), server_pool, socket, new Poco::Net::TCPServerParams));
        });

        port_name = "postgresql_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(Poco::Timespan());
            socket.setSendTimeout(settings.send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "PostgreSQL compatibility protocol: " + address.toString(),
                std::make_unique<TCPServer>(new PostgreSQLHandlerFactory(*this), server_pool, socket, new Poco::Net::TCPServerParams));
        });

#if USE_GRPC
        port_name = "grpc_port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::SocketAddress server_address(listen_host, port);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "gRPC protocol: " + server_address.toString(),
                std::make_unique<GRPCServer>(*this, makeSocketAddress(listen_host, port, &logger())));
        });
#endif

        /// Prometheus (if defined and not setup yet with http_port)
        port_name = "prometheus.port";
        createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, listen_host, port);
            socket.setReceiveTimeout(settings.http_receive_timeout);
            socket.setSendTimeout(settings.http_send_timeout);
            return ProtocolServerAdapter(
                listen_host,
                port_name,
                "Prometheus: http://" + address.toString(),
                std::make_unique<HTTPServer>(
                    context(), createHandlerFactory(*this, async_metrics, "PrometheusHandler-factory"), server_pool, socket, http_params));
        });
    }

    /// Now iterate over interserver_listen_hosts
    for (const auto & interserver_listen_host : interserver_listen_hosts)
    {
         /// Interserver IO HTTP
        const char * port_name = "interserver_http_port";
        createServer(config, interserver_listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(config, socket, interserver_listen_host, port);
            socket.setReceiveTimeout(settings.http_receive_timeout);
            socket.setSendTimeout(settings.http_send_timeout);
            return ProtocolServerAdapter(
                interserver_listen_host,
                port_name,
                "replica communication (interserver): http://" + address.toString(),
                std::make_unique<HTTPServer>(
                    context(),
                    createHandlerFactory(*this, async_metrics, "InterserverIOHTTPHandler-factory"),
                    server_pool,
                    socket,
                    http_params));
        });

        port_name = "interserver_https_port";
        createServer(config, interserver_listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
        {
#if USE_SSL
            Poco::Net::SecureServerSocket socket;
            auto address = socketBindListen(config, socket, interserver_listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(settings.http_receive_timeout);
            socket.setSendTimeout(settings.http_send_timeout);
            return ProtocolServerAdapter(
                interserver_listen_host,
                port_name,
                "secure replica communication (interserver): https://" + address.toString(),
                std::make_unique<HTTPServer>(
                    context(),
                    createHandlerFactory(*this, async_metrics, "InterserverIOHTTPSHandler-factory"),
                    server_pool,
                    socket,
                    http_params));
#else
            UNUSED(port);
            throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
        });
    }

}

void Server::updateServers(
    Poco::Util::AbstractConfiguration & config,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    std::vector<ProtocolServerAdapter> & servers)
{
    Poco::Logger * log = &logger();

    const auto listen_hosts = getListenHosts(config);
    const auto interserver_listen_hosts = getInterserverListenHosts(config);
    const auto listen_try = getListenTry(config);

    /// Remove servers once all their connections are closed
    auto check_server = [&log](const char prefix[], auto & server)
    {
        if (!server.isStopping())
            return false;
        size_t current_connections = server.currentConnections();
        LOG_DEBUG(log, "Server {}{}: {} ({} connections)",
            server.getDescription(),
            prefix,
            !current_connections ? "finished" : "waiting",
            current_connections);
        return !current_connections;
    };

    std::erase_if(servers, std::bind_front(check_server, " (from one of previous reload)"));

    for (auto & server : servers)
    {
        if (!server.isStopping())
        {
            bool has_host = std::find(listen_hosts.begin(), listen_hosts.end(), server.getListenHost()) != listen_hosts.end();
            bool has_port = !config.getString(server.getPortName(), "").empty();
            if (!has_host || !has_port || config.getInt(server.getPortName()) != server.portNumber())
            {
                server.stop();
                LOG_INFO(log, "Stopped listening for {}", server.getDescription());
            }
        }
    }

    createServers(config, listen_hosts, interserver_listen_hosts, listen_try, server_pool, async_metrics, servers, /* start_servers= */ true);

    std::erase_if(servers, std::bind_front(check_server, ""));
}

}
