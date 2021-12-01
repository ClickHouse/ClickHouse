#include "Server.h"

#include <memory>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <errno.h>
#include <pwd.h>
#include <unistd.h>
#include <Poco/Version.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Environment.h>
#include <common/scope_guard.h>
#include <common/defines.h>
#include <common/logger_useful.h>
#include <common/phdr_cache.h>
#include <common/ErrorHandlers.h>
#include <common/getMemoryAmount.h>
#include <common/errnoToString.h>
#include <common/coverage.h>
#include <Common/ClickHouseRevision.h>
#include <Common/DNSResolver.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/getExecutablePath.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/getMappedArea.h>
#include <Common/remapExecutable.h>
#include <Common/TLDListsHolder.h>
#include <IO/HTTPCommon.h>
#include <IO/UseSSL.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ExternalModelsLoader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Access/AccessControlManager.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/attachSystemTables.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Formats/registerFormats.h>
#include <Storages/registerStorages.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Common/Config/ConfigReloader.h>
#include <Server/HTTPHandlerFactory.h>
#include "MetricsTransmitter.h"
#include <Common/StatusFile.h>
#include <Server/TCPHandlerFactory.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ThreadFuzzer.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/Elf.h>
#include <Server/MySQLHandlerFactory.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/HTTP/HTTPServer.h>
#include <filesystem>


#if !defined(ARCADIA_BUILD)
#   include "config_core.h"
#   include "Common/config_version.h"
#   if USE_OPENCL
#       include "Common/BitonicSort.h" // Y_IGNORE
#   endif
#endif

#if defined(OS_LINUX)
#    include <sys/mman.h>
#    include <sys/ptrace.h>
#    include <Common/hasLinuxCapability.h>
#    include <unistd.h>
#    include <sys/syscall.h>
#endif

#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SecureServerSocket.h>
#endif

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

#if USE_NURAFT
#   include <Server/KeeperTCPHandlerFactory.h>
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

Poco::Net::SocketAddress Server::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
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

    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(&logger(), "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config().getUInt("listen_backlog", 64));

    return address;
}

void Server::createServer(const std::string & listen_host, const char * port_name, bool listen_try, CreateServerFunc && func) const
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


int Server::main(const std::vector<std::string> & /*args*/)
{
    Poco::Logger * log = &logger();

    UseSSL use_ssl;

    MainThreadStatus::getInstance();

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();
    registerDictionaries();
    registerDisks();
    registerFormats();

    CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::getVersionRevision());
    CurrentMetrics::set(CurrentMetrics::VersionInteger, ClickHouseRevision::getVersionInteger());

    if (ThreadFuzzer::instance().isEffective())
        LOG_WARNING(log, "ThreadFuzzer is enabled. Application will run slowly and unstable.");

#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    LOG_WARNING(log, "Server was built in debug mode. It will work slowly.");
#endif

#if defined(SANITIZER)
    LOG_WARNING(log, "Server was built with sanitizer. It will work slowly.");
#endif

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases, ...
      */
    auto shared_context = Context::createShared();
    global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::SERVER);

    // Initialize global thread pool. Do it before we fetch configs from zookeeper
    // nodes (`from_zk`), because ZooKeeper interface uses the pool. We will
    // ignore `max_thread_pool_size` in configs we fetch from ZK, but oh well.
    GlobalThreadPool::initialize(config().getUInt("max_thread_pool_size", 10000));

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

        String calculated_binary_hash = getHashOfLoadedBinaryHex();

        if (stored_binary_hash.empty())
        {
            LOG_WARNING(log, "Calculated checksum of the binary: {}."
                " There is no information about the reference checksum.", calculated_binary_hash);
        }
        else if (calculated_binary_hash == stored_binary_hash)
        {
            LOG_INFO(log, "Calculated checksum of the binary: {}, integrity check passed.", calculated_binary_hash);
        }
        else
        {
            /// If program is run under debugger, ptrace will fail.
            if (ptrace(PTRACE_TRACEME, 0, nullptr, nullptr) == -1)
            {
                /// Program is run under debugger. Modification of it's binary image is ok for breakpoints.
                LOG_WARNING(log, "Server is run under debugger and its binary image is modified (most likely with breakpoints).",
                    calculated_binary_hash);
            }
            else
            {
                throw Exception(ErrorCodes::CORRUPTED_DATA,
                    "Calculated checksum of the ClickHouse binary ({0}) does not correspond"
                    " to the reference checksum stored in the binary ({1})."
                    " It may indicate one of the following:"
                    " - the file {2} was changed just after startup;"
                    " - the file {2} is damaged on disk due to faulty hardware;"
                    " - the loaded executable is damaged in memory due to faulty hardware;"
                    " - the file {2} was intentionally modified;"
                    " - logical error in code."
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
            remapExecutable();
            LOG_DEBUG(log, "The code in memory has been successfully remapped.");
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

    std::string path = getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH));
    std::string default_database = config().getString("default_database", "default");

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

    global_context->setPath(path);

    StatusFile status{path + "status", StatusFile::write_full_info};

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

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", DateLUT::instance().getTimeZone());

    /// Storage with temporary data for processing of heavy queries.
    {
        std::string tmp_path = config().getString("tmp_path", path + "tmp/");
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
        auto flags_path = fs::path(path) / "flags/";
        fs::create_directories(flags_path);
        global_context->setFlagsPath(flags_path);
    }

    /** Directory with user provided files that are usable by 'file' table function.
      */
    {

        std::string user_files_path = config().getString("user_files_path", fs::path(path) / "user_files/");
        global_context->setUserFilesPath(user_files_path);
        fs::create_directories(user_files_path);
    }

    {
        std::string dictionaries_lib_path = config().getString("dictionaries_lib_path", fs::path(path) / "dictionaries_lib/");
        global_context->setDictionariesLibPath(dictionaries_lib_path);
        fs::create_directories(dictionaries_lib_path);
    }

    /// top_level_domains_lists
    {
        const std::string & top_level_domains_path = config().getString("top_level_domains_path", fs::path(path) / "top_level_domains/");
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", config());
    }

    {
        fs::create_directories(fs::path(path) / "data/");
        fs::create_directories(fs::path(path) / "metadata/");

        /// Directory with metadata of tables, which was marked as dropped by Atomic database
        fs::create_directories(fs::path(path) / "metadata_dropped/");
    }

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

            // FIXME logging-related things need synchronization -- see the 'Logger * log' saved
            // in a lot of places. For now, disable updating log configuration without server restart.
            //setTextLog(global_context->getTextLog());
            //buildLoggers(*config, logger());
            global_context->setClustersConfig(config);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros", log));
            global_context->setExternalAuthenticatorsConfig(*config);
            global_context->setExternalModelsConfig(config);

            /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
            if (config->has("max_table_size_to_drop"))
                global_context->setMaxTableSizeToDrop(config->getUInt64("max_table_size_to_drop"));

            if (config->has("max_partition_size_to_drop"))
                global_context->setMaxPartitionSizeToDrop(config->getUInt64("max_partition_size_to_drop"));

            if (!initial_loading)
            {
                /// We do not load ZooKeeper configuration on the first config loading
                /// because TestKeeper server is not started yet.
                if (config->has("zookeeper"))
                    global_context->reloadZooKeeperIfChanged(config);

                global_context->reloadAuxiliaryZooKeepersConfigIfChanged(config);
            }

            global_context->updateStorageConfiguration(*config);
            global_context->updateInterserverCredentials(*config);
        },
        /* already_loaded = */ false);  /// Reload it right now (initial loading)

    auto & access_control = global_context->getAccessControlManager();
    if (config().has("custom_settings_prefixes"))
        access_control.setCustomSettingsPrefixes(config().getString("custom_settings_prefixes"));

    /// Initialize access storages.
    access_control.addStoragesFromMainConfig(config(), config_path, [&] { return global_context->getZooKeeper(); });

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]()
    {
        main_config_reloader->reload();
        access_control.reloadUsersConfigs();
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

    /// Size of cache for marks (index of MergeTree family of tables). It is mandatory.
    size_t mark_cache_size = config().getUInt64("mark_cache_size");
    if (!mark_cache_size)
        LOG_ERROR(log, "Too low mark cache size will lead to severe performance degradation.");
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_INFO(log, "Mark cache size was lowered to {} because the system has low amount of memory",
            formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_size);

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
    fs::path format_schema_path(config().getString("format_schema_path", fs::path(path) / "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path);
    fs::create_directories(format_schema_path);

    /// Check sanity of MergeTreeSettings on server startup
    global_context->getMergeTreeSettings().sanityCheck(settings);
    global_context->getReplicatedMergeTreeSettings().sanityCheck(settings);

    Poco::Timespan keep_alive_timeout(config().getUInt("keep_alive_timeout", 10), 0);

    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings.http_receive_timeout);
    http_params->setKeepAliveTimeout(keep_alive_timeout);

    auto servers_to_start_before_tables = std::make_shared<std::vector<ProtocolServerAdapter>>();

    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config(), "", "listen_host");

    bool listen_try = config().getBool("listen_try", false);
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
        listen_try = true;
    }

    if (config().has("keeper_server"))
    {
#if USE_NURAFT
        /// Initialize test keeper RAFT. Do nothing if no nu_keeper_server in config.
        global_context->initializeKeeperStorageDispatcher();
        for (const auto & listen_host : listen_hosts)
        {
            /// TCP Keeper
            const char * port_name = "keeper_server.tcp_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                servers_to_start_before_tables->emplace_back(
                    port_name,
                    std::make_unique<Poco::Net::TCPServer>(
                        new KeeperTCPHandlerFactory(*this, false), server_pool, socket, new Poco::Net::TCPServerParams));

                LOG_INFO(log, "Listening for connections to Keeper (tcp): {}", address.toString());
            });

            const char * secure_port_name = "keeper_server.tcp_port_secure";
            createServer(listen_host, secure_port_name, listen_try, [&](UInt16 port)
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                servers_to_start_before_tables->emplace_back(
                    secure_port_name,
                    std::make_unique<Poco::Net::TCPServer>(
                        new KeeperTCPHandlerFactory(*this, true), server_pool, socket, new Poco::Net::TCPServerParams));
                LOG_INFO(log, "Listening for connections to Keeper with secure protocol (tcp_secure): {}", address.toString());
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

    for (auto & server : *servers_to_start_before_tables)
        server.start();

    SCOPE_EXIT({
        /// Stop reloading of the main config. This must be done before `global_context->shutdown()` because
        /// otherwise the reloading may pass a changed config to some destroyed parts of ContextSharedPart.
        main_config_reloader.reset();

        /** Ask to cancel background jobs all table engines,
          *  and also query_log.
          * It is important to do early, not in destructor of Context, because
          *  table engines could use Context on destroy.
          */
        LOG_INFO(log, "Shutting down storages.");

        global_context->shutdown();

        LOG_DEBUG(log, "Shut down storages.");

        if (!servers_to_start_before_tables->empty())
        {
            LOG_DEBUG(log, "Waiting for current connections to servers for tables to finish.");
            int current_connections = 0;
            for (auto & server : *servers_to_start_before_tables)
            {
                server.stop();
                current_connections += server.currentConnections();
            }

            if (current_connections)
                LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            if (current_connections > 0)
                current_connections = waitServersToFinish(*servers_to_start_before_tables, config().getInt("shutdown_wait_unfinished", 5));

            if (current_connections)
                LOG_INFO(log, "Closed connections to servers for tables. But {} remain. Probably some tables of other users cannot finish their connections after context shutdown.", current_connections);
            else
                LOG_INFO(log, "Closed connections to servers for tables.");

            global_context->shutdownKeeperStorageDispatcher();
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

    LOG_INFO(log, "Loading metadata from {}", path);

    try
    {
        loadMetadataSystem(global_context);
        /// After attaching system databases we can initialize system log.
        global_context->initializeSystemLogs();
        auto & database_catalog = DatabaseCatalog::instance();
        /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
        attachSystemTablesServer(*database_catalog.getSystemDatabase(), has_zookeeper);
        /// We load temporary database first, because projections need it.
        database_catalog.initializeAndLoadTemporaryDatabase();
        /// Then, load remaining databases
        loadMetadata(global_context, default_database);
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
    ///
    /// It also cannot work with sanitizers.
    /// Sanitizers are using quick "frame walking" stack unwinding (this implies -fno-omit-frame-pointer)
    /// And they do unwinding frequently (on every malloc/free, thread/mutex operations, etc).
    /// They change %rbp during unwinding and it confuses libunwind if signal comes during sanitizer unwinding
    ///  and query profiler decide to unwind stack with libunwind at this moment.
    ///
    /// Symptoms: you'll get silent Segmentation Fault - without sanitizer message and without usual ClickHouse diagnostics.
    ///
    /// Look at compiler-rt/lib/sanitizer_common/sanitizer_stacktrace.h
    ///
#if USE_UNWIND && !WITH_COVERAGE && !defined(SANITIZER) && defined(__x86_64__)
    /// Profilers cannot work reliably with any other libunwind or without PHDR cache.
    if (hasPHDRCache())
    {
        global_context->initializeTraceCollector();

        /// Set up server-wide memory profiler (for total memory tracker).
        UInt64 total_memory_profiler_step = config().getUInt64("total_memory_profiler_step", 0);
        if (total_memory_profiler_step)
        {
            total_memory_tracker.setOrRaiseProfilerLimit(total_memory_profiler_step);
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
    LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they cannot work under sanitizers"
        " when two different stack unwinding methods will interfere with each other.");
#endif

#if !defined(__x86_64__)
    LOG_INFO(log, "Query Profiler is only tested on x86_64. It also known to not work under qemu-user.");
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
        dns_cache_updater = std::make_unique<DNSCacheUpdater>(global_context, config().getInt("dns_cache_update_period", 15));
    }

#if defined(OS_LINUX)
    if (!TasksStatsCounters::checkIfAvailable())
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

    auto servers = std::make_shared<std::vector<ProtocolServerAdapter>>();
    {
        /// This object will periodically calculate some metrics.
        AsynchronousMetrics async_metrics(
            global_context, config().getUInt("asynchronous_metrics_update_period_s", 1), servers_to_start_before_tables, servers);
        attachSystemTablesAsync(*DatabaseCatalog::instance().getSystemDatabase(), async_metrics);

        for (const auto & listen_host : listen_hosts)
        {
            /// HTTP
            const char * port_name = "http_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.http_receive_timeout);
                socket.setSendTimeout(settings.http_send_timeout);

                servers->emplace_back(
                    port_name,
                    std::make_unique<HTTPServer>(
                        context(), createHandlerFactory(*this, async_metrics, "HTTPHandler-factory"), server_pool, socket, http_params));

                LOG_INFO(log, "Listening for http://{}", address.toString());
            });

            /// HTTPS
            port_name = "https_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings.http_receive_timeout);
                socket.setSendTimeout(settings.http_send_timeout);
                servers->emplace_back(
                    port_name,
                    std::make_unique<HTTPServer>(
                        context(), createHandlerFactory(*this, async_metrics, "HTTPSHandler-factory"), server_pool, socket, http_params));

                LOG_INFO(log, "Listening for https://{}", address.toString());
#else
                UNUSED(port);
                throw Exception{"HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                    ErrorCodes::SUPPORT_IS_DISABLED};
#endif
            });

            /// TCP
            port_name = "tcp_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                servers->emplace_back(port_name, std::make_unique<Poco::Net::TCPServer>(
                    new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ false),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));

                LOG_INFO(log, "Listening for connections with native protocol (tcp): {}", address.toString());
            });

            /// TCP with PROXY protocol, see https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
            port_name = "tcp_with_proxy_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                servers->emplace_back(port_name, std::make_unique<Poco::Net::TCPServer>(
                    new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ true),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));

                LOG_INFO(log, "Listening for connections with native protocol (tcp) with PROXY: {}", address.toString());
            });

            /// TCP with SSL
            port_name = "tcp_port_secure";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings.receive_timeout);
                socket.setSendTimeout(settings.send_timeout);
                servers->emplace_back(port_name, std::make_unique<Poco::Net::TCPServer>(
                    new TCPHandlerFactory(*this, /* secure */ true, /* proxy protocol */ false),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));
                LOG_INFO(log, "Listening for connections with secure native protocol (tcp_secure): {}", address.toString());
#else
                UNUSED(port);
                throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                    ErrorCodes::SUPPORT_IS_DISABLED};
#endif
            });

            /// Interserver IO HTTP
            port_name = "interserver_http_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.http_receive_timeout);
                socket.setSendTimeout(settings.http_send_timeout);
                servers->emplace_back(
                    port_name,
                    std::make_unique<HTTPServer>(
                        context(),
                        createHandlerFactory(*this, async_metrics, "InterserverIOHTTPHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                LOG_INFO(log, "Listening for replica communication (interserver): http://{}", address.toString());
            });

            port_name = "interserver_https_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings.http_receive_timeout);
                socket.setSendTimeout(settings.http_send_timeout);
                servers->emplace_back(
                    port_name,
                    std::make_unique<HTTPServer>(
                        context(),
                        createHandlerFactory(*this, async_metrics, "InterserverIOHTTPSHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                LOG_INFO(log, "Listening for secure replica communication (interserver): https://{}", address.toString());
#else
                UNUSED(port);
                throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
            });

            port_name = "mysql_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(Poco::Timespan());
                socket.setSendTimeout(settings.send_timeout);
                servers->emplace_back(port_name, std::make_unique<Poco::Net::TCPServer>(
                    new MySQLHandlerFactory(*this),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));

                LOG_INFO(log, "Listening for MySQL compatibility protocol: {}", address.toString());
            });

            port_name = "postgresql_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(Poco::Timespan());
                socket.setSendTimeout(settings.send_timeout);
                servers->emplace_back(port_name, std::make_unique<Poco::Net::TCPServer>(
                    new PostgreSQLHandlerFactory(*this),
                    server_pool,
                    socket,
                    new Poco::Net::TCPServerParams));

                LOG_INFO(log, "Listening for PostgreSQL compatibility protocol: " + address.toString());
            });

#if USE_GRPC
            port_name = "grpc_port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::SocketAddress server_address(listen_host, port);
                servers->emplace_back(port_name, std::make_unique<GRPCServer>(*this, makeSocketAddress(listen_host, port, log)));
                LOG_INFO(log, "Listening for gRPC protocol: " + server_address.toString());
            });
#endif

            /// Prometheus (if defined and not setup yet with http_port)
            port_name = "prometheus.port";
            createServer(listen_host, port_name, listen_try, [&](UInt16 port)
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(settings.http_receive_timeout);
                socket.setSendTimeout(settings.http_send_timeout);
                servers->emplace_back(
                    port_name,
                    std::make_unique<HTTPServer>(
                        context(),
                        createHandlerFactory(*this, async_metrics, "PrometheusHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                LOG_INFO(log, "Listening for Prometheus: http://{}", address.toString());
            });
        }

        if (servers->empty())
             throw Exception("No servers started (add valid listen_host and 'tcp_port' or 'http_port' to configuration file.)",
                ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        /// Must be done after initialization of `servers`, because async_metrics will access `servers` variable from its thread.
        async_metrics.start();
        global_context->enableNamedSessions();

        {
            String level_str = config().getString("text_log.level", "");
            int level = level_str.empty() ? INT_MAX : Poco::Logger::parseLevel(level_str);
            setTextLog(global_context->getTextLog(), level);
        }

        buildLoggers(config(), logger());

        main_config_reloader->start();
        access_control.startPeriodicReloadingUsersConfigs();
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
            global_context->loadDictionaries(config());
        }
        catch (...)
        {
            LOG_ERROR(log, "Caught exception while loading dictionaries.");
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
                                                                     "distributed_ddl", "DDLWorker", &CurrentMetrics::MaxDDLEntryID));
        }

        for (auto & server : *servers)
            server.start();
        LOG_INFO(log, "Ready for connections.");

        SCOPE_EXIT({
            LOG_DEBUG(log, "Received termination signal.");
            LOG_DEBUG(log, "Waiting for current connections to close.");

            is_cancelled = true;

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

            /// Killing remaining queries.
            global_context->getProcessList().killAllQueries();

            if (current_connections)
                current_connections = waitServersToFinish(*servers, config().getInt("shutdown_wait_unfinished", 5));

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
                forceShutdown();
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
}
