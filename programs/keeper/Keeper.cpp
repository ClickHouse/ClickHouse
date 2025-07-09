#include <Keeper.h>

#include <Common/ClickHouseRevision.h>
#include <Common/formatReadable.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/DNSResolver.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Coordination/Defines.h>
#include <Common/Config/ConfigReloader.h>
#include <filesystem>
#include <Core/ServerUUID.h>
#include <Common/logger_useful.h>
#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/DateLUT.h>
#include <Common/MemoryWorker.h>
#include <Common/ErrorHandlers.h>
#include <Common/assertProcessUserMatchesDataOwner.h>
#include <Common/makeSocketAddress.h>
#include <Server/waitServersToFinish.h>
#include <Server/CloudPlacementInfo.h>
#include <base/getMemoryAmount.h>
#include <base/scope_guard.h>
#include <base/safeExit.h>
#include <base/Numa.h>
#include <Poco/Net/NetException.h>
#include <Poco/Net/TCPServerParams.h>
#include <Poco/Net/TCPServer.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Environment.h>
#include <sys/stat.h>
#include <pwd.h>

#include <Common/Jemalloc.h>

#include <Interpreters/Context.h>

#include <Coordination/FourLetterCommand.h>
#include <Coordination/KeeperAsynchronousMetrics.h>

#include <Server/HTTP/HTTPServer.h>
#include <Server/HTTPHandlerFactory.h>
#include <Server/KeeperReadinessHandler.h>
#include <Server/PrometheusRequestHandlerFactory.h>
#include <Server/TCPServer.h>

#include <Core/Defines.h>
#include "config.h"
#include <Common/config_version.h>
#include "config_tools.h"


#if USE_SSL
#    include <Poco/Net/Context.h>
#    include <Poco/Net/SecureServerSocket.h>
#    include <Server/CertificateReloader.h>
#endif

#if USE_GWP_ASAN
#    include <Common/GWPAsan.h>
#endif

#include <Server/ProtocolServerAdapter.h>
#include <Server/KeeperTCPHandlerFactory.h>

#include <Disks/registerDisks.h>

#include <incbin.h>
/// A minimal file used when the keeper is run without installation
INCBIN(keeper_resource_embedded_xml, SOURCE_DIR "/programs/keeper/keeper_embedded.xml");

extern const char * GIT_HASH;

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
        return static_cast<UInt8>(code) ? code : 1;
    }
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int NETWORK_ERROR;
    extern const int LOGICAL_ERROR;
}

Poco::Net::SocketAddress Keeper::socketBindListen(Poco::Net::ServerSocket & socket, const std::string & host, UInt16 port, [[maybe_unused]] bool secure) const
{
    auto address = makeSocketAddress(host, port, &logger());
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config().getBool("listen_reuse_port", false));
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
        if (listen_try)
        {
            LOG_WARNING(&logger(), "Listen [{}]:{} failed: {}. If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, "
                "then consider to "
                "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                " Example for disabled IPv4: <listen_host>::</listen_host>",
                listen_host, port, getCurrentExceptionMessage(false));
        }
        else
        {
            throw Exception(ErrorCodes::NETWORK_ERROR, "Listen [{}]:{} failed: {}", listen_host, port, getCurrentExceptionMessage(false));
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
        auto header_str = fmt::format("{0} [OPTION] [-- [ARG]...]\n"
#if ENABLE_CLICKHOUSE_KEEPER_CLIENT
                                      "{0} client [OPTION]\n"
#endif
                                      "positional arguments can be used to rewrite config.xml properties, for example, --http_port=8010",
                                      commandName());
        help_formatter.setHeader(header_str);
        help_formatter.format(std::cout);
        return 0;
    }
    if (config().hasOption("version"))
    {
        std::cout << VERSION_NAME << " keeper version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
        return 0;
    }

    return Application::run(); // NOLINT
}

void Keeper::initialize(Poco::Util::Application & self)
{
    ConfigProcessor::registerEmbeddedConfig("keeper_config.xml", std::string_view(reinterpret_cast<const char *>(gkeeper_resource_embedded_xmlData), gkeeper_resource_embedded_xmlSize));

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

bool Keeper::allowTextLog() const
{
    return false;
}

void Keeper::handleCustomArguments(const std::string & arg, [[maybe_unused]] const std::string & value) // NOLINT
{
    if (arg == "force-recovery")
    {
        assert(value.empty());
        config().setBool("keeper_server.force_recovery", true);
        return;
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid argument {} provided", arg);
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
    options.addOption(
        Poco::Util::Option("force-recovery", "force-recovery", "Force recovery mode allowing Keeper to overwrite cluster configuration without quorum")
        .required(false)
        .repeatable(false)
        .noArgument()
        .callback(Poco::Util::OptionCallback<Keeper>(this, &Keeper::handleCustomArguments)));
    BaseDaemon::defineOptions(options);
}

namespace
{

struct KeeperHTTPContext : public IHTTPContext
{
    explicit KeeperHTTPContext(ContextPtr context_)
        : context(std::move(context_))
    {}

    uint64_t getMaxHstsAge() const override
    {
        return context->getConfigRef().getUInt64("keeper_server.hsts_max_age", 0);
    }

    uint64_t getMaxUriSize() const override
    {
        return context->getConfigRef().getUInt64("keeper_server.http_max_uri_size", 1048576);
    }

    uint64_t getMaxFields() const override
    {
        return context->getConfigRef().getUInt64("keeper_server.http_max_fields", 1000000);
    }

    uint64_t getMaxFieldNameSize() const override
    {
        return context->getConfigRef().getUInt64("keeper_server.http_max_field_name_size", 128 * 1024);
    }

    uint64_t getMaxFieldValueSize() const override
    {
        return context->getConfigRef().getUInt64("keeper_server.http_max_field_value_size", 128 * 1024);
    }

    Poco::Timespan getReceiveTimeout() const override
    {
        return {context->getConfigRef().getInt64("keeper_server.http_receive_timeout", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0};
    }

    Poco::Timespan getSendTimeout() const override
    {
        return {context->getConfigRef().getInt64("keeper_server.http_send_timeout", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0};
    }

    ContextPtr context;
};

HTTPContextPtr httpContext()
{
    return std::make_shared<KeeperHTTPContext>(Context::getGlobalContextInstance());
}

String getKeeperPath(Poco::Util::LayeredConfiguration & config)
{
    String path;
    if (config.has("keeper_server.storage_path"))
    {
        path = config.getString("keeper_server.storage_path");
    }
    else if (config.has("keeper_server.log_storage_path"))
    {
        path = std::filesystem::path(config.getString("keeper_server.log_storage_path")).parent_path();
    }
    else if (config.has("keeper_server.snapshot_storage_path"))
    {
        path = std::filesystem::path(config.getString("keeper_server.snapshot_storage_path")).parent_path();
    }
    else if (std::filesystem::is_directory(std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination"))
    {
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                        "By default 'keeper_server.storage_path' could be assigned to {}, but the directory {} already exists. Please specify 'keeper_server.storage_path' in the keeper configuration explicitly",
                        KEEPER_DEFAULT_PATH, String{std::filesystem::path{config.getString("path", DBMS_DEFAULT_PATH)} / "coordination"});
    }
    else
    {
        path = KEEPER_DEFAULT_PATH;
    }
    return path;
}


}

int Keeper::main(const std::vector<std::string> & /*args*/)
try
{
#if USE_JEMALLOC
    setJemallocBackgroundThreads(true);
#endif
    Poco::Logger * log = &logger();

    MainThreadStatus::getInstance();

    if (auto total_numa_memory = getNumaNodesTotalMemory(); total_numa_memory.has_value())
    {
        LOG_INFO(
            log, "Keeper is bound to a subset of NUMA nodes. Total memory of all available nodes: {}", ReadableSize(*total_numa_memory));
    }

#if !defined(NDEBUG) || !defined(__OPTIMIZE__)
    LOG_WARNING(log, "Keeper was built in debug mode. It will work slowly.");
#endif

#if defined(SANITIZER)
    LOG_WARNING(log, "Keeper was built with sanitizer. It will work slowly.");
#endif

    if (!config().has("keeper_server"))
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Keeper configuration (<keeper_server> section) not found in config");

    auto updateMemorySoftLimitInConfig = [&](Poco::Util::AbstractConfiguration & config)
    {
        UInt64 memory_soft_limit = 0;
        if (config.has("keeper_server.max_memory_usage_soft_limit"))
        {
            memory_soft_limit = config.getUInt64("keeper_server.max_memory_usage_soft_limit");
        }

        /// if memory soft limit is not set, we will use default value
        if (memory_soft_limit == 0)
        {
            Float64 ratio = 0.9;
            if (config.has("keeper_server.max_memory_usage_soft_limit_ratio"))
                ratio = config.getDouble("keeper_server.max_memory_usage_soft_limit_ratio");

            size_t physical_server_memory = getMemoryAmount();
            if (ratio > 0 && physical_server_memory > 0)
            {
                memory_soft_limit = static_cast<UInt64>(physical_server_memory * ratio);
                config.setUInt64("keeper_server.max_memory_usage_soft_limit", memory_soft_limit);
            }
        }
        LOG_INFO(log, "keeper_server.max_memory_usage_soft_limit is set to {}", formatReadableSizeWithBinarySuffix(memory_soft_limit));
    };

    updateMemorySoftLimitInConfig(config());

    std::string path = getKeeperPath(config());
    std::filesystem::create_directories(path);

    /// Check that the process user id matches the owner of the data.
    assertProcessUserMatchesDataOwner(path, [&](const PreformattedMessage & message){ LOG_WARNING(log, fmt::runtime(message.text)); });

    DB::ServerUUID::load(path + "/uuid", log);

    std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");

    PlacementInfo::PlacementInfo::instance().initialize(config());

    GlobalThreadPool::initialize(
        /// We need to have sufficient amount of threads for connections + nuraft workers + keeper workers, 1000 is an estimation
        std::min(1000U, config().getUInt("max_thread_pool_size", 1000)),
        config().getUInt("max_thread_pool_free_size", 100),
        config().getUInt("thread_pool_queue_size", 1000)
    );
    /// Wait for all threads to avoid possible use-after-free (for example logging objects can be already destroyed).
    SCOPE_EXIT({
        Stopwatch watch;
        LOG_INFO(log, "Waiting for background threads");
        GlobalThreadPool::instance().shutdown();
        LOG_INFO(log, "Background threads finished in {} ms", watch.elapsedMilliseconds());
    });

    MemoryWorker memory_worker(
        config().getUInt64("memory_worker_period_ms", 0), config().getBool("memory_worker_correct_memory_tracker", false), /*use_cgroup*/ true, /*page_cache*/ nullptr);
    memory_worker.start();

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::serverTimezoneInstance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", getDateLUTTimeZone(DateLUT::serverTimezoneInstance()));

    /// Don't want to use DNS cache
    DNSResolver::instance().setDisableCacheFlag();

    Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
    std::mutex servers_lock;
    auto servers = std::make_shared<std::vector<ProtocolServerAdapter>>();

    auto shared_context = Context::createShared();
    auto global_context = Context::createGlobal(shared_context.get());

    global_context->makeGlobalContext();
    global_context->setApplicationType(Context::ApplicationType::KEEPER);
    global_context->setPath(path);
    global_context->setRemoteHostFilter(config());

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    registerDisks(/*global_skip_access_check=*/false);

    /// This object will periodically calculate some metrics.
    KeeperAsynchronousMetrics async_metrics(
        global_context,
        config().getUInt("asynchronous_metrics_update_period_s", 1),
        [&]() -> std::vector<ProtocolServerMetrics>
        {
            std::vector<ProtocolServerMetrics> metrics;

            std::lock_guard lock(servers_lock);
            metrics.reserve(servers->size());
            for (const auto & server : *servers)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads(), server.refusedConnections()});
            return metrics;
        },
        /*update_jemalloc_epoch_=*/memory_worker.getSource() != MemoryWorker::MemoryUsageSource::Jemalloc,
        /*update_rss_=*/memory_worker.getSource() == MemoryWorker::MemoryUsageSource::None);

    std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config(), "", "listen_host");

    bool listen_try = config().getBool("listen_try", false);
    if (listen_hosts.empty())
    {
        listen_hosts.emplace_back("::1");
        listen_hosts.emplace_back("127.0.0.1");
        listen_try = true;
    }

    /// Initialize keeper RAFT. Do nothing if no keeper_server in config.
    global_context->initializeKeeperDispatcher(/* start_async = */ false);
    FourLetterCommandFactory::registerCommands(*global_context->getKeeperDispatcher());

    auto config_getter = [&] () -> const Poco::Util::AbstractConfiguration &
    {
        return global_context->getConfigRef();
    };

    auto tcp_receive_timeout = config().getInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC);
    auto tcp_send_timeout = config().getInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC);

    for (const auto & listen_host : listen_hosts)
    {
        /// TCP Keeper
        const char * port_name = "keeper_server.tcp_port";
        createServer(listen_host, port_name, listen_try, [&](UInt16 port)
        {
            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(socket, listen_host, port);
            socket.setReceiveTimeout(Poco::Timespan{tcp_receive_timeout, 0});
            socket.setSendTimeout(Poco::Timespan{tcp_send_timeout, 0});
            servers->emplace_back(
                listen_host,
                port_name,
                "Keeper (tcp): " + address.toString(),
                std::make_unique<TCPServer>(
                    new KeeperTCPHandlerFactory(
                        config_getter, global_context->getKeeperDispatcher(),
                        tcp_receive_timeout, tcp_send_timeout, false), server_pool, socket));
        });

        const char * secure_port_name = "keeper_server.tcp_port_secure";
        createServer(listen_host, secure_port_name, listen_try, [&](UInt16 port)
        {
#if USE_SSL
            Poco::Net::SecureServerSocket socket;
            auto address = socketBindListen(socket, listen_host, port, /* secure = */ true);
            socket.setReceiveTimeout(Poco::Timespan{tcp_receive_timeout, 0});
            socket.setSendTimeout(Poco::Timespan{tcp_send_timeout, 0});
            servers->emplace_back(
                listen_host,
                secure_port_name,
                "Keeper with secure protocol (tcp_secure): " + address.toString(),
                std::make_unique<TCPServer>(
                    new KeeperTCPHandlerFactory(
                        config_getter, global_context->getKeeperDispatcher(),
                        tcp_receive_timeout, tcp_send_timeout, true), server_pool, socket));
#else
            UNUSED(port);
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
        });

        const auto & config = config_getter();
        auto http_context = httpContext();
        Poco::Timespan keep_alive_timeout(config.getUInt("keep_alive_timeout", 10), 0);
        Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
        http_params->setTimeout(http_context->getReceiveTimeout());
        http_params->setKeepAliveTimeout(keep_alive_timeout);

        /// Prometheus (if defined and not setup yet with http_port)
        port_name = "prometheus.port";
        createServer(
            listen_host,
            port_name,
            listen_try,
            [&, my_http_context = std::move(http_context)](UInt16 port) mutable
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(socket, listen_host, port);
                socket.setReceiveTimeout(my_http_context->getReceiveTimeout());
                socket.setSendTimeout(my_http_context->getSendTimeout());
                servers->emplace_back(
                    listen_host,
                    port_name,
                    "Prometheus: http://" + address.toString(),
                    std::make_unique<HTTPServer>(
                        std::move(my_http_context),
                        createKeeperPrometheusHandlerFactory(*this, config_getter(), async_metrics, "PrometheusHandler-factory"),
                        server_pool,
                        socket,
                        http_params));
            });

        /// HTTP control endpoints
        port_name = "keeper_server.http_control.port";
        createServer(listen_host, port_name, listen_try, [&](UInt16 port) mutable
        {
            auto my_http_context = httpContext();
            Poco::Timespan my_keep_alive_timeout(config.getUInt("keep_alive_timeout", 10), 0);
            Poco::Net::HTTPServerParams::Ptr my_http_params = new Poco::Net::HTTPServerParams;
            my_http_params->setTimeout(my_http_context->getReceiveTimeout());
            my_http_params->setKeepAliveTimeout(my_keep_alive_timeout);

            Poco::Net::ServerSocket socket;
            auto address = socketBindListen(socket, listen_host, port);
            socket.setReceiveTimeout(my_http_context->getReceiveTimeout());
            socket.setSendTimeout(my_http_context->getSendTimeout());
            servers->emplace_back(
                listen_host,
                port_name,
                "HTTP Control: http://" + address.toString(),
                std::make_unique<HTTPServer>(
                    std::move(my_http_context), createKeeperHTTPControlMainHandlerFactory(config_getter(), global_context->getKeeperDispatcher(), "KeeperHTTPControlHandler-factory"), server_pool, socket, http_params)
                    );
        });
    }

    for (auto & server : *servers)
    {
        server.start();
        LOG_INFO(log, "Listening for {}", server.getDescription());
    }

    async_metrics.start();

    zkutil::EventPtr unused_event = std::make_shared<Poco::Event>();
    zkutil::ZooKeeperNodeCache unused_cache([] { return nullptr; });

    const std::string cert_path = config().getString("openSSL.server.certificateFile", "");
    const std::string key_path = config().getString("openSSL.server.privateKeyFile", "");

    std::vector<std::string> extra_paths = {include_from_path};
    if (!cert_path.empty())
        extra_paths.emplace_back(cert_path);
    if (!key_path.empty())
        extra_paths.emplace_back(key_path);

    /// ConfigReloader have to strict parameters which are redundant in our case
    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        extra_paths,
        getKeeperPath(config()),
        std::move(unused_cache),
        unused_event,
        [&](ConfigurationPtr config, bool /* initial_loading */)
        {
            updateLevels(*config, logger());

            updateMemorySoftLimitInConfig(*config);

            if (config->has("keeper_server"))
                global_context->updateKeeperConfiguration(*config);

#if USE_SSL
            CertificateReloader::instance().tryLoad(*config);
            CertificateReloader::instance().tryLoadClient(*config);
#endif
        });

    SCOPE_EXIT({
        LOG_INFO(log, "Shutting down.");
        main_config_reloader.reset();

        async_metrics.stop();

        LOG_DEBUG(log, "Waiting for current connections to Keeper to finish.");
        size_t current_connections = 0;
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
            current_connections = waitServersToFinish(*servers, servers_lock, config().getInt("shutdown_wait_unfinished", 5));

        if (current_connections)
            LOG_INFO(log, "Closed connections to Keeper. But {} remain. Probably some users cannot finish their connections after context shutdown.", current_connections);
        else
            LOG_INFO(log, "Closed connections to Keeper.");

        global_context->shutdownKeeperDispatcher();

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

    std::optional<CgroupsMemoryUsageObserver> cgroups_memory_usage_observer;
    try
    {
        auto wait_time = config().getUInt64("keeper_server.cgroups_memory_observer_wait_time", 15);
        if (wait_time != 0)
        {
            cgroups_memory_usage_observer.emplace(std::chrono::seconds(wait_time));
            /// Not calling cgroups_memory_usage_observer->setLimits() here (as for the normal ClickHouse server) because Keeper controls
            /// its memory usage by other means (via setting 'max_memory_usage_soft_limit').
            cgroups_memory_usage_observer->setOnMemoryAmountAvailableChangedFn([&]() { main_config_reloader->reload(); });
            cgroups_memory_usage_observer->startThread();
        }
    }
    catch (Exception &)
    {
        tryLogCurrentException(log, "Disabling cgroup memory observer because of an error during initialization");
    }

#if USE_GWP_ASAN
    GWPAsan::initFinished();
#endif

    LOG_INFO(log, "Ready for connections.");

    waitForTerminationRequest();

    return Application::EXIT_OK;
}
catch (...)
{
    /// Poco does not provide stacktrace.
    tryLogCurrentException("Application");
    auto code = getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : -1;
}


void Keeper::logRevision() const
{
    LOG_INFO(getLogger("Application"),
        "Starting ClickHouse Keeper {} (revision: {}, git hash: {}, build id: {}), PID {}",
        VERSION_STRING,
        ClickHouseRevision::getVersionRevision(),
        GIT_HASH,
        build_id.empty() ? "<unknown>" : build_id,
        getpid());
}


}
