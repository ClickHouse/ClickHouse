#include "Server.h"

#include <memory>
#include <sys/resource.h>
#include <errno.h>
#include <Poco/Version.h>
#include <Poco/DirectoryIterator.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <ext/scope_guard.h>
#include <common/logger_useful.h>
#include <common/ErrorHandlers.h>
#include <common/getMemoryAmount.h>
#include <Common/ClickHouseRevision.h>
#include <Common/DNSResolver.h>
#include <Common/CurrentMetrics.h>
#include <Common/Macros.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/config.h>
#include <Common/getFQDNOrHostName.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfPhysicalCPUCores.h>
#include <Common/TaskStatsInfoGetter.h>
#include <IO/HTTPCommon.h>
#include <Interpreters/AsynchronousMetrics.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/attachSystemTables.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Storages/registerStorages.h>
#include <Common/Config/ConfigReloader.h>
#include "HTTPHandlerFactory.h"
#include "MetricsTransmitter.h"
#include <Common/StatusFile.h>
#include "TCPHandlerFactory.h"

#if USE_POCO_NETSSL
#include <Poco/Net/Context.h>
#include <Poco/Net/SecureServerSocket.h>
#endif

namespace CurrentMetrics
{
    extern const Metric Revision;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int SUPPORT_IS_DISABLED;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int EXCESSIVE_ELEMENT_IN_CONFIG;
}


static std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception("path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return std::move(path);
}

void Server::uninitialize()
{
    logger().information("shutting down");
    BaseDaemon::uninitialize();
}

void Server::initialize(Poco::Util::Application & self)
{
    BaseDaemon::initialize(self);
    logger().information("starting up");
}

std::string Server::getDefaultCorePath() const
{
    return getCanonicalPath(config().getString("path")) + "cores";
}

int Server::main(const std::vector<std::string> & /*args*/)
{
    Logger * log = &logger();

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();

    CurrentMetrics::set(CurrentMetrics::Revision, ClickHouseRevision::get());

    /** Context contains all that query execution is dependent:
      *  settings, available functions, data types, aggregate functions, databases...
      */
    global_context = std::make_unique<Context>(Context::createGlobal());
    global_context->setGlobalContext(*global_context);
    global_context->setApplicationType(Context::ApplicationType::SERVER);

    bool has_zookeeper = config().has("zookeeper");

    zkutil::ZooKeeperNodeCache main_config_zk_node_cache([&] { return global_context->getZooKeeper(); });
    if (loaded_config.has_zk_includes)
    {
        auto old_configuration = loaded_config.configuration;
        ConfigProcessor config_processor(config_path);
        loaded_config = config_processor.loadConfigWithZooKeeperIncludes(
            main_config_zk_node_cache, /* fallback_to_preprocessed = */ true);
        config_processor.savePreprocessedConfig(loaded_config);
        config().removeConfiguration(old_configuration.get());
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
    }

    std::string path = getCanonicalPath(config().getString("path"));
    std::string default_database = config().getString("default_database", "default");

    global_context->setPath(path);

    /// Create directories for 'path' and for default database, if not exist.
    Poco::File(path + "data/" + default_database).createDirectories();
    Poco::File(path + "metadata/" + default_database).createDirectories();

    StatusFile status{path + "status"};

    SCOPE_EXIT({
        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context.reset();

        LOG_DEBUG(log, "Destroyed global context.");
    });

    /// Try to increase limit on number of open files.
    {
        rlimit rlim;
        if (getrlimit(RLIMIT_NOFILE, &rlim))
            throw Poco::Exception("Cannot getrlimit");

        if (rlim.rlim_cur == rlim.rlim_max)
        {
            LOG_DEBUG(log, "rlimit on number of file descriptors is " << rlim.rlim_cur);
        }
        else
        {
            rlim_t old = rlim.rlim_cur;
            rlim.rlim_cur = config().getUInt("max_open_files", rlim.rlim_max);
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                LOG_WARNING(log,
                    "Cannot set max number of file descriptors to " << rlim.rlim_cur
                        << ". Try to specify max_open_files according to your system limits. error: "
                        << strerror(errno));
            else
                LOG_DEBUG(log, "Set max number of file descriptors to " << rlim.rlim_cur << " (was " << old << ").");
        }
    }

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::instance();
    LOG_TRACE(log, "Initialized DateLUT with time zone `" << DateLUT::instance().getTimeZone() << "'.");

    /// Directory with temporary data for processing of heavy queries.
    {
        std::string tmp_path = config().getString("tmp_path", path + "tmp/");
        global_context->setTemporaryPath(tmp_path);
        Poco::File(tmp_path).createDirectories();

        /// Clearing old temporary files.
        Poco::DirectoryIterator dir_end;
        for (Poco::DirectoryIterator it(tmp_path); it != dir_end; ++it)
        {
            if (it->isFile() && startsWith(it.name(), "tmp"))
            {
                LOG_DEBUG(log, "Removing old temporary file " << it->path());
                it->remove();
            }
        }
    }

    /** Directory with 'flags': files indicating temporary settings for the server set by system administrator.
      * Flags may be cleared automatically after being applied by the server.
      * Examples: do repair of local data; clone all replicated tables from replica.
      */
    {
        Poco::File(path + "flags/").createDirectories();
        global_context->setFlagsPath(path + "flags/");
    }

    /** Directory with user provided files that are usable by 'file' table function.
      */
    {

        std::string user_files_path = config().getString("user_files_path", path + "user_files/");
        global_context->setUserFilesPath(user_files_path);
        Poco::File(user_files_path).createDirectories();
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
                LOG_DEBUG(log,
                    "Configuration parameter '" + String(host_tag) + "' doesn't exist or exists and empty. Will use '" + this_host
                        + "' as replica host.");
            }

            String port_str = config().getString(port_tag);
            int port = parse<int>(port_str);

            if (port < 0 || port > 0xFFFF)
                throw Exception("Out of range '" + String(port_tag) + "': " + toString(port), ErrorCodes::ARGUMENT_OUT_OF_BOUND);

            global_context->setInterserverIOAddress(this_host, port);
            global_context->setInterserverScheme(scheme);
        }
    }

    if (config().has("interserver_http_credentials"))
    {
        String user = config().getString("interserver_http_credentials.user", "");
        String password = config().getString("interserver_http_credentials.password", "");

        if (user.empty())
            throw Exception("Configuration parameter interserver_http_credentials user can't be empty", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        global_context->setInterserverCredentials(user, password);
    }

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros"));

    /// Initialize main config reloader.
    std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");
    auto main_config_reloader = std::make_unique<ConfigReloader>(config_path,
        include_from_path,
        std::move(main_config_zk_node_cache),
        [&](ConfigurationPtr config)
        {
            buildLoggers(*config);
            global_context->setClustersConfig(config);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros"));
        },
        /* already_loaded = */ true);

    /// Initialize users config reloader.
    std::string users_config_path = config().getString("users_config", config_path);
    /// If path to users' config isn't absolute, try guess its root (current) dir.
    /// At first, try to find it in dir of main config, after will use current dir.
    if (users_config_path.empty() || users_config_path[0] != '/')
    {
        std::string config_dir = Poco::Path(config_path).parent().toString();
        if (Poco::File(config_dir + users_config_path).exists())
            users_config_path = config_dir + users_config_path;
    }
    auto users_config_reloader = std::make_unique<ConfigReloader>(users_config_path,
        include_from_path,
        zkutil::ZooKeeperNodeCache([&] { return global_context->getZooKeeper(); }),
        [&](ConfigurationPtr config) { global_context->setUsersConfig(config); },
        /* already_loaded = */ false);

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]()
    {
        main_config_reloader->reload();
        users_config_reloader->reload();
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(config().getInt("max_concurrent_queries", 0));

    /// Setup protection to avoid accidental DROP for big tables (that are greater than 50 GB by default)
    if (config().has("max_table_size_to_drop"))
        global_context->setMaxTableSizeToDrop(config().getUInt64("max_table_size_to_drop"));

    if (config().has("max_partition_size_to_drop"))
        global_context->setMaxPartitionSizeToDrop(config().getUInt64("max_partition_size_to_drop"));

    /// Size of cache for uncompressed blocks. Zero means disabled.
    size_t uncompressed_cache_size = config().getUInt64("uncompressed_cache_size", 0);
    if (uncompressed_cache_size)
        global_context->setUncompressedCache(uncompressed_cache_size);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());
    Settings & settings = global_context->getSettingsRef();

    /// Size of cache for marks (index of MergeTree family of tables). It is necessary.
    size_t mark_cache_size = config().getUInt64("mark_cache_size");
    if (mark_cache_size)
        global_context->setMarkCache(mark_cache_size);

    /// Set path for format schema files
    auto format_schema_path = Poco::File(config().getString("format_schema_path", path + "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path.path() + "/");
    format_schema_path.createDirectories();

    LOG_INFO(log, "Loading metadata.");
    loadMetadataSystem(*global_context);
    /// After attaching system databases we can initialize system log.
    global_context->initializeSystemLogs();
    /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
    attachSystemTablesServer(*global_context->getDatabase("system"), has_zookeeper);
    /// Then, load remaining databases
    loadMetadata(*global_context);
    LOG_DEBUG(log, "Loaded metadata.");

    global_context->setCurrentDatabase(default_database);

    SCOPE_EXIT({
        /** Ask to cancel background jobs all table engines,
          *  and also query_log.
          * It is important to do early, not in destructor of Context, because
          *  table engines could use Context on destroy.
          */
        LOG_INFO(log, "Shutting down storages.");
        global_context->shutdown();
        LOG_DEBUG(log, "Shutted down storages.");
    });

    if (has_zookeeper && config().has("distributed_ddl"))
    {
        /// DDL worker should be started after all tables were loaded
        String ddl_zookeeper_path = config().getString("distributed_ddl.path", "/clickhouse/task_queue/ddl/");
        global_context->setDDLWorker(std::make_shared<DDLWorker>(ddl_zookeeper_path, *global_context, &config(), "distributed_ddl"));
    }

    std::unique_ptr<DNSCacheUpdater> dns_cache_updater;
    if (config().has("disable_internal_dns_cache") && config().getInt("disable_internal_dns_cache"))
    {
        /// Disable DNS caching at all
        DNSResolver::instance().setDisableCacheFlag();
    }
    else
    {
        /// Initialize a watcher updating DNS cache in case of network errors
        dns_cache_updater = std::make_unique<DNSCacheUpdater>(*global_context);
    }

    if (!TaskStatsInfoGetter::checkProcessHasRequiredPermissions())
    {
        LOG_INFO(log, "It looks like the process has not CAP_NET_ADMIN capability, some performance statistics will be disabled."
                      " It could happen due to incorrect clickhouse package installation."
                      " You could resolve the problem manually calling 'sudo setcap cap_net_admin=+ep /usr/bin/clickhouse'");
    }

    {
        Poco::Timespan keep_alive_timeout(config().getUInt("keep_alive_timeout", 10), 0);

        Poco::ThreadPool server_pool(3, config().getUInt("max_connections", 1024));
        Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
        http_params->setTimeout(settings.http_receive_timeout);
        http_params->setKeepAliveTimeout(keep_alive_timeout);

        std::vector<std::unique_ptr<Poco::Net::TCPServer>> servers;

        std::vector<std::string> listen_hosts = DB::getMultipleValuesFromConfig(config(), "", "listen_host");

        bool listen_try = config().getBool("listen_try", false);
        if (listen_hosts.empty())
        {
            listen_hosts.emplace_back("::1");
            listen_hosts.emplace_back("127.0.0.1");
            listen_try = true;
        }

        auto make_socket_address = [&](const std::string & host, UInt16 port)
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
                    LOG_ERROR(log,
                        "Cannot resolve listen_host (" << host << "), error " << e.code() << ": " << e.message() << ". "
                        "If it is an IPv6 address and your host has disabled IPv6, then consider to "
                        "specify IPv4 address to listen in <listen_host> element of configuration "
                        "file. Example: <listen_host>0.0.0.0</listen_host>");
                }

                throw;
            }
            return socket_address;
        };

        auto socket_bind_listen = [&](auto & socket, const std::string & host, UInt16 port, bool secure = 0)
        {
               auto address = make_socket_address(host, port);
#if !POCO_CLICKHOUSE_PATCH || POCO_VERSION <= 0x02000000 // TODO: fill correct version
               if (secure)
                   /// Bug in old poco, listen() after bind() with reusePort param will fail because have no implementation in SecureServerSocketImpl
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
        };

        for (const auto & listen_host : listen_hosts)
        {
            /// For testing purposes, user may omit tcp_port or http_port or https_port in configuration file.
            try
            {
                /// HTTP
                if (config().has("http_port"))
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("http_port"));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(new Poco::Net::HTTPServer(
                        new HTTPHandlerFactory(*this, "HTTPHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                    LOG_INFO(log, "Listening http://" + address.toString());
                }

                /// HTTPS
                if (config().has("https_port"))
                {
#if USE_POCO_NETSSL
                    initSSL();
                    Poco::Net::SecureServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("https_port"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(new Poco::Net::HTTPServer(
                        new HTTPHandlerFactory(*this, "HTTPSHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                    LOG_INFO(log, "Listening https://" + address.toString());
#else
                    throw Exception{"HTTPS protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }

                /// TCP
                if (config().has("tcp_port"))
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("tcp_port"));
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new Poco::Net::TCPServer(
                        new TCPHandlerFactory(*this),
                        server_pool,
                        socket,
                        new Poco::Net::TCPServerParams));

                    LOG_INFO(log, "Listening tcp: " + address.toString());
                }

                /// TCP with SSL
                if (config().has("tcp_port_secure"))
                {
#if USE_POCO_NETSSL
                    initSSL();
                    Poco::Net::SecureServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("tcp_port_secure"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.receive_timeout);
                    socket.setSendTimeout(settings.send_timeout);
                    servers.emplace_back(new Poco::Net::TCPServer(
                        new TCPHandlerFactory(*this, /* secure= */ true ),
                                                                  server_pool,
                                                                  socket,
                                                                  new Poco::Net::TCPServerParams));
                    LOG_INFO(log, "Listening tcp_secure: " + address.toString());
#else
                    throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                        ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }

                /// At least one of TCP and HTTP servers must be created.
                if (servers.empty())
                    throw Exception("No 'tcp_port' and 'http_port' is specified in configuration file.", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

                /// Interserver IO HTTP
                if (config().has("interserver_http_port"))
                {
                    Poco::Net::ServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("interserver_http_port"));
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(new Poco::Net::HTTPServer(
                        new InterserverIOHTTPHandlerFactory(*this, "InterserverIOHTTPHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                    LOG_INFO(log, "Listening interserver http: " + address.toString());
                }

                if (config().has("interserver_https_port"))
                {
#if USE_POCO_NETSSL
                    initSSL();
                    Poco::Net::SecureServerSocket socket;
                    auto address = socket_bind_listen(socket, listen_host, config().getInt("interserver_https_port"), /* secure = */ true);
                    socket.setReceiveTimeout(settings.http_receive_timeout);
                    socket.setSendTimeout(settings.http_send_timeout);
                    servers.emplace_back(new Poco::Net::HTTPServer(
                        new InterserverIOHTTPHandlerFactory(*this, "InterserverIOHTTPHandler-factory"),
                        server_pool,
                        socket,
                        http_params));

                    LOG_INFO(log, "Listening interserver https: " + address.toString());
#else
                    throw Exception{"SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.",
                            ErrorCodes::SUPPORT_IS_DISABLED};
#endif
                }
            }
            catch (const Poco::Net::NetException & e)
            {
                if (listen_try)
                    LOG_ERROR(log, "Listen [" << listen_host << "]: " << e.code() << ": " << e.what() << ": " << e.message()
                        << "  If it is an IPv6 or IPv4 address and your host has disabled IPv6 or IPv4, then consider to "
                        "specify not disabled IPv4 or IPv6 address to listen in <listen_host> element of configuration "
                        "file. Example for disabled IPv6: <listen_host>0.0.0.0</listen_host> ."
                        " Example for disabled IPv4: <listen_host>::</listen_host>");
                else
                    throw;
            }
        }

        if (servers.empty())
             throw Exception("No servers started (add valid listen_host and 'tcp_port' or 'http_port' to configuration file.)", ErrorCodes::NO_ELEMENTS_IN_CONFIG);

        for (auto & server : servers)
            server->start();

        main_config_reloader->start();
        users_config_reloader->start();

        {
            std::stringstream message;
            message << "Available RAM = " << formatReadableSizeWithBinarySuffix(getMemoryAmount()) << ";"
                << " physical cores = " << getNumberOfPhysicalCPUCores() << ";"
                // on ARM processors it can show only enabled at current moment cores
                << " threads = " <<  std::thread::hardware_concurrency() << ".";
            LOG_INFO(log, message.str());
        }

        LOG_INFO(log, "Ready for connections.");

        SCOPE_EXIT({
            LOG_DEBUG(log, "Received termination signal.");
            LOG_DEBUG(log, "Waiting for current connections to close.");

            is_cancelled = true;

            int current_connections = 0;
            for (auto & server : servers)
            {
                server->stop();
                current_connections += server->currentConnections();
            }

            LOG_DEBUG(log,
                "Closed all listening sockets."
                    << (current_connections ? " Waiting for " + toString(current_connections) + " outstanding connections." : ""));

            if (current_connections)
            {
                const int sleep_max_ms = 1000 * config().getInt("shutdown_wait_unfinished", 5);
                const int sleep_one_ms = 100;
                int sleep_current_ms = 0;
                while (sleep_current_ms < sleep_max_ms)
                {
                    current_connections = 0;
                    for (auto & server : servers)
                        current_connections += server->currentConnections();
                    if (!current_connections)
                        break;
                    sleep_current_ms += sleep_one_ms;
                    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_one_ms));
                }
            }

            LOG_DEBUG(
                log, "Closed connections." << (current_connections ? " But " + toString(current_connections) + " remains."
                    " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>" : ""));

            main_config_reloader.reset();
            users_config_reloader.reset();
        });

        /// try to load dictionaries immediately, throw on error and die
        try
        {
            if (!config().getBool("dictionaries_lazy_load", true))
            {
                global_context->tryCreateEmbeddedDictionaries();
                global_context->tryCreateExternalDictionaries();
            }
        }
        catch (...)
        {
            LOG_ERROR(log, "Caught exception while loading dictionaries.");
            throw;
        }

        /// This object will periodically calculate some metrics.
        AsynchronousMetrics async_metrics(*global_context);
        attachSystemTablesAsync(*global_context->getDatabase("system"), async_metrics);

        std::vector<std::unique_ptr<MetricsTransmitter>> metrics_transmitters;
        for (const auto & graphite_key : DB::getMultipleKeysFromConfig(config(), "", "graphite"))
        {
            metrics_transmitters.emplace_back(std::make_unique<MetricsTransmitter>(
                *global_context, async_metrics, graphite_key));
        }

        SessionCleaner session_cleaner(*global_context);

        waitForTerminationRequest();
    }

    return Application::EXIT_OK;
}
}

int mainEntryClickHouseServer(int argc, char ** argv)
{
    DB::Server app;
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
