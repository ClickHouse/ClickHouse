#include "Server.h"

#include <memory>
#include <Interpreters/ClientInfo.h>
#include <sys/resource.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <pwd.h>
#include <unistd.h>
#include <Poco/Net/HTTPServer.h>
#include <Poco/Net/NetException.h>
#include <Poco/Util/HelpFormatter.h>
#include <Poco/Environment.h>
#include <Poco/Config.h>
#include <Common/scope_guard_safe.h>
#include <Common/logger_useful.h>
#include <base/phdr_cache.h>
#include <Common/ErrorHandlers.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <base/getMemoryAmount.h>
#include <base/getAvailableMemoryAmount.h>
#include <base/errnoToString.h>
#include <base/coverage.h>
#include <base/getFQDNOrHostName.h>
#include <base/safeExit.h>
#include <base/Numa.h>
#include <Common/PoolId.h>
#include <Common/MemoryTracker.h>
#include <Common/MemoryWorker.h>
#include <Common/ClickHouseRevision.h>
#include <Common/DNSResolver.h>
#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/CurrentMetrics.h>
#include <Common/ISlotControl.h>
#include <Common/Macros.h>
#include <Common/ShellCommand.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ZooKeeper/ZooKeeperNodeCache.h>
#include <Common/formatReadable.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/getExecutablePath.h>
#include <Common/ProfileEvents.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/ThreadProfileEvents.h>
#include <Common/ThreadStatus.h>
#include <Common/getMappedArea.h>
#include <Common/remapExecutable.h>
#include <Common/TLDListsHolder.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/assertProcessUserMatchesDataOwner.h>
#include <Common/makeSocketAddress.h>
#include <Common/FailPoint.h>
#include <Common/CPUID.h>
#include <Common/HTTPConnectionPool.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Server/waitServersToFinish.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/ServerSettings.h>
#include <Core/ServerUUID.h>
#include <Core/Settings.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/SharedThreadPools.h>
#include <Interpreters/CancellationChecker.h>
#include <Interpreters/ServerAsynchronousMetrics.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DNSCacheUpdater.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/executeQuery.h>
#include <Interpreters/loadMetadata.h>
#include <Interpreters/registerInterpreters.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/User.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachInformationSchemaTables.h>
#include <Storages/Cache/registerRemoteFileMetadatas.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <Formats/registerFormats.h>
#include <Storages/registerStorages.h>
#include <Databases/registerDatabases.h>
#include <Dictionaries/registerDictionaries.h>
#include <Disks/registerDisks.h>
#include <Common/Scheduler/Nodes/registerSchedulerNodes.h>
#include <Common/Scheduler/Workload/IWorkloadEntityStorage.h>
#include <Common/Config/ConfigReloader.h>
#include <Server/HTTPHandlerFactory.h>
#include <Common/ReplicasReconnector.h>
#include "MetricsTransmitter.h"
#include <Common/StatusFile.h>
#include <Server/TCPHandlerFactory.h>
#include <Server/TCPServer.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/ThreadFuzzer.h>
#include <Common/getHashOfLoadedBinary.h>
#include <Common/filesystemHelpers.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <Parsers/ASTAlterQuery.h>
#include <Server/HTTP/HTTPServerConnectionFactory.h>
#include <Server/MySQLHandlerFactory.h>
#include <Server/PostgreSQLHandlerFactory.h>
#include <Server/ProxyV1HandlerFactory.h>
#include <Server/TLSHandlerFactory.h>
#include <Server/ProtocolServerAdapter.h>
#include <Server/KeeperReadinessHandler.h>
#include <Server/HTTP/HTTPServer.h>
#include <Server/CloudPlacementInfo.h>
#include <Interpreters/AsynchronousInsertQueue.h>

#include <filesystem>
#include <unordered_set>

#include <Common/Jemalloc.h>

#include "config.h"
#include <Common/config_version.h>

#if defined(OS_LINUX)
#    include <cstdlib>
#    include <sys/un.h>
#    include <sys/mman.h>
#    include <sys/ptrace.h>
#    include <Common/hasLinuxCapability.h>
#endif

#if USE_SSL
#    include <Poco/Net/SecureServerSocket.h>
#    include <Server/CertificateReloader.h>
#    include <Server/SSH/SSHPtyHandlerFactory.h>
#    include <Common/LibSSHInitializer.h>
#    include <Common/LibSSHLogger.h>
#endif

#if USE_GRPC
#   include <Server/GRPCServer.h>
#endif

#if USE_NURAFT
#    include <Coordination/FourLetterCommand.h>
#    include <Server/KeeperTCPHandlerFactory.h>
#endif

#if USE_AZURE_BLOB_STORAGE
#   include <azure/storage/common/internal/xml_wrapper.hpp>
#   include <azure/core/diagnostics/logger.hpp>
#endif


#include <incbin.h>
/// A minimal file used when the server is run without installation
INCBIN(resource_embedded_xml, SOURCE_DIR "/programs/server/embedded.xml");

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsSeconds http_send_timeout;
    extern const SettingsSeconds receive_timeout;
    extern const SettingsSeconds send_timeout;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool allow_remote_fs_zero_copy_replication;
}

namespace ServerSetting
{
    extern const ServerSettingsUInt32 allow_feature_tier;
    extern const ServerSettingsUInt32 asynchronous_heavy_metrics_update_period_s;
    extern const ServerSettingsUInt32 asynchronous_metrics_update_period_s;
    extern const ServerSettingsBool asynchronous_metrics_enable_heavy_metrics;
    extern const ServerSettingsBool async_insert_queue_flush_on_shutdown;
    extern const ServerSettingsUInt64 async_insert_threads;
    extern const ServerSettingsBool async_load_databases;
    extern const ServerSettingsBool async_load_system_database;
    extern const ServerSettingsUInt64 background_buffer_flush_schedule_pool_size;
    extern const ServerSettingsUInt64 background_common_pool_size;
    extern const ServerSettingsUInt64 background_distributed_schedule_pool_size;
    extern const ServerSettingsUInt64 background_fetches_pool_size;
    extern const ServerSettingsFloat background_merges_mutations_concurrency_ratio;
    extern const ServerSettingsString background_merges_mutations_scheduling_policy;
    extern const ServerSettingsUInt64 background_message_broker_schedule_pool_size;
    extern const ServerSettingsUInt64 background_move_pool_size;
    extern const ServerSettingsUInt64 background_pool_size;
    extern const ServerSettingsUInt64 background_schedule_pool_size;
    extern const ServerSettingsUInt64 backups_io_thread_pool_queue_size;
    extern const ServerSettingsDouble cache_size_to_ram_max_ratio;
    extern const ServerSettingsDouble cannot_allocate_thread_fault_injection_probability;
    extern const ServerSettingsUInt64 cgroups_memory_usage_observer_wait_time;
    extern const ServerSettingsUInt64 compiled_expression_cache_elements_size;
    extern const ServerSettingsUInt64 compiled_expression_cache_size;
    extern const ServerSettingsUInt64 concurrent_threads_soft_limit_num;
    extern const ServerSettingsUInt64 concurrent_threads_soft_limit_ratio_to_cores;
    extern const ServerSettingsString concurrent_threads_scheduler;
    extern const ServerSettingsUInt64 config_reload_interval_ms;
    extern const ServerSettingsUInt64 database_catalog_drop_table_concurrency;
    extern const ServerSettingsString default_database;
    extern const ServerSettingsBool disable_internal_dns_cache;
    extern const ServerSettingsBool s3queue_disable_streaming;
    extern const ServerSettingsUInt64 disk_connections_soft_limit;
    extern const ServerSettingsUInt64 disk_connections_store_limit;
    extern const ServerSettingsUInt64 disk_connections_warn_limit;
    extern const ServerSettingsBool dns_allow_resolve_names_to_ipv4;
    extern const ServerSettingsBool dns_allow_resolve_names_to_ipv6;
    extern const ServerSettingsUInt64 dns_cache_max_entries;
    extern const ServerSettingsInt32 dns_cache_update_period;
    extern const ServerSettingsUInt32 dns_max_consecutive_failures;
    extern const ServerSettingsBool enable_azure_sdk_logging;
    extern const ServerSettingsUInt64 global_profiler_cpu_time_period_ns;
    extern const ServerSettingsUInt64 global_profiler_real_time_period_ns;
    extern const ServerSettingsUInt64 http_connections_soft_limit;
    extern const ServerSettingsUInt64 http_connections_store_limit;
    extern const ServerSettingsUInt64 http_connections_warn_limit;
    extern const ServerSettingsString index_mark_cache_policy;
    extern const ServerSettingsUInt64 index_mark_cache_size;
    extern const ServerSettingsDouble index_mark_cache_size_ratio;
    extern const ServerSettingsString vector_similarity_index_cache_policy;
    extern const ServerSettingsUInt64 vector_similarity_index_cache_size;
    extern const ServerSettingsUInt64 vector_similarity_index_cache_max_entries;
    extern const ServerSettingsDouble vector_similarity_index_cache_size_ratio;
    extern const ServerSettingsString index_uncompressed_cache_policy;
    extern const ServerSettingsUInt64 index_uncompressed_cache_size;
    extern const ServerSettingsDouble index_uncompressed_cache_size_ratio;
    extern const ServerSettingsString iceberg_metadata_files_cache_policy;
    extern const ServerSettingsUInt64 iceberg_metadata_files_cache_size;
    extern const ServerSettingsUInt64 iceberg_metadata_files_cache_max_entries;
    extern const ServerSettingsDouble iceberg_metadata_files_cache_size_ratio;
    extern const ServerSettingsUInt64 io_thread_pool_queue_size;
    extern const ServerSettingsSeconds keep_alive_timeout;
    extern const ServerSettingsString mark_cache_policy;
    extern const ServerSettingsUInt64 mark_cache_size;
    extern const ServerSettingsDouble mark_cache_size_ratio;
    extern const ServerSettingsUInt64 max_fetch_partition_thread_pool_size;
    extern const ServerSettingsUInt64 max_active_parts_loading_thread_pool_size;
    extern const ServerSettingsUInt64 max_backups_io_thread_pool_free_size;
    extern const ServerSettingsUInt64 max_backups_io_thread_pool_size;
    extern const ServerSettingsUInt64 max_concurrent_insert_queries;
    extern const ServerSettingsUInt64 max_concurrent_queries;
    extern const ServerSettingsUInt64 max_concurrent_select_queries;
    extern const ServerSettingsInt32 max_connections;
    extern const ServerSettingsUInt64 max_database_num_to_warn;
    extern const ServerSettingsUInt32 max_database_replicated_create_table_thread_pool_size;
    extern const ServerSettingsUInt64 max_dictionary_num_to_warn;
    extern const ServerSettingsUInt64 max_io_thread_pool_free_size;
    extern const ServerSettingsUInt64 max_io_thread_pool_size;
    extern const ServerSettingsUInt64 max_keep_alive_requests;
    extern const ServerSettingsUInt64 max_outdated_parts_loading_thread_pool_size;
    extern const ServerSettingsUInt64 max_partition_size_to_drop;
    extern const ServerSettingsUInt64 max_part_num_to_warn;
    extern const ServerSettingsUInt64 max_pending_mutations_to_warn;
    extern const ServerSettingsUInt64 max_pending_mutations_execution_time_to_warn;
    extern const ServerSettingsUInt64 max_parts_cleaning_thread_pool_size;
    extern const ServerSettingsUInt64 max_remote_read_network_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_remote_write_network_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_local_read_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_local_write_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_server_memory_usage;
    extern const ServerSettingsDouble max_server_memory_usage_to_ram_ratio;
    extern const ServerSettingsUInt64 max_table_num_to_warn;
    extern const ServerSettingsUInt64 max_table_size_to_drop;
    extern const ServerSettingsUInt64 max_temporary_data_on_disk_size;
    extern const ServerSettingsUInt64 max_thread_pool_free_size;
    extern const ServerSettingsUInt64 max_thread_pool_size;
    extern const ServerSettingsUInt64 max_unexpected_parts_loading_thread_pool_size;
    extern const ServerSettingsUInt64 max_view_num_to_warn;
    extern const ServerSettingsUInt64 max_waiting_queries;
    extern const ServerSettingsUInt64 memory_worker_period_ms;
    extern const ServerSettingsBool memory_worker_correct_memory_tracker;
    extern const ServerSettingsBool memory_worker_use_cgroup;
    extern const ServerSettingsUInt64 merges_mutations_memory_usage_soft_limit;
    extern const ServerSettingsDouble merges_mutations_memory_usage_to_ram_ratio;
    extern const ServerSettingsString merge_workload;
    extern const ServerSettingsUInt64 mmap_cache_size;
    extern const ServerSettingsString mutation_workload;
    extern const ServerSettingsString query_condition_cache_policy;
    extern const ServerSettingsUInt64 query_condition_cache_size;
    extern const ServerSettingsDouble query_condition_cache_size_ratio;
    extern const ServerSettingsBool prepare_system_log_tables_on_startup;
    extern const ServerSettingsBool show_addresses_in_stack_traces;
    extern const ServerSettingsBool shutdown_wait_backups_and_restores;
    extern const ServerSettingsUInt64 shutdown_wait_unfinished;
    extern const ServerSettingsBool shutdown_wait_unfinished_queries;
    extern const ServerSettingsUInt64 storage_connections_soft_limit;
    extern const ServerSettingsUInt64 storage_connections_store_limit;
    extern const ServerSettingsUInt64 storage_connections_warn_limit;
    extern const ServerSettingsUInt64 tables_loader_background_pool_size;
    extern const ServerSettingsUInt64 tables_loader_foreground_pool_size;
    extern const ServerSettingsString temporary_data_in_cache;
    extern const ServerSettingsUInt64 thread_pool_queue_size;
    extern const ServerSettingsString tmp_policy;
    extern const ServerSettingsUInt64 total_memory_profiler_sample_max_allocation_size;
    extern const ServerSettingsUInt64 total_memory_profiler_sample_min_allocation_size;
    extern const ServerSettingsUInt64 total_memory_profiler_step;
    extern const ServerSettingsDouble total_memory_tracker_sample_probability;
    extern const ServerSettingsBool throw_on_unknown_workload;
    extern const ServerSettingsString uncompressed_cache_policy;
    extern const ServerSettingsUInt64 uncompressed_cache_size;
    extern const ServerSettingsDouble uncompressed_cache_size_ratio;
    extern const ServerSettingsString primary_index_cache_policy;
    extern const ServerSettingsUInt64 primary_index_cache_size;
    extern const ServerSettingsDouble primary_index_cache_size_ratio;
    extern const ServerSettingsBool dictionaries_lazy_load;
    extern const ServerSettingsBool wait_dictionaries_load_at_startup;
    extern const ServerSettingsUInt64 max_prefixes_deserialization_thread_pool_size;
    extern const ServerSettingsUInt64 max_prefixes_deserialization_thread_pool_free_size;
    extern const ServerSettingsUInt64 prefixes_deserialization_thread_pool_thread_pool_queue_size;
    extern const ServerSettingsUInt64 max_format_parsing_thread_pool_size;
    extern const ServerSettingsUInt64 max_format_parsing_thread_pool_free_size;
    extern const ServerSettingsUInt64 format_parsing_thread_pool_queue_size;
    extern const ServerSettingsUInt64 page_cache_history_window_ms;
    extern const ServerSettingsString page_cache_policy;
    extern const ServerSettingsDouble page_cache_size_ratio;
    extern const ServerSettingsUInt64 page_cache_min_size;
    extern const ServerSettingsUInt64 page_cache_max_size;
    extern const ServerSettingsDouble page_cache_free_memory_ratio;
    extern const ServerSettingsUInt64 page_cache_shards;
    extern const ServerSettingsUInt64 os_cpu_busy_time_threshold;
    extern const ServerSettingsFloat min_os_cpu_wait_time_ratio_to_drop_connection;
    extern const ServerSettingsFloat max_os_cpu_wait_time_ratio_to_drop_connection;
}

namespace ErrorCodes
{
    extern const int STARTUP_SCRIPTS_ERROR;
}

}

namespace CurrentMetrics
{
    extern const Metric Revision;
    extern const Metric VersionInteger;
    extern const Metric MemoryTracking;
    extern const Metric MergesMutationsMemoryTracking;
    extern const Metric MaxDDLEntryID;
    extern const Metric MaxPushedDDLEntryID;
    extern const Metric StartupScriptsExecutionState;
    extern const Metric IsServerShuttingDown;
}

namespace ProfileEvents
{
    extern const Event MainConfigLoads;
    extern const Event ServerStartupMilliseconds;
    extern const Event InterfaceNativeSendBytes;
    extern const Event InterfaceNativeReceiveBytes;
    extern const Event InterfaceHTTPSendBytes;
    extern const Event InterfaceHTTPReceiveBytes;
    extern const Event InterfacePrometheusSendBytes;
    extern const Event InterfacePrometheusReceiveBytes;
    extern const Event InterfaceInterserverSendBytes;
    extern const Event InterfaceInterserverReceiveBytes;
    extern const Event InterfaceMySQLSendBytes;
    extern const Event InterfaceMySQLReceiveBytes;
    extern const Event InterfacePostgreSQLSendBytes;
    extern const Event InterfacePostgreSQLReceiveBytes;
}

namespace fs = std::filesystem;

int mainEntryClickHouseServer(int argc, char ** argv)
{
    DB::Server app;

    /// Do not fork separate process from watchdog if we attached to terminal.
    /// Otherwise it breaks gdb usage.
    /// Can be overridden by environment variable (cannot use server config at this moment).
    if (argc > 0)
    {
        const char * env_watchdog = getenv("CLICKHOUSE_WATCHDOG_ENABLE"); // NOLINT(concurrency-mt-unsafe)
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
        return static_cast<UInt8>(code) ? code : 1;
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
    extern const int NETWORK_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int BAD_ARGUMENTS;
}


enum StartupScriptsExecutionState : CurrentMetrics::Value
{
    NotFinished = 0,
    Success = 1,
    Failure = 2,
};


static std::string getCanonicalPath(std::string && path)
{
    Poco::trimInPlace(path);
    if (path.empty())
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "path configuration parameter is empty");
    if (path.back() != '/')
        path += '/';
    return std::move(path);
}

static constexpr unsigned DEFAULT_LISTEN_BACKLOG = 4096;

static Poco::Net::TCPServerParams::Ptr makeServerParams(const Poco::Util::AbstractConfiguration & config)
{
    Poco::Net::TCPServerParams::Ptr params = new Poco::Net::TCPServerParams();
    params->setMaxQueued(config.getUInt("listen_backlog", DEFAULT_LISTEN_BACKLOG));
    return params;
}

Poco::Net::SocketAddress Server::socketBindListen(
    const Poco::Util::AbstractConfiguration & config,
    Poco::Net::ServerSocket & socket,
    const std::string & host,
    UInt16 port,
    [[maybe_unused]] bool secure) const
{
    auto address = makeSocketAddress(host, port, &logger());
    socket.bind(address, /* reuseAddress = */ true, /* reusePort = */ config.getBool("listen_reuse_port", false));
    /// If caller requests any available port from the OS, discover it after binding.
    if (port == 0)
    {
        address = socket.address();
        LOG_DEBUG(&logger(), "Requested any available port (port == 0), actual port is {:d}", address.port());
    }

    socket.listen(/* backlog = */ config.getUInt("listen_backlog", DEFAULT_LISTEN_BACKLOG));

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
    {
        Poco::Util::AbstractConfiguration::Keys protocols;
        config.keys("protocols", protocols);
        listen_try =
            DB::getMultipleValuesFromConfig(config, "", "listen_host").empty() &&
            std::none_of(protocols.begin(), protocols.end(), [&](const auto & protocol)
            {
                return config.has("protocols." + protocol + ".host") && config.has("protocols." + protocol + ".port");
            });
    }
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


#if defined(OS_LINUX)
namespace
{

void setOOMScore(int value, LoggerRawPtr log)
{
    try
    {
        std::string value_string = std::to_string(value);
        DB::WriteBufferFromFile buf("/proc/self/oom_score_adj");
        buf.write(value_string.c_str(), value_string.size());
        buf.next();
        buf.close();
    }
    catch (const Poco::Exception & e)
    {
        LOG_WARNING(log, "Failed to adjust OOM score: '{}'.", e.displayText());
        return;
    }
    LOG_INFO(log, "Set OOM score adjustment to {}", value);
}

}
#endif


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
        std::cout << VERSION_NAME << " server version " << VERSION_STRING << VERSION_OFFICIAL << "." << std::endl;
        return 0;
    }
    return Application::run(); // NOLINT
}

void Server::initialize(Poco::Util::Application & self)
{
    ConfigProcessor::registerEmbeddedConfig("config.xml", std::string_view(reinterpret_cast<const char *>(gresource_embedded_xmlData), gresource_embedded_xmlSize));
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
    LoggerPtr log)
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

namespace
{

/// Unused in other builds
#if defined(OS_LINUX)
String readLine(const String & path)
{
    ReadBufferFromFile in(path);
    String contents;
    readStringUntilNewlineInto(contents, in);
    return contents;
}

int readNumber(const String & path)
{
    ReadBufferFromFile in(path);
    int result;
    readText(result, in);
    return result;
}

#endif

void sanityChecks(Server & server)
{
    std::string data_path = getCanonicalPath(server.config().getString("path", DBMS_DEFAULT_PATH));
    std::string logs_path = server.config().getString("logger.log", "");

    if (server.logger().is(Poco::Message::PRIO_TEST))
        server.context()->addOrUpdateWarningMessage(
            Context::WarningType::SERVER_LOGGING_LEVEL_TEST,
            PreformattedMessage::create(
                "Server logging level is set to 'test' and performance is degraded. This cannot be used in production."));
#if defined(OS_LINUX)
    try
    {
        const std::unordered_set<std::string> fast_clock_sources = {
            // ARM clock
            "arch_sys_counter",
            // KVM guest clock
            "kvm-clock",
            // X86 clock
            "tsc",
        };
        const char * filename = "/sys/devices/system/clocksource/clocksource0/current_clocksource";
        if (!fast_clock_sources.contains(readLine(filename)))
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_FAST_CLOCK_SOURCE_NOT_USED,
                PreformattedMessage::create("Linux is not using a fast clock source. Performance can be degraded. Check {}", filename));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/vm/overcommit_memory";
        if (readNumber(filename) == 2)
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MEMORY_OVERCOMMIT_DISABLED,
                PreformattedMessage::create("Linux memory overcommit is disabled. Check {}", String(filename)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/sys/kernel/mm/transparent_hugepage/enabled";
        if (readLine(filename).find("[always]") != std::string::npos)
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_TRANSPARENT_HUGEPAGES_SET_TO_ALWAYS,
                PreformattedMessage::create("Linux transparent hugepages are set to \"always\". Check {}", String(filename)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/pid_max";
        if (readNumber(filename) < 30000)
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MAX_PID_TOO_LOW,
               PreformattedMessage::create("Linux max PID is too low. Check {}", String(filename)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/threads-max";
        if (readNumber(filename) < 30000)
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::LINUX_MAX_THREADS_COUNT_TOO_LOW,
                PreformattedMessage::create("Linux threads max count is too low. Check {}", String(filename)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        const char * filename = "/proc/sys/kernel/task_delayacct";
        if (readNumber(filename) == 0)
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::DELAY_ACCOUNTING_DISABLED,
                PreformattedMessage::create(
                    "Delay accounting is not enabled, OSIOWaitMicroseconds will not be gathered. You can enable it "
                    "using `echo 1 > {}` or by using sysctl.",
                    String(filename)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    std::string dev_id = getBlockDeviceId(data_path);
    if (getBlockDeviceType(dev_id) == BlockDeviceType::ROT && getBlockDeviceReadAheadBytes(dev_id) == 0)
        server.context()->addOrUpdateWarningMessage(
            Context::WarningType::ROTATIONAL_DISK_WITH_DISABLED_READHEAD,
            PreformattedMessage::create(
                "Rotational disk with disabled readahead is in use. Performance can be degraded. Used for data: {}", String(data_path)));
#endif

    try
    {
        if (getAvailableMemoryAmount() < (2l << 30))
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::AVAILABLE_MEMORY_TOO_LOW,
                PreformattedMessage::create("Available memory at server startup is too low (2GiB)."));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        if (!enoughSpaceInDirectory(data_path, 1ull << 30))
            server.context()->addOrUpdateWarningMessage(
                Context::WarningType::AVAILABLE_DISK_SPACE_TOO_LOW_FOR_DATA,
                PreformattedMessage::create("Available disk space for data at server startup is too low (1GiB): {}", String(data_path)));
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    try
    {
        if (!logs_path.empty() && fs::is_regular_file(logs_path))
        {
            auto logs_parent = fs::path(logs_path).parent_path();
            if (!enoughSpaceInDirectory(logs_parent, 1ull << 30))
                server.context()->addOrUpdateWarningMessage(
                    Context::WarningType::AVAILABLE_DISK_SPACE_TOO_LOW_FOR_LOGS,
                    PreformattedMessage::create("Available disk space for logs at server startup is too low (1GiB): {}", String(logs_parent)));
        }
    }
    catch (...) // NOLINT(bugprone-empty-catch)
    {
    }

    if (server.context()->getMergeTreeSettings()[MergeTreeSetting::allow_remote_fs_zero_copy_replication])
    {
        constexpr auto message_format_string
            = "The setting 'allow_remote_fs_zero_copy_replication' is enabled for MergeTree tables."
              " But the feature of 'zero-copy replication' is under development and is not ready for production."
              " The usage of this feature can lead to data corruption and loss. The setting should be disabled in production.";
        server.context()->addOrUpdateWarningMessage(
            Context::WarningType::SETTING_ZERO_COPY_REPLICATION_ENABLED,
            PreformattedMessage::create(message_format_string));
    }
}

}

void loadStartupScripts(const Poco::Util::AbstractConfiguration & config, ContextMutablePtr context, Poco::Logger * log)
{
    try
    {
        Poco::Util::AbstractConfiguration::Keys keys;
        config.keys("startup_scripts", keys);

        SetResultDetailsFunc callback;
        std::vector<String> skipped_startup_scripts;

        for (const auto & key : keys)
        {
            if (key == "throw_on_error")
                continue;
            std::string full_prefix = "startup_scripts." + key;

            auto user = config.getString(full_prefix + ".user", "");
            auto startup_context = Context::createCopy(context);

            if (!user.empty())
            {
                auto & access_control = startup_context->getAccessControl();
                startup_context->setUser(access_control.getID<User>(user));
            }

            startup_context->makeQueryContext();

            if (config.has(full_prefix + ".condition"))
            {
                auto condition = config.getString(full_prefix + ".condition");
                auto condition_read_buffer = ReadBufferFromString(condition);
                auto condition_write_buffer = WriteBufferFromOwnString();

                LOG_DEBUG(log, "Checking startup query condition `{}`", condition);
                startup_context->setQueryKind(ClientInfo::QueryKind::INITIAL_QUERY);
                executeQuery(condition_read_buffer, condition_write_buffer, true, startup_context, callback, QueryFlags{ .internal = true }, std::nullopt, {});

                auto result = condition_write_buffer.str();
                if (result != "1\n" && result != "true\n")
                {
                    if (result != "0\n" && result != "false\n")
                    {
                        if (result.empty())
                            LOG_DEBUG(log, "Skipping startup script as condition query returned empty value.");
                        else
                            LOG_DEBUG(
                                log,
                                "Skipping startup script as condition query returned value `{}` "
                                "which can't be interpreted as a boolean (`0`, `false`, `1`, `true`).",
                                result);
                        skipped_startup_scripts.emplace_back(full_prefix);
                    }
                    continue;
                }

                LOG_DEBUG(log, "Condition is true, will execute the query next");
            }

            auto query = config.getString(full_prefix + ".query");
            auto read_buffer = ReadBufferFromString(query);
            auto write_buffer = WriteBufferFromOwnString();

            LOG_DEBUG(log, "Executing query `{}`", query);
            startup_context->setQueryKind(ClientInfo::QueryKind::INITIAL_QUERY);
            executeQuery(read_buffer, write_buffer, true, startup_context, callback, QueryFlags{ .internal = true }, std::nullopt, {});
        }

        if (!skipped_startup_scripts.empty())
        {
            context->addOrUpdateWarningMessage(
                Context::WarningType::SKIPPING_CONDITION_QUERY,
                PreformattedMessage::create(
                    "Skipped the following startup script(s): {} as the condition query for those returned values, "
                    "which can't be interpreted as a boolean (`0`, `false`, `1`, `true`).",
                    fmt::join(skipped_startup_scripts, ", ")));
        }

        CurrentMetrics::set(CurrentMetrics::StartupScriptsExecutionState, StartupScriptsExecutionState::Success);
    }
    catch (...)
    {
        CurrentMetrics::set(CurrentMetrics::StartupScriptsExecutionState, StartupScriptsExecutionState::Failure);
        tryLogCurrentException(log, "Failed to parse startup scripts file");
        if (config.getBool("startup_scripts.throw_on_error", false))
            throw Exception(
                ErrorCodes::STARTUP_SCRIPTS_ERROR,
                "Cannot finish startup_script successfully. Use startup_scripts.throw_on_error setting to change this behavior");
    }
}

static void initializeAzureSDKLogger(
    [[ maybe_unused ]] const ServerSettings & server_settings,
    [[ maybe_unused ]] int server_logs_level)
{
#if USE_AZURE_BLOB_STORAGE
    if (!server_settings[ServerSetting::enable_azure_sdk_logging])
        return;

    using AzureLogsLevel = Azure::Core::Diagnostics::Logger::Level;

    static const std::unordered_map<AzureLogsLevel, std::pair<Poco::Message::Priority, DB::LogsLevel>> azure_to_server_mapping =
    {
        {AzureLogsLevel::Error, {Poco::Message::PRIO_DEBUG, LogsLevel::debug}},
        {AzureLogsLevel::Warning, {Poco::Message::PRIO_DEBUG, LogsLevel::debug}},
        {AzureLogsLevel::Informational, {Poco::Message::PRIO_TRACE, LogsLevel::trace}},
        {AzureLogsLevel::Verbose, {Poco::Message::PRIO_TEST, LogsLevel::test}},
    };

    static const std::map<Poco::Message::Priority, AzureLogsLevel> server_to_azure_mapping =
    {
        {Poco::Message::PRIO_DEBUG, AzureLogsLevel::Warning},
        {Poco::Message::PRIO_TRACE, AzureLogsLevel::Informational},
        {Poco::Message::PRIO_TEST, AzureLogsLevel::Verbose},
    };

    static const LoggerPtr azure_sdk_logger = getLogger("AzureSDK");

    auto it = server_to_azure_mapping.lower_bound(static_cast<Poco::Message::Priority>(server_logs_level));
    chassert(it != server_to_azure_mapping.end());
    Azure::Core::Diagnostics::Logger::SetLevel(it->second);

    Azure::Core::Diagnostics::Logger::SetListener([](AzureLogsLevel level, const std::string & message)
    {
        auto [poco_level, db_level] = azure_to_server_mapping.at(level);
        LOG_IMPL(azure_sdk_logger, db_level, poco_level, fmt::runtime(message));
    });
#endif
}

#if defined(SANITIZER)
static std::vector<String> getSanitizerNames()
{
    std::vector<String> names;

#if defined(ADDRESS_SANITIZER)
    names.push_back("address");
#endif
#if defined(THREAD_SANITIZER)
    names.push_back("thread");
#endif
#if defined(MEMORY_SANITIZER)
    names.push_back("memory");
#endif
#if defined(UNDEFINED_BEHAVIOR_SANITIZER)
    names.push_back("undefined behavior");
#endif

    return names;
}
#endif

int Server::main(const std::vector<std::string> & /*args*/)
try
{
#if USE_JEMALLOC
    setJemallocBackgroundThreads(true);
#endif

#if USE_SSL
    ::ssh::LibSSHInitializer::instance();
    ::ssh::libsshLogger::initialize();
#endif

    Stopwatch startup_watch;

    Poco::Logger * log = &logger();

    MainThreadStatus::getInstance();

    ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config());

    StackTrace::setShowAddresses(server_settings[ServerSetting::show_addresses_in_stack_traces]);

#if USE_HDFS
    /// This will point libhdfs3 to the right location for its config.
    /// Note: this has to be done once at server initialization, because 'setenv' is not thread-safe.

    String libhdfs3_conf = config().getString("hdfs.libhdfs3_conf", "");
    if (!libhdfs3_conf.empty())
    {
        if (std::filesystem::path{libhdfs3_conf}.is_relative() && !std::filesystem::exists(libhdfs3_conf))
        {
            const String config_path = config().getString("config-file", "config.xml");
            const auto config_dir = std::filesystem::path{config_path}.remove_filename();
            if (std::filesystem::exists(config_dir / libhdfs3_conf))
                libhdfs3_conf = std::filesystem::absolute(config_dir / libhdfs3_conf);
        }
        setenv("LIBHDFS3_CONF", libhdfs3_conf.c_str(), true /* overwrite */); // NOLINT
    }
#endif

    if (auto total_numa_memory = getNumaNodesTotalMemory(); total_numa_memory.has_value())
    {
        LOG_INFO(
            log, "ClickHouse is bound to a subset of NUMA nodes. Total memory of all available nodes: {}", ReadableSize(*total_numa_memory));
    }

    registerInterpreters();
    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerDatabases();
    registerStorages();
    registerDictionaries();
    registerDisks(/* global_skip_access_check= */ false);
    registerFormats();
    registerRemoteFileMetadatas();
    registerSchedulerNodes();

    QueryPlanStepRegistry::registerPlanSteps();

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
    global_context->addOrUpdateWarningMessage(Context::WarningType::SERVER_BUILT_IN_DEBUG_MODE, PreformattedMessage::create("Server was built in debug mode. It will work slowly."));
#endif

    if (ThreadFuzzer::instance().isEffective())
        global_context->addOrUpdateWarningMessage(
            Context::WarningType::THREAD_FUZZER_IS_ENABLED,
            PreformattedMessage::create("ThreadFuzzer is enabled. Application will run slowly and unstable."));

#if defined(SANITIZER)
    auto sanitizers = getSanitizerNames();

    String log_message;
    if (sanitizers.empty())
        log_message = "sanitizer";
    else if (sanitizers.size() == 1)
        log_message = fmt::format("{} sanitizer", sanitizers.front());
    else
        log_message = fmt::format("sanitizers ({})", fmt::join(sanitizers, ", "));

    global_context->addOrUpdateWarningMessage(
        Context::WarningType::SERVER_BUILT_WITH_SANITIZERS,
        PreformattedMessage::create("Server was built with {}. It will work slowly.", log_message));
#endif

#if defined(SANITIZE_COVERAGE) || WITH_COVERAGE
    global_context->addOrUpdateWarningMessage(
        Context::WarningType::SERVER_BUILT_WITH_COVERAGE,
        PreformattedMessage::create("Server was built with code coverage. It will work slowly."));
#endif

    const size_t physical_server_memory = getMemoryAmount();

    LOG_INFO(
        log,
        "Available RAM: {}; logical cores: {}; used cores: {}.",
        formatReadableSizeWithBinarySuffix(physical_server_memory),
        std::thread::hardware_concurrency(),
        getNumberOfCPUCoresToUse() // on ARM processors it can show only enabled at current moment cores
    );

#if defined(__x86_64__)
    String cpu_info;
#define COLLECT_FLAG(X) \
    if (CPU::have##X()) \
    {                   \
        if (!cpu_info.empty()) \
            cpu_info += ", ";  \
        cpu_info += #X; \
    }

    CPU_ID_ENUMERATE(COLLECT_FLAG)
#undef COLLECT_FLAG

    LOG_INFO(log, "Available CPU instruction sets: {}", cpu_info);
#endif

    bool has_trace_collector = false;
    /// Disable it if we collect test coverage information, because it will work extremely slow.
#if !WITH_COVERAGE
    /// Profilers cannot work reliably with any other libunwind or without PHDR cache.
    has_trace_collector = hasPHDRCache() && config().has("trace_log");
#endif

    /// Describe multiple reasons when query profiler cannot work.

#if WITH_COVERAGE
    LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they work extremely slow with test coverage.");
#endif

#if defined(SANITIZER)
    LOG_INFO(log, "Query Profiler is disabled because it cannot work under sanitizers"
        " when two different stack unwinding methods will interfere with each other.");
#endif

    if (!hasPHDRCache())
        LOG_INFO(log, "Query Profiler and TraceCollector are disabled because they require PHDR cache to be created"
            " (otherwise the function 'dl_iterate_phdr' is not lock free and not async-signal safe).");

    // Settings validation for page cache. Ensure that page_cache_max_size is > page_cache_min_size.
    // Otherwise, crash might happen during cache resizing in src/Common/PageCache.cpp::autoResize
    size_t page_cache_min_size = server_settings[ServerSetting::page_cache_min_size];
    size_t page_cache_max_size = server_settings[ServerSetting::page_cache_max_size];
    if (page_cache_max_size != 0 && (page_cache_min_size > page_cache_max_size))
    {
        throw Exception(
            ErrorCodes::INVALID_CONFIG_PARAMETER,
            "Invalid page cache configuration: page_cache_min_size ({}) is greater than page_cache_max_size ({}).",
            page_cache_min_size,
            page_cache_max_size);
    }

    // Initialize global thread pool. Do it before we fetch configs from zookeeper
    // nodes (`from_zk`), because ZooKeeper interface uses the pool. We will
    // ignore `max_thread_pool_size` in configs we fetch from ZK, but oh well.
    GlobalThreadPool::initialize(
        server_settings[ServerSetting::max_thread_pool_size],
        server_settings[ServerSetting::max_thread_pool_free_size],
        server_settings[ServerSetting::thread_pool_queue_size],
        has_trace_collector ? server_settings[ServerSetting::global_profiler_real_time_period_ns] : 0,
        has_trace_collector ? server_settings[ServerSetting::global_profiler_cpu_time_period_ns] : 0);

    if (has_trace_collector)
    {
        global_context->createTraceCollector();

        /// Set up server-wide memory profiler (for total memory tracker).
        if (server_settings[ServerSetting::total_memory_profiler_step])
            total_memory_tracker.setProfilerStep(server_settings[ServerSetting::total_memory_profiler_step]);

        if (server_settings[ServerSetting::total_memory_tracker_sample_probability] > 0.0)
            total_memory_tracker.setSampleProbability(server_settings[ServerSetting::total_memory_tracker_sample_probability]);

        if (server_settings[ServerSetting::total_memory_profiler_sample_min_allocation_size])
            total_memory_tracker.setSampleMinAllocationSize(server_settings[ServerSetting::total_memory_profiler_sample_min_allocation_size]);

        if (server_settings[ServerSetting::total_memory_profiler_sample_max_allocation_size])
            total_memory_tracker.setSampleMaxAllocationSize(server_settings[ServerSetting::total_memory_profiler_sample_max_allocation_size]);
    }

    Poco::ThreadPool server_pool(
        /* minCapacity */3,
        /* maxCapacity */server_settings[ServerSetting::max_connections],
        /* idleTime */60,
        /* stackSize */POCO_THREAD_STACK_SIZE,
        server_settings[ServerSetting::global_profiler_real_time_period_ns],
        server_settings[ServerSetting::global_profiler_cpu_time_period_ns]);

    std::mutex servers_lock;
    std::vector<ProtocolServerAdapter> servers;
    std::vector<ProtocolServerAdapter> servers_to_start_before_tables;

    /// Wait for all threads to avoid possible use-after-free (for example logging objects can be already destroyed).
    SCOPE_EXIT({
        Stopwatch watch;
        LOG_INFO(log, "Waiting for background threads");
        GlobalThreadPool::instance().shutdown();
        LOG_INFO(log, "Background threads finished in {} ms", watch.elapsedMilliseconds());
    });

    if (page_cache_max_size != 0)
    {
        global_context->setPageCache(
            std::chrono::milliseconds(Int64(server_settings[ServerSetting::page_cache_history_window_ms])),
            server_settings[ServerSetting::page_cache_policy],
            server_settings[ServerSetting::page_cache_size_ratio],
            server_settings[ServerSetting::page_cache_min_size],
            server_settings[ServerSetting::page_cache_max_size],
            server_settings[ServerSetting::page_cache_free_memory_ratio],
            server_settings[ServerSetting::page_cache_shards]);
        total_memory_tracker.setPageCache(global_context->getPageCache().get());
    }

    MemoryWorker memory_worker(
        server_settings[ServerSetting::memory_worker_period_ms],
        server_settings[ServerSetting::memory_worker_correct_memory_tracker],
        global_context->getServerSettings()[ServerSetting::memory_worker_use_cgroup],
        global_context->getPageCache());

    /// This object will periodically calculate some metrics.
    ServerAsynchronousMetrics async_metrics(
        global_context,
        server_settings[ServerSetting::asynchronous_metrics_update_period_s],
        server_settings[ServerSetting::asynchronous_metrics_enable_heavy_metrics],
        server_settings[ServerSetting::asynchronous_heavy_metrics_update_period_s],
        [&]() -> std::vector<ProtocolServerMetrics>
        {
            std::vector<ProtocolServerMetrics> metrics;

            std::lock_guard lock(servers_lock);
            metrics.reserve(servers_to_start_before_tables.size() + servers.size());

            for (const auto & server : servers_to_start_before_tables)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads(), server.refusedConnections()});

            for (const auto & server : servers)
                metrics.emplace_back(ProtocolServerMetrics{server.getPortName(), server.currentThreads(), server.refusedConnections()});
            return metrics;
        },
        /*update_jemalloc_epoch_=*/memory_worker.getSource() != MemoryWorker::MemoryUsageSource::Jemalloc,
        /*update_rss_=*/memory_worker.getSource() == MemoryWorker::MemoryUsageSource::None);

    /// NOTE: global context should be destroyed *before* GlobalThreadPool::shutdown()
    /// Otherwise GlobalThreadPool::shutdown() will hang, since Context holds some threads.
    SCOPE_EXIT({
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
            size_t current_connections = 0;
            {
                std::lock_guard lock(servers_lock);
                for (auto & server : servers_to_start_before_tables)
                {
                    server.stop();
                    current_connections += server.currentConnections();
                }
            }

            if (current_connections)
                LOG_INFO(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            if (current_connections > 0)
                current_connections = waitServersToFinish(servers_to_start_before_tables, servers_lock, server_settings[ServerSetting::shutdown_wait_unfinished]);

            if (current_connections)
                LOG_INFO(log, "Closed connections to servers for tables. But {} remain. Probably some tables of other users cannot finish their connections after context shutdown.", current_connections);
            else
                LOG_INFO(log, "Closed connections to servers for tables.");
        }

        global_context->shutdownKeeperDispatcher();

        /// Wait server pool to avoid use-after-free of destroyed context in the handlers
        server_pool.joinAll();

        /** Explicitly destroy Context. It is more convenient than in destructor of Server, because logger is still available.
          * At this moment, no one could own shared part of Context.
          */
        global_context->resetSharedContext();
        global_context.reset();
        shared_context.reset();
        LOG_DEBUG(log, "Destroyed global context.");
    });


#if USE_AZURE_BLOB_STORAGE
    /// It makes sense to deinitialize libxml after joining of all threads
    /// in global pool because libxml uses thread-local memory allocations via
    /// 'pthread_key_create' and 'pthread_setspecific' which should be deallocated
    /// at 'pthread_exit'. Deinitialization of libxml leads to call of 'pthread_key_delete'
    /// and if it is done before joining of threads, allocated memory will not be freed
    /// and there may be memory leaks in threads that used libxml.
    GlobalThreadPool::instance().addOnDestroyCallback([]
    {
        Azure::Storage::_internal::XmlGlobalDeinitialize();
    });
#endif

    getIOThreadPool().initialize(
        server_settings[ServerSetting::max_io_thread_pool_size],
        server_settings[ServerSetting::max_io_thread_pool_free_size],
        server_settings[ServerSetting::io_thread_pool_queue_size]);

    getBackupsIOThreadPool().initialize(
        server_settings[ServerSetting::max_backups_io_thread_pool_size],
        server_settings[ServerSetting::max_backups_io_thread_pool_free_size],
        server_settings[ServerSetting::backups_io_thread_pool_queue_size]);

    getFetchPartitionThreadPool().initialize(
        server_settings[ServerSetting::max_fetch_partition_thread_pool_size],
        0, // FETCH PARTITION is relatively rare, no need to keep threads
        server_settings[ServerSetting::max_fetch_partition_thread_pool_size]);

    getActivePartsLoadingThreadPool().initialize(
        server_settings[ServerSetting::max_active_parts_loading_thread_pool_size],
        0, // We don't need any threads once all the parts will be loaded
        server_settings[ServerSetting::max_active_parts_loading_thread_pool_size]);

    getOutdatedPartsLoadingThreadPool().initialize(
        server_settings[ServerSetting::max_outdated_parts_loading_thread_pool_size],
        0, // We don't need any threads once all the parts will be loaded
        server_settings[ServerSetting::max_outdated_parts_loading_thread_pool_size]);

    /// It could grow if we need to synchronously wait until all the data parts will be loaded.
    getOutdatedPartsLoadingThreadPool().setMaxTurboThreads(
        server_settings[ServerSetting::max_active_parts_loading_thread_pool_size]
    );

    getUnexpectedPartsLoadingThreadPool().initialize(
        server_settings[ServerSetting::max_unexpected_parts_loading_thread_pool_size],
        0, // We don't need any threads once all the parts will be loaded
        server_settings[ServerSetting::max_unexpected_parts_loading_thread_pool_size]);

    /// It could grow if we need to synchronously wait until all the data parts will be loaded.
    getUnexpectedPartsLoadingThreadPool().setMaxTurboThreads(
        server_settings[ServerSetting::max_active_parts_loading_thread_pool_size]
    );

    getPartsCleaningThreadPool().initialize(
        server_settings[ServerSetting::max_parts_cleaning_thread_pool_size],
        0, // We don't need any threads one all the parts will be deleted
        server_settings[ServerSetting::max_parts_cleaning_thread_pool_size]);

    auto max_database_replicated_create_table_thread_pool_size = server_settings[ServerSetting::max_database_replicated_create_table_thread_pool_size]
        ? server_settings[ServerSetting::max_database_replicated_create_table_thread_pool_size]
        : getNumberOfCPUCoresToUse();
    getDatabaseReplicatedCreateTablesThreadPool().initialize(
        max_database_replicated_create_table_thread_pool_size,
        0, // We don't need any threads once all the tables will be created
        max_database_replicated_create_table_thread_pool_size);

    getDatabaseCatalogDropTablesThreadPool().initialize(
        server_settings[ServerSetting::database_catalog_drop_table_concurrency],
        0, // We don't need any threads if there are no DROP queries.
        server_settings[ServerSetting::database_catalog_drop_table_concurrency]);

    getMergeTreePrefixesDeserializationThreadPool().initialize(
        server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_size],
        server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_free_size],
        server_settings[ServerSetting::prefixes_deserialization_thread_pool_thread_pool_queue_size]);

    getFormatParsingThreadPool().initialize(
        server_settings[ServerSetting::max_format_parsing_thread_pool_size],
        server_settings[ServerSetting::max_format_parsing_thread_pool_free_size],
        server_settings[ServerSetting::format_parsing_thread_pool_queue_size]);

    std::string path_str = getCanonicalPath(config().getString("path", DBMS_DEFAULT_PATH));
    fs::path path = path_str;

    /// Check that the process user id matches the owner of the data.
    assertProcessUserMatchesDataOwner(
        path_str,
        [&](const PreformattedMessage & message)
        {
            global_context->addOrUpdateWarningMessage(
                Context::WarningType::PROCESS_USER_MATCHES_DATA_OWNER, message);
        });

    global_context->setPath(path_str);

    StatusFile status{path / "status", StatusFile::write_full_info};

    ServerUUID::load(path / "uuid", log);

    PlacementInfo::PlacementInfo::instance().initialize(config());

    zkutil::validateZooKeeperConfig(config());
    bool has_zookeeper = zkutil::hasZooKeeperConfig(config());

    zkutil::ZooKeeperNodeCache main_config_zk_node_cache([&] { return global_context->getZooKeeper(); });
    zkutil::EventPtr main_config_zk_changed_event = std::make_shared<Poco::Event>();
    if (loaded_config.has_zk_includes)
    {
        auto old_configuration = loaded_config.configuration;
        ConfigProcessor config_processor(config_path);
        loaded_config = config_processor.loadConfigWithZooKeeperIncludes(
            main_config_zk_node_cache, main_config_zk_changed_event, /* fallback_to_preprocessed = */ true);
        config_processor.savePreprocessedConfig(loaded_config, path_str);
        config().removeConfiguration(old_configuration.get());
        config().add(loaded_config.configuration.duplicate(), PRIO_DEFAULT, false);
        global_context->setConfig(loaded_config.configuration);
    }

    Settings::checkNoSettingNamesAtTopLevel(config(), config_path);

    /// We need to reload server settings because config could be updated via zookeeper.
    server_settings.loadSettingsFromConfig(config());

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
        if (stored_binary_hash.empty())
        {
            LOG_WARNING(log, "Integrity check of the executable skipped because the reference checksum could not be read.");
        }
        else
        {
            String calculated_binary_hash = getHashOfLoadedBinaryHex();
            if (calculated_binary_hash == stored_binary_hash)
            {
                LOG_INFO(log, "Integrity check of the executable successfully passed (checksum: {})", calculated_binary_hash);
            }
            else
            {
                /// If program is run under debugger, ptrace will fail.
                if (ptrace(PTRACE_TRACEME, 0, nullptr, nullptr) == -1)
                {
                    /// Program is run under debugger. Modification of it's binary image is ok for breakpoints.
                    global_context->addOrUpdateWarningMessage(
                        Context::WarningType::SERVER_RUN_UNDER_DEBUGGER,
                        PreformattedMessage::create(
                            "Server is run under debugger and its binary image {} is modified (most likely with breakpoints).",
                            calculated_binary_hash));
                }
                else
                {
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Calculated checksum of the executable ({0}) does not correspond"
                        " to the reference checksum stored in the executable ({1})."
                        " This may indicate one of the following:"
                        " - the executable {2} was changed just after startup;"
                        " - the executable {2} was corrupted on disk due to faulty hardware;"
                        " - the loaded executable was corrupted in memory due to faulty hardware;"
                        " - the file {2} was intentionally modified;"
                        " - a logical error in the code.",
                        calculated_binary_hash,
                        stored_binary_hash,
                        executable_path);
                }
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
                        LOG_WARNING(log, "Failed mlock: {}", errnoToString());
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

    FailPointInjection::enableFromGlobalConfig(config());
#endif

    memory_worker.start();

#if defined(OS_LINUX)
    int default_oom_score = 0;

#if !defined(NDEBUG)
    /// In debug version on Linux, increase oom score so that clickhouse is killed
    /// first, instead of some service. Use a carefully chosen random score of 555:
    /// the maximum is 1000, and chromium uses 300 for its tab processes. Ignore
    /// whatever errors that occur, because it's just a debugging aid and we don't
    /// care if it breaks.
    default_oom_score = 555;
#endif

    int oom_score = config().getInt("oom_score", default_oom_score);
    if (oom_score)
        setOOMScore(oom_score, log);
#endif

    std::unique_ptr<DB::BackgroundSchedulePoolTaskHolder> cancellation_task;

    SCOPE_EXIT({
        if (cancellation_task)
            CancellationChecker::getInstance().terminateThread();
    });

    if (server_settings[ServerSetting::background_schedule_pool_size] > 1)
    {
        auto cancellation_task_holder = global_context->getSchedulePool().createTask(
            "CancellationChecker",
            [] { CancellationChecker::getInstance().workerFunction(); }
        );
        cancellation_task = std::make_unique<DB::BackgroundSchedulePoolTaskHolder>(std::move(cancellation_task_holder));
        (*cancellation_task)->activateAndSchedule();
    }

    global_context->setRemoteHostFilter(config());
    global_context->setHTTPHeaderFilter(config());

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
            rlim.rlim_cur = config().getUInt("max_open_files", static_cast<unsigned>(rlim.rlim_max));
            int rc = setrlimit(RLIMIT_NOFILE, &rlim);
            if (rc != 0)
                LOG_WARNING(log, "Cannot set max number of file descriptors to {}. Try to specify max_open_files according to your system limits. error: {}", rlim.rlim_cur, errnoToString());
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
                LOG_WARNING(log, "Cannot set max number of threads to {}. error: {}", rlim.rlim_cur, errnoToString());
                rlim.rlim_cur = old;
            }
            else
            {
                LOG_DEBUG(log, "Set max number of threads to {} (was {}).", rlim.rlim_cur, old);
            }
        }

        if (rlim.rlim_cur < 30000)
        {
            global_context->addOrUpdateWarningMessage(
                Context::WarningType::MAX_NUM_THREADS_LOWER_THAN_LIMIT,
                PreformattedMessage::create(
                    "Maximum number of threads is lower than 30000. There could be problems with handling a lot of simultaneous queries."));
        }
    }

    static ServerErrorHandler error_handler;
    Poco::ErrorHandler::set(&error_handler);

    /// Initialize DateLUT early, to not interfere with running time of first query.
    LOG_DEBUG(log, "Initializing DateLUT.");
    DateLUT::serverTimezoneInstance();
    LOG_TRACE(log, "Initialized DateLUT with time zone '{}'.", getDateLUTTimeZone(DateLUT::serverTimezoneInstance()));

    if (config().has("macros"))
        global_context->setMacros(std::make_unique<Macros>(config(), "macros", log));

    /// Storage with temporary data for processing of heavy queries.
    if (!server_settings[ServerSetting::tmp_policy].value.empty())
    {
        global_context->setTemporaryStoragePolicy(server_settings[ServerSetting::tmp_policy], server_settings[ServerSetting::max_temporary_data_on_disk_size]);
    }
    else if (!server_settings[ServerSetting::temporary_data_in_cache].value.empty())
    {
        global_context->setTemporaryStorageInCache(server_settings[ServerSetting::temporary_data_in_cache], server_settings[ServerSetting::max_temporary_data_on_disk_size]);
    }
    else
    {
        std::string temporary_path = config().getString("tmp_path", path / "tmp/");
        global_context->setTemporaryStoragePath(temporary_path, server_settings[ServerSetting::max_temporary_data_on_disk_size]);
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
    }

    {
        std::string user_scripts_path = config().getString("user_scripts_path", path / "user_scripts/");
        global_context->setUserScriptsPath(user_scripts_path);
    }

    /// top_level_domains_lists
    {
        const std::string & top_level_domains_path = config().getString("top_level_domains_path", path / "top_level_domains/");
        TLDListsHolder::getInstance().parseConfig(fs::path(top_level_domains_path) / "", config());
    }

    {
        fs::create_directories(path / "data");
        fs::create_directories(path / "metadata");

        /// Directory with metadata of tables, which was marked as dropped by Atomic database
        fs::create_directories(path / "metadata_dropped");
    }

    if (config().has("interserver_http_port") && config().has("interserver_https_port"))
        throw Exception(ErrorCodes::EXCESSIVE_ELEMENT_IN_CONFIG, "Both http and https interserver ports are specified");

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
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "Out of range '{}': {}", String(port_tag), port);

            global_context->setInterserverIOAddress(this_host, port);
            global_context->setInterserverScheme(scheme);
        }
    }

    LOG_DEBUG(log, "Initializing interserver credentials.");
    global_context->updateInterserverCredentials(config());

    /// Set up caches.

    const size_t max_cache_size = static_cast<size_t>(physical_server_memory * server_settings[ServerSetting::cache_size_to_ram_max_ratio]);

    String uncompressed_cache_policy = server_settings[ServerSetting::uncompressed_cache_policy];
    size_t uncompressed_cache_size = server_settings[ServerSetting::uncompressed_cache_size];
    double uncompressed_cache_size_ratio = server_settings[ServerSetting::uncompressed_cache_size_ratio];
    if (uncompressed_cache_size > max_cache_size)
    {
        uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered uncompressed cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(uncompressed_cache_size));
    }
    global_context->setUncompressedCache(uncompressed_cache_policy, uncompressed_cache_size, uncompressed_cache_size_ratio);

    String mark_cache_policy = server_settings[ServerSetting::mark_cache_policy];
    size_t mark_cache_size = server_settings[ServerSetting::mark_cache_size];
    double mark_cache_size_ratio = server_settings[ServerSetting::mark_cache_size_ratio];
    if (mark_cache_size > max_cache_size)
    {
        mark_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(mark_cache_size));
    }
    global_context->setMarkCache(mark_cache_policy, mark_cache_size, mark_cache_size_ratio);

    String primary_index_cache_policy = server_settings[ServerSetting::primary_index_cache_policy];
    size_t primary_index_cache_size = server_settings[ServerSetting::primary_index_cache_size];
    double primary_index_cache_size_ratio = server_settings[ServerSetting::primary_index_cache_size_ratio];
    if (primary_index_cache_size > max_cache_size)
    {
        primary_index_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered primary index cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(primary_index_cache_size));
    }
    global_context->setPrimaryIndexCache(primary_index_cache_policy, primary_index_cache_size, primary_index_cache_size_ratio);

    String index_uncompressed_cache_policy = server_settings[ServerSetting::index_uncompressed_cache_policy];
    size_t index_uncompressed_cache_size = server_settings[ServerSetting::index_uncompressed_cache_size];
    double index_uncompressed_cache_size_ratio = server_settings[ServerSetting::index_uncompressed_cache_size_ratio];
    if (index_uncompressed_cache_size > max_cache_size)
    {
        index_uncompressed_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered index uncompressed cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(index_uncompressed_cache_size));
    }
    global_context->setIndexUncompressedCache(index_uncompressed_cache_policy, index_uncompressed_cache_size, index_uncompressed_cache_size_ratio);

    String index_mark_cache_policy = server_settings[ServerSetting::index_mark_cache_policy];
    size_t index_mark_cache_size = server_settings[ServerSetting::index_mark_cache_size];
    double index_mark_cache_size_ratio = server_settings[ServerSetting::index_mark_cache_size_ratio];
    if (index_mark_cache_size > max_cache_size)
    {
        index_mark_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered index mark cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(index_mark_cache_size));
    }
    global_context->setIndexMarkCache(index_mark_cache_policy, index_mark_cache_size, index_mark_cache_size_ratio);

    String vector_similarity_index_cache_policy = server_settings[ServerSetting::vector_similarity_index_cache_policy];
    size_t vector_similarity_index_cache_size = server_settings[ServerSetting::vector_similarity_index_cache_size];
    size_t vector_similarity_index_cache_max_entries = server_settings[ServerSetting::vector_similarity_index_cache_max_entries];
    double vector_similarity_index_cache_size_ratio = server_settings[ServerSetting::vector_similarity_index_cache_size_ratio];
    if (vector_similarity_index_cache_size > max_cache_size)
    {
        vector_similarity_index_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered vector similarity index cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(vector_similarity_index_cache_size));
    }
    global_context->setVectorSimilarityIndexCache(vector_similarity_index_cache_policy, vector_similarity_index_cache_size, vector_similarity_index_cache_max_entries, vector_similarity_index_cache_size_ratio);

    size_t mmap_cache_size = server_settings[ServerSetting::mmap_cache_size];
    if (mmap_cache_size > max_cache_size)
    {
        mmap_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered mmap file cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(mmap_cache_size));
    }
    global_context->setMMappedFileCache(mmap_cache_size);

#if USE_AVRO
    String iceberg_metadata_files_cache_policy = server_settings[ServerSetting::iceberg_metadata_files_cache_policy];
    size_t iceberg_metadata_files_cache_size = server_settings[ServerSetting::iceberg_metadata_files_cache_size];
    size_t iceberg_metadata_files_cache_max_entries = server_settings[ServerSetting::iceberg_metadata_files_cache_max_entries];
    double iceberg_metadata_files_cache_size_ratio = server_settings[ServerSetting::iceberg_metadata_files_cache_size_ratio];
    if (iceberg_metadata_files_cache_size > max_cache_size)
    {
        iceberg_metadata_files_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered Iceberg metadata cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(iceberg_metadata_files_cache_size));
    }
    global_context->setIcebergMetadataFilesCache(iceberg_metadata_files_cache_policy, iceberg_metadata_files_cache_size, iceberg_metadata_files_cache_max_entries, iceberg_metadata_files_cache_size_ratio);
#endif

    String query_condition_cache_policy = server_settings[ServerSetting::query_condition_cache_policy];
    size_t query_condition_cache_size = server_settings[ServerSetting::query_condition_cache_size];
    double query_condition_cache_size_ratio = server_settings[ServerSetting::query_condition_cache_size_ratio];
    if (query_condition_cache_size > max_cache_size)
    {
        query_condition_cache_size = max_cache_size;
        LOG_INFO(log, "Lowered query condition cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(query_condition_cache_size));
    }
    global_context->setQueryConditionCache(query_condition_cache_policy, query_condition_cache_size, query_condition_cache_size_ratio);

    size_t query_result_cache_max_size_in_bytes = config().getUInt64("query_cache.max_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_SIZE);
    size_t query_result_cache_max_entries = config().getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);
    size_t query_result_cache_max_entry_size_in_bytes = config().getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
    size_t query_result_cache_max_entry_size_in_rows = config().getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
    if (query_result_cache_max_size_in_bytes > max_cache_size)
    {
        query_result_cache_max_size_in_bytes = max_cache_size;
        LOG_INFO(log, "Lowered query result cache size to {} because the system has limited RAM", formatReadableSizeWithBinarySuffix(query_result_cache_max_size_in_bytes));
    }
    global_context->setQueryResultCache(query_result_cache_max_size_in_bytes, query_result_cache_max_entries, query_result_cache_max_entry_size_in_bytes, query_result_cache_max_entry_size_in_rows);

#if USE_EMBEDDED_COMPILER
    size_t compiled_expression_cache_max_size_in_bytes = server_settings[ServerSetting::compiled_expression_cache_size];
    size_t compiled_expression_cache_max_elements = server_settings[ServerSetting::compiled_expression_cache_elements_size];
    CompiledExpressionCacheFactory::instance().init(compiled_expression_cache_max_size_in_bytes, compiled_expression_cache_max_elements);
#endif

    NamedCollectionFactory::instance().loadIfNot();
    FileCacheFactory::instance().loadDefaultCaches(config(), global_context);

    /// Initialize main config reloader.
    std::string include_from_path = config().getString("include_from", "/etc/metrika.xml");

    if (config().has("query_masking_rules"))
    {
        SensitiveDataMasker::setInstance(std::make_unique<SensitiveDataMasker>(config(), "query_masking_rules"));
    }

    std::optional<CgroupsMemoryUsageObserver> cgroups_memory_usage_observer;
    try
    {
        auto wait_time = server_settings[ServerSetting::cgroups_memory_usage_observer_wait_time];
        if (wait_time != 0)
            cgroups_memory_usage_observer.emplace(std::chrono::seconds(wait_time));
    }
    catch (Exception &)
    {
        tryLogCurrentException(log, "Disabling cgroup memory observer because of an error during initialization");
    }

    std::string cert_path = config().getString("openSSL.server.certificateFile", "");
    std::string key_path = config().getString("openSSL.server.privateKeyFile", "");

    std::vector<std::string> extra_paths = {include_from_path};
    if (!cert_path.empty())
        extra_paths.emplace_back(cert_path);
    if (!key_path.empty())
        extra_paths.emplace_back(key_path);

    Poco::Util::AbstractConfiguration::Keys protocols;
    config().keys("protocols", protocols);
    for (const auto & protocol : protocols)
    {
        cert_path = config().getString("protocols." + protocol + ".certificateFile", "");
        key_path = config().getString("protocols." + protocol + ".privateKeyFile", "");
        if (!cert_path.empty())
            extra_paths.emplace_back(cert_path);
        if (!key_path.empty())
            extra_paths.emplace_back(key_path);
    }

    auto main_config_reloader = std::make_unique<ConfigReloader>(
        config_path,
        extra_paths,
        config().getString("path", DBMS_DEFAULT_PATH),
        std::move(main_config_zk_node_cache),
        main_config_zk_changed_event,
        [&](ConfigurationPtr config, bool initial_loading)
        {
            Settings::checkNoSettingNamesAtTopLevel(*config, config_path);

            ServerSettings new_server_settings;
            new_server_settings.loadSettingsFromConfig(*config);

            size_t max_server_memory_usage = new_server_settings[ServerSetting::max_server_memory_usage];
            const double max_server_memory_usage_to_ram_ratio = new_server_settings[ServerSetting::max_server_memory_usage_to_ram_ratio];
            const size_t current_physical_server_memory = getMemoryAmount(); /// With cgroups, the amount of memory available to the server can be changed dynamically.
            const size_t default_max_server_memory_usage = static_cast<size_t>(current_physical_server_memory * max_server_memory_usage_to_ram_ratio);

            if (max_server_memory_usage == 0)
            {
                max_server_memory_usage = default_max_server_memory_usage;
                LOG_INFO(log, "Changed setting 'max_server_memory_usage' to {}"
                    " ({} available memory * {:.2f} max_server_memory_usage_to_ram_ratio)",
                    formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                    formatReadableSizeWithBinarySuffix(current_physical_server_memory),
                    max_server_memory_usage_to_ram_ratio);
            }
            else if (max_server_memory_usage > default_max_server_memory_usage)
            {
                max_server_memory_usage = default_max_server_memory_usage;
                LOG_INFO(log, "Lowered setting 'max_server_memory_usage' to {}"
                    " because the system has too little memory. The new value was"
                    " calculated as {} available memory * {:.2f} max_server_memory_usage_to_ram_ratio",
                    formatReadableSizeWithBinarySuffix(max_server_memory_usage),
                    formatReadableSizeWithBinarySuffix(current_physical_server_memory),
                    max_server_memory_usage_to_ram_ratio);
            }

            total_memory_tracker.setHardLimit(max_server_memory_usage);
            total_memory_tracker.setDescription("(total)");
            total_memory_tracker.setMetric(CurrentMetrics::MemoryTracking);

            size_t merges_mutations_memory_usage_soft_limit = new_server_settings[ServerSetting::merges_mutations_memory_usage_soft_limit];

            size_t default_merges_mutations_server_memory_usage = static_cast<size_t>(current_physical_server_memory * new_server_settings[ServerSetting::merges_mutations_memory_usage_to_ram_ratio]);
            if (merges_mutations_memory_usage_soft_limit == 0)
            {
                merges_mutations_memory_usage_soft_limit = default_merges_mutations_server_memory_usage;
                LOG_INFO(log, "Setting merges_mutations_memory_usage_soft_limit was set to {}"
                    " ({} available * {:.2f} merges_mutations_memory_usage_to_ram_ratio)",
                    formatReadableSizeWithBinarySuffix(merges_mutations_memory_usage_soft_limit),
                    formatReadableSizeWithBinarySuffix(current_physical_server_memory),
                    new_server_settings[ServerSetting::merges_mutations_memory_usage_to_ram_ratio].value);
            }
            else if (merges_mutations_memory_usage_soft_limit > default_merges_mutations_server_memory_usage)
            {
                merges_mutations_memory_usage_soft_limit = default_merges_mutations_server_memory_usage;
                LOG_WARNING(log, "Setting merges_mutations_memory_usage_soft_limit was set to {}"
                    " ({} available * {:.2f} merges_mutations_memory_usage_to_ram_ratio)",
                    formatReadableSizeWithBinarySuffix(merges_mutations_memory_usage_soft_limit),
                    formatReadableSizeWithBinarySuffix(current_physical_server_memory),
                    new_server_settings[ServerSetting::merges_mutations_memory_usage_to_ram_ratio].value);
            }

            LOG_INFO(log, "Merges and mutations memory limit is set to {}",
                formatReadableSizeWithBinarySuffix(merges_mutations_memory_usage_soft_limit));
            background_memory_tracker.setSoftLimit(merges_mutations_memory_usage_soft_limit);
            background_memory_tracker.setDescription("(background)");
            background_memory_tracker.setMetric(CurrentMetrics::MergesMutationsMemoryTracking);

            auto * global_overcommit_tracker = global_context->getGlobalOvercommitTracker();
            total_memory_tracker.setOvercommitTracker(global_overcommit_tracker);

            // FIXME logging-related things need synchronization -- see the 'Logger * log' saved
            // in a lot of places. For now, disable updating log configuration without server restart.
            //setTextLog(global_context->getTextLog());
            updateLevels(*config, logger());
            global_context->setClustersConfig(config, has_zookeeper);
            global_context->setMacros(std::make_unique<Macros>(*config, "macros", log));
            global_context->setExternalAuthenticatorsConfig(*config);

            global_context->setDashboardsConfig(config);

            if (global_context->isServerCompletelyStarted())
            {
                /// It does not make sense to reload anything before server has started.
                /// Moreover, it may break initialization order.
                global_context->loadOrReloadDictionaries(*config);
                global_context->loadOrReloadUserDefinedExecutableFunctions(*config);
            }

            global_context->setRemoteHostFilter(*config);
            global_context->setHTTPHeaderFilter(*config);

            global_context->setMaxTableSizeToDrop(new_server_settings[ServerSetting::max_table_size_to_drop]);
            global_context->setMaxPartitionSizeToDrop(new_server_settings[ServerSetting::max_partition_size_to_drop]);
            global_context->setMaxTableNumToWarn(new_server_settings[ServerSetting::max_table_num_to_warn]);
            global_context->setMaxViewNumToWarn(new_server_settings[ServerSetting::max_view_num_to_warn]);
            global_context->setMaxDictionaryNumToWarn(new_server_settings[ServerSetting::max_dictionary_num_to_warn]);
            global_context->setMaxDatabaseNumToWarn(new_server_settings[ServerSetting::max_database_num_to_warn]);
            global_context->setMaxPartNumToWarn(new_server_settings[ServerSetting::max_part_num_to_warn]);
            global_context->setMaxPendingMutationsToWarn(new_server_settings[ServerSetting::max_pending_mutations_to_warn]);
            global_context->setMaxPendingMutationsExecutionTimeToWarn(new_server_settings[ServerSetting::max_pending_mutations_execution_time_to_warn]);
            global_context->getAccessControl().setAllowTierSettings(new_server_settings[ServerSetting::allow_feature_tier]);

            global_context->setServerSetting("s3queue_disable_streaming", new_server_settings[ServerSetting::s3queue_disable_streaming].value);

            global_context->setOSCPUOverloadSettings(new_server_settings[ServerSetting::min_os_cpu_wait_time_ratio_to_drop_connection], new_server_settings[ServerSetting::max_os_cpu_wait_time_ratio_to_drop_connection]);

            size_t remote_read_bandwidth = new_server_settings[ServerSetting::max_remote_read_network_bandwidth_for_server];
            size_t remote_write_bandwidth = new_server_settings[ServerSetting::max_remote_write_network_bandwidth_for_server];

            global_context->reloadRemoteThrottlerConfig(remote_read_bandwidth,remote_write_bandwidth);
            LOG_INFO(log, "Setting max_remote_read_network_bandwidth_for_server was set to {}", remote_read_bandwidth);
            LOG_INFO(log, "Setting max_remote_write_network_bandwidth_for_server was set to {}", remote_write_bandwidth);

            size_t local_read_bandwidth = new_server_settings[ServerSetting::max_local_read_bandwidth_for_server];
            size_t local_write_bandwidth = new_server_settings[ServerSetting::max_local_write_bandwidth_for_server];

            global_context->reloadLocalThrottlerConfig(local_read_bandwidth,local_write_bandwidth);
            LOG_INFO(log, "Setting max_local_read_bandwidth_for_server was set to {}", local_read_bandwidth);
            LOG_INFO(log, "Setting max_local_write_bandwidth_for_server was set to {}", local_write_bandwidth);

            /// Only for system.server_settings
            global_context->setConfigReloaderInterval(new_server_settings[ServerSetting::config_reload_interval_ms]);

            auto [concurrent_threads_soft_limit, concurrency_control_scheduler] = global_context->setConcurrentThreadsSoftLimit(
                new_server_settings[ServerSetting::concurrent_threads_soft_limit_num],
                new_server_settings[ServerSetting::concurrent_threads_soft_limit_ratio_to_cores],
                new_server_settings[ServerSetting::concurrent_threads_scheduler]);
            LOG_INFO(log, "ConcurrencyControl limit is set to {} CPU slots with '{}' scheduler",
                concurrent_threads_soft_limit == UnlimitedSlots ? std::string("UNLIMITED") : std::to_string(concurrent_threads_soft_limit),
                concurrency_control_scheduler);

            global_context->getProcessList().setMaxSize(new_server_settings[ServerSetting::max_concurrent_queries]);
            global_context->getProcessList().setMaxInsertQueriesAmount(new_server_settings[ServerSetting::max_concurrent_insert_queries]);
            global_context->getProcessList().setMaxSelectQueriesAmount(new_server_settings[ServerSetting::max_concurrent_select_queries]);
            global_context->getProcessList().setMaxWaitingQueriesAmount(new_server_settings[ServerSetting::max_waiting_queries]);

            if (config->has("keeper_server"))
                global_context->updateKeeperConfiguration(*config);

            /// Reload the number of threads for global pools.
            /// Note: If you specified it in the top level config (not it config of default profile)
            /// then ClickHouse will use it exactly.
            /// This is done for backward compatibility.
            if (global_context->areBackgroundExecutorsInitialized())
            {
                auto new_pool_size = new_server_settings[ServerSetting::background_pool_size];
                auto new_ratio = new_server_settings[ServerSetting::background_merges_mutations_concurrency_ratio];
                global_context->getMergeMutateExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, static_cast<size_t>(new_pool_size * new_ratio));
                global_context->getMergeMutateExecutor()->updateSchedulingPolicy(new_server_settings[ServerSetting::background_merges_mutations_scheduling_policy].toString());
            }

            if (global_context->areBackgroundExecutorsInitialized())
            {
                auto new_pool_size = new_server_settings[ServerSetting::background_move_pool_size];
                global_context->getMovesExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size);
            }

            if (global_context->areBackgroundExecutorsInitialized())
            {
                auto new_pool_size = new_server_settings[ServerSetting::background_fetches_pool_size];
                global_context->getFetchesExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size);
            }

            if (global_context->areBackgroundExecutorsInitialized())
            {
                auto new_pool_size = new_server_settings[ServerSetting::background_common_pool_size];
                global_context->getCommonExecutor()->increaseThreadsAndMaxTasksCount(new_pool_size, new_pool_size);
            }

            global_context->getBufferFlushSchedulePool().increaseThreadsCount(new_server_settings[ServerSetting::background_buffer_flush_schedule_pool_size]);
            global_context->getSchedulePool().increaseThreadsCount(new_server_settings[ServerSetting::background_schedule_pool_size]);
            global_context->getMessageBrokerSchedulePool().increaseThreadsCount(new_server_settings[ServerSetting::background_message_broker_schedule_pool_size]);
            global_context->getDistributedSchedulePool().increaseThreadsCount(new_server_settings[ServerSetting::background_distributed_schedule_pool_size]);

            global_context->getAsyncLoader().setMaxThreads(TablesLoaderForegroundPoolId, new_server_settings[ServerSetting::tables_loader_foreground_pool_size]);
            global_context->getAsyncLoader().setMaxThreads(TablesLoaderBackgroundLoadPoolId, new_server_settings[ServerSetting::tables_loader_background_pool_size]);
            global_context->getAsyncLoader().setMaxThreads(TablesLoaderBackgroundStartupPoolId, new_server_settings[ServerSetting::tables_loader_background_pool_size]);

            getIOThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_io_thread_pool_size],
                new_server_settings[ServerSetting::max_io_thread_pool_free_size],
                new_server_settings[ServerSetting::io_thread_pool_queue_size]);

            getBackupsIOThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_backups_io_thread_pool_size],
                new_server_settings[ServerSetting::max_backups_io_thread_pool_free_size],
                new_server_settings[ServerSetting::backups_io_thread_pool_queue_size]);

            getFetchPartitionThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_fetch_partition_thread_pool_size],
                0, // FETCH PARTITION is relatively rare, no need to keep threads
                new_server_settings[ServerSetting::max_fetch_partition_thread_pool_size]);

            getActivePartsLoadingThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_active_parts_loading_thread_pool_size],
                0, // We don't need any threads once all the parts will be loaded
                new_server_settings[ServerSetting::max_active_parts_loading_thread_pool_size]);

            getOutdatedPartsLoadingThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_outdated_parts_loading_thread_pool_size],
                0, // We don't need any threads once all the parts will be loaded
                new_server_settings[ServerSetting::max_outdated_parts_loading_thread_pool_size]);

            /// It could grow if we need to synchronously wait until all the data parts will be loaded.
            getOutdatedPartsLoadingThreadPool().setMaxTurboThreads(
                new_server_settings[ServerSetting::max_active_parts_loading_thread_pool_size]
            );

            getPartsCleaningThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_parts_cleaning_thread_pool_size],
                0, // We don't need any threads one all the parts will be deleted
                new_server_settings[ServerSetting::max_parts_cleaning_thread_pool_size]);

            getMergeTreePrefixesDeserializationThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_size],
                new_server_settings[ServerSetting::max_prefixes_deserialization_thread_pool_free_size],
                new_server_settings[ServerSetting::prefixes_deserialization_thread_pool_thread_pool_queue_size]);

            getFormatParsingThreadPool().reloadConfiguration(
                new_server_settings[ServerSetting::max_format_parsing_thread_pool_size],
                new_server_settings[ServerSetting::max_format_parsing_thread_pool_free_size],
                new_server_settings[ServerSetting::format_parsing_thread_pool_queue_size]);

            global_context->setMergeWorkload(new_server_settings[ServerSetting::merge_workload]);
            global_context->setMutationWorkload(new_server_settings[ServerSetting::mutation_workload]);
            global_context->setThrowOnUnknownWorkload(new_server_settings[ServerSetting::throw_on_unknown_workload]);

            if (config->has("resources"))
            {
                global_context->getResourceManager()->updateConfiguration(*config);
            }

            if (!initial_loading)
            {
                /// We do not load ZooKeeper configuration on the first config loading
                /// because TestKeeper server is not started yet.
                if (zkutil::hasZooKeeperConfig(*config))
                    global_context->reloadZooKeeperIfChanged(config);

                global_context->reloadAuxiliaryZooKeepersConfigIfChanged(config);

                global_context->reloadQueryMaskingRulesIfChanged(config);

                if (global_context->isServerCompletelyStarted())
                {
                    std::lock_guard lock(servers_lock);
                    updateServers(*config, server_pool, async_metrics, servers, servers_to_start_before_tables);
                }
            }

            global_context->updateStorageConfiguration(*config);
            global_context->updateInterserverCredentials(*config);

            global_context->updateUncompressedCacheConfiguration(*config);
            global_context->updateMarkCacheConfiguration(*config);
            global_context->updatePrimaryIndexCacheConfiguration(*config);
            global_context->updateIndexUncompressedCacheConfiguration(*config);
            global_context->updateIndexMarkCacheConfiguration(*config);
            global_context->updateVectorSimilarityIndexCacheConfiguration(*config);
            global_context->updateMMappedFileCacheConfiguration(*config);
            global_context->updateQueryResultCacheConfiguration(*config);
            global_context->updateQueryConditionCacheConfiguration(*config);

            CompressionCodecEncrypted::Configuration::instance().tryLoad(*config, "encryption_codecs");
#if USE_SSL
            CertificateReloader::instance().tryReloadAll(*config);
#endif
            NamedCollectionFactory::instance().reloadFromConfig(*config);
            FileCacheFactory::instance().updateSettingsFromConfig(*config);

            HTTPConnectionPools::instance().setLimits(
                HTTPConnectionPools::Limits{
                    new_server_settings[ServerSetting::disk_connections_soft_limit],
                    new_server_settings[ServerSetting::disk_connections_warn_limit],
                    new_server_settings[ServerSetting::disk_connections_store_limit],
                },
                HTTPConnectionPools::Limits{
                    new_server_settings[ServerSetting::storage_connections_soft_limit],
                    new_server_settings[ServerSetting::storage_connections_warn_limit],
                    new_server_settings[ServerSetting::storage_connections_store_limit],
                },
                HTTPConnectionPools::Limits{
                    new_server_settings[ServerSetting::http_connections_soft_limit],
                    new_server_settings[ServerSetting::http_connections_warn_limit],
                    new_server_settings[ServerSetting::http_connections_store_limit],
                });

            DNSResolver::instance().setFilterSettings(new_server_settings[ServerSetting::dns_allow_resolve_names_to_ipv4], new_server_settings[ServerSetting::dns_allow_resolve_names_to_ipv6]);

            if (global_context->isServerCompletelyStarted())
                CannotAllocateThreadFaultInjector::setFaultProbability(new_server_settings[ServerSetting::cannot_allocate_thread_fault_injection_probability]);

            ProfileEvents::increment(ProfileEvents::MainConfigLoads);

            /// Must be the last.
            latest_config = config;
        });

    const auto listen_hosts = getListenHosts(config());
    const auto interserver_listen_hosts = getInterserverListenHosts(config());
    const auto listen_try = getListenTry(config());

    if (config().has("keeper_server.server_id"))
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
                    socket.setReceiveTimeout(Poco::Timespan(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0));
                    socket.setSendTimeout(Poco::Timespan(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0));
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "Keeper (tcp): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new KeeperTCPHandlerFactory(
                                config_getter,
                                global_context->getKeeperDispatcher(),
                                global_context->getSettingsRef()[Setting::receive_timeout].totalSeconds(),
                                global_context->getSettingsRef()[Setting::send_timeout].totalSeconds(),
                                false),
                            server_pool,
                            socket));
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
                    socket.setReceiveTimeout(Poco::Timespan(config().getUInt64("keeper_server.socket_receive_timeout_sec", DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC), 0));
                    socket.setSendTimeout(Poco::Timespan(config().getUInt64("keeper_server.socket_send_timeout_sec", DBMS_DEFAULT_SEND_TIMEOUT_SEC), 0));
                    return ProtocolServerAdapter(
                        listen_host,
                        secure_port_name,
                        "Keeper with secure protocol (tcp_secure): " + address.toString(),
                        std::make_unique<TCPServer>(
                            new KeeperTCPHandlerFactory(
                                config_getter,
                                global_context->getKeeperDispatcher(),
                                global_context->getSettingsRef()[Setting::receive_timeout].totalSeconds(),
                                global_context->getSettingsRef()[Setting::send_timeout].totalSeconds(),
                                true),
                            server_pool,
                            socket));
#else
                    UNUSED(port);
                    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
                });

            /// HTTP control endpoints
            port_name = "keeper_server.http_control.port";
            createServer(config(), listen_host, port_name, listen_try, /* start_server: */ false,
            servers_to_start_before_tables,
            [&](UInt16 port) -> ProtocolServerAdapter
            {
                auto http_context = httpContext();
                Poco::Timespan keep_alive_timeout(config().getUInt("keep_alive_timeout", 10), 0);
                Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
                http_params->setTimeout(http_context->getReceiveTimeout());
                http_params->setKeepAliveTimeout(keep_alive_timeout);

                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(config(), socket, listen_host, port);
                socket.setReceiveTimeout(http_context->getReceiveTimeout());
                socket.setSendTimeout(http_context->getSendTimeout());
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "HTTP Control: http://" + address.toString(),
                    std::make_unique<HTTPServer>(
                        std::move(http_context),
                        createKeeperHTTPControlMainHandlerFactory(
                            config_getter(),
                            global_context->getKeeperDispatcher(),
                            "KeeperHTTPControlHandler-factory"), server_pool, socket, http_params));
            });
        }
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "ClickHouse server built without NuRaft library. Cannot use internal coordination.");
#endif

    }

    {
        std::lock_guard lock(servers_lock);
        /// We should start interserver communications before (and more important shutdown after) tables.
        /// Because server can wait for a long-running queries (for example in tcp_handler) after interserver handler was already shut down.
        /// In this case we will have replicated tables which are unable to send any parts to other replicas, but still can
        /// communicate with zookeeper, execute merges, etc.
        createInterserverServers(
            config(),
            interserver_listen_hosts,
            listen_try,
            server_pool,
            async_metrics,
            servers_to_start_before_tables,
            /* start_servers= */ false);


        for (auto & server : servers_to_start_before_tables)
        {
            server.start();
            LOG_INFO(log, "Listening for {}", server.getDescription());
        }
    }

    /// Initialize access storages.
    auto & access_control = global_context->getAccessControl();
    try
    {
        access_control.setupFromMainConfig(config(), config_path, [&] { return global_context->getZooKeeper(); });
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception while setting up access control.");
        throw;
    }

    if (cgroups_memory_usage_observer)
    {
        cgroups_memory_usage_observer->setOnMemoryAmountAvailableChangedFn([&]() { main_config_reloader->reload(); });
        cgroups_memory_usage_observer->startThread();
    }

    /// Reload config in SYSTEM RELOAD CONFIG query.
    global_context->setConfigReloadCallback([&]()
    {
        main_config_reloader->reload();
        access_control.reload(AccessControl::ReloadMode::USERS_CONFIG_ONLY);
    });

    global_context->setStopServersCallback([&](const ServerType & server_type)
    {
        std::lock_guard lock(servers_lock);
        stopServers(servers, server_type);
    });

    global_context->setStartServersCallback([&](const ServerType & server_type)
    {
        std::lock_guard lock(servers_lock);
        createServers(
            config(),
            listen_hosts,
            listen_try,
            server_pool,
            async_metrics,
            servers,
            /* start_servers= */ true,
            server_type);
    });

    /// Limit on total number of concurrently executed queries.
    global_context->getProcessList().setMaxSize(server_settings[ServerSetting::max_concurrent_queries]);

    /// Load global settings from default_profile and system_profile.
    global_context->setDefaultProfiles(config());

    /// Initialize background executors after we load default_profile config.
    /// This is needed to load proper values of background_pool_size etc.
    global_context->initializeBackgroundExecutorsIfNeeded();

    if (server_settings[ServerSetting::async_insert_threads])
    {
        global_context->setAsynchronousInsertQueue(std::make_shared<AsynchronousInsertQueue>(
            global_context,
            server_settings[ServerSetting::async_insert_threads],
            server_settings[ServerSetting::async_insert_queue_flush_on_shutdown]));
    }

    /// Set path for format schema files
    fs::path format_schema_path(config().getString("format_schema_path", path / "format_schemas/"));
    global_context->setFormatSchemaPath(format_schema_path);
    fs::create_directories(format_schema_path);

    /// Set the path for google proto files
    if (config().has("google_protos_path"))
        global_context->setGoogleProtosPath(fs::weakly_canonical(config().getString("google_protos_path")));

    /// Set path for filesystem caches
    fs::path filesystem_caches_path(config().getString("filesystem_caches_path", ""));
    if (!filesystem_caches_path.empty())
        global_context->setFilesystemCachesPath(filesystem_caches_path);

    /// NOTE: Do sanity checks after we loaded all possible substitutions (for the configuration) from ZK
    /// Additionally, making the check after the default profile is initialized.
    /// It is important to initialize MergeTreeSettings after Settings, to support compatibility for MergeTreeSettings.
    sanityChecks(*this);

    /// Check sanity of MergeTreeSettings on server startup
    {
        /// All settings can be changed in the global config
        bool allowed_experimental = true;
        bool allowed_beta = true;
        size_t background_pool_tasks = global_context->getMergeMutateExecutor()->getMaxTasksCount();
        global_context->getMergeTreeSettings().sanityCheck(background_pool_tasks, allowed_experimental, allowed_beta);
        global_context->getReplicatedMergeTreeSettings().sanityCheck(background_pool_tasks, allowed_experimental, allowed_beta);
    }
    /// try set up encryption. There are some errors in config, error will be printed and server wouldn't start.
    CompressionCodecEncrypted::Configuration::instance().load(config(), "encryption_codecs");

    /// DNSCacheUpdater uses BackgroundSchedulePool which lives in shared context
    /// and thus this object must be created after the SCOPE_EXIT object where shared
    /// context is destroyed.
    /// In addition this object has to be created before the loading of the tables.
    std::unique_ptr<DNSCacheUpdater> dns_cache_updater;
    if (server_settings[ServerSetting::disable_internal_dns_cache])
    {
        /// Disable DNS caching at all
        DNSResolver::instance().setDisableCacheFlag();
        LOG_DEBUG(log, "DNS caching disabled");
    }
    else
    {
        DNSResolver::instance().setCacheMaxEntries(server_settings[ServerSetting::dns_cache_max_entries]);

        /// Initialize a watcher periodically updating DNS cache
        dns_cache_updater = std::make_unique<DNSCacheUpdater>(
            global_context, server_settings[ServerSetting::dns_cache_update_period], server_settings[ServerSetting::dns_max_consecutive_failures]);
    }

    if (dns_cache_updater)
        dns_cache_updater->start();

    auto replicas_reconnector = ReplicasReconnector::init(global_context);

    /// Set current database name before loading tables and databases because
    /// system logs may copy global context.
    std::string default_database = server_settings[ServerSetting::default_database];
    if (default_database.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "default_database cannot be empty");
    global_context->setCurrentDatabaseNameInGlobalContext(default_database);

    LOG_INFO(log, "Loading metadata from {}", path_str);

    LoadTaskPtrs load_system_metadata_tasks;
    LoadTaskPtrs load_metadata_tasks;

    // Make sure that if exception is thrown during startup async, new async loading jobs are not going to be called.
    // This is important for the case when exception is thrown from loading of metadata with `async_load_databases = false`
    // to avoid simultaneously running table startups and destructing databases.
    SCOPE_EXIT_SAFE(
        LOG_INFO(log, "Stopping AsyncLoader.");

        // Waits for all currently running jobs to finish and do not run any other pending jobs.
        global_context->getAsyncLoader().shutdown();
    );

    try
    {
        /// Don't run background queries until we loaded tables.
        /// (In particular things would break if a background drop query happens before the
        /// loadMarkedAsDroppedTables() call below - it'll see dropped table metadata and try to
        /// drop the table a second time and throw an exception.)
        global_context->getRefreshSet().setRefreshesStopped(true);

        auto & database_catalog = DatabaseCatalog::instance();
        /// We load temporary database first, because projections need it.
        database_catalog.initializeAndLoadTemporaryDatabase();
        load_system_metadata_tasks = loadMetadataSystem(global_context, server_settings[ServerSetting::async_load_system_database]);
        maybeConvertSystemDatabase(global_context, load_system_metadata_tasks);

        /// Startup scripts can depend on the system log tables.
        if (config().has("startup_scripts") && !server_settings[ServerSetting::prepare_system_log_tables_on_startup].changed)
            global_context->setServerSetting("prepare_system_log_tables_on_startup", true);

        /// After attaching system databases we can initialize system log.
        global_context->initializeSystemLogs();
        global_context->setSystemZooKeeperLogAfterInitializationIfNeeded();
        /// Build loggers before tables startup to make log messages from tables
        /// attach available in system.text_log
        buildLoggers(config(), logger());
        initializeAzureSDKLogger(server_settings, logger().getLevel());
        /// After the system database is created, attach virtual system tables (in addition to query_log and part_log)
        attachSystemTablesServer(global_context, *database_catalog.getSystemDatabase(), has_zookeeper);
        attachInformationSchema(global_context, *database_catalog.getDatabase(DatabaseCatalog::INFORMATION_SCHEMA));
        attachInformationSchema(global_context, *database_catalog.getDatabase(DatabaseCatalog::INFORMATION_SCHEMA_UPPERCASE));
        /// Firstly remove partially dropped databases, to avoid race with Materialized...SyncThread,
        /// that may execute DROP before loadMarkedAsDroppedTables() in background,
        /// and so loadMarkedAsDroppedTables() will find it and try to add, and UUID will overlap.
        database_catalog.loadMarkedAsDroppedTables();
        database_catalog.createBackgroundTasks();
        /// Then, load remaining databases (some of them maybe be loaded asynchronously)
        load_metadata_tasks = loadMetadata(global_context, default_database, server_settings[ServerSetting::async_load_databases]);
        /// If we need to convert database engines, disable async tables loading
        convertDatabasesEnginesIfNeed(load_metadata_tasks, global_context);
        database_catalog.startupBackgroundTasks();
        /// After loading validate that default database exists
        database_catalog.assertDatabaseExists(default_database);
        /// Load user-defined SQL functions.
        global_context->getUserDefinedSQLObjectsStorage().loadObjects();
        /// Load WORKLOADs and RESOURCEs.
        global_context->getWorkloadEntityStorage().loadEntities();

        global_context->getRefreshSet().setRefreshesStopped(false);
    }
    catch (...)
    {
        tryLogCurrentException(log, "Caught exception while loading metadata");
        throw;
    }

    DatabaseCatalog::instance().startReplicatedDDLQueries();

    LOG_DEBUG(log, "Loaded metadata.");

    if (has_trace_collector)
        global_context->initializeTraceCollector();

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
            createServers(config(), listen_hosts, listen_try, server_pool, async_metrics, servers);
            if (servers.empty())
                throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                                "No servers started (add valid listen_host and 'tcp_port' or 'http_port' "
                                "to configuration file.)");
        }

        if (servers.empty())
             throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                             "No servers started (add valid listen_host and 'tcp_port' or 'http_port' "
                             "to configuration file.)");

#if USE_SSL
        CertificateReloader::instance().tryLoad(config());
        CertificateReloader::instance().tryLoadClient(config());
#endif

        /// Must be done after initialization of `servers`, because async_metrics will access `servers` variable from its thread.
        async_metrics.start();
        global_context->setAsynchronousMetrics(&async_metrics);

        main_config_reloader->start();
        access_control.startPeriodicReloading();

        /// try to load dictionaries immediately, throw on error and die
        try
        {
            global_context->loadOrReloadDictionaries(config());

            if (!server_settings[ServerSetting::dictionaries_lazy_load] && server_settings[ServerSetting::wait_dictionaries_load_at_startup])
                global_context->waitForDictionariesLoad();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading dictionaries.");
            throw;
        }

        /// try to load embedded dictionaries immediately, throw on error and die
        try
        {
            global_context->tryCreateEmbeddedDictionaries();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while loading embedded dictionaries.");
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
            String ddl_queue_path = config().getString("distributed_ddl.path", "/clickhouse/task_queue/ddl/");
            String ddl_replicas_path = config().getString("distributed_ddl.replicas_path", "/clickhouse/task_queue/replicas/");
            int pool_size = config().getInt("distributed_ddl.pool_size", 1);
            if (pool_size < 1)
                throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "distributed_ddl.pool_size should be greater then 0");
            global_context->setDDLWorker(
                std::make_unique<DDLWorker>(
                    pool_size,
                    ddl_queue_path,
                    ddl_replicas_path,
                    global_context,
                    &config(),
                    "distributed_ddl",
                    "DDLWorker",
                    &CurrentMetrics::MaxDDLEntryID,
                    &CurrentMetrics::MaxPushedDDLEntryID),
                joinTasks(load_system_metadata_tasks, load_metadata_tasks));
        }

        /// Do not keep tasks in server, they should be kept inside databases. Used here to make dependent tasks only.
        load_system_metadata_tasks.clear();
        load_system_metadata_tasks.shrink_to_fit();
        load_metadata_tasks.clear();
        load_metadata_tasks.shrink_to_fit();

        if (config().has("startup_scripts"))
            loadStartupScripts(config(), global_context, log);

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

        startup_watch.stop();
        ProfileEvents::increment(ProfileEvents::ServerStartupMilliseconds, startup_watch.elapsedMilliseconds());

        CannotAllocateThreadFaultInjector::setFaultProbability(server_settings[ServerSetting::cannot_allocate_thread_fault_injection_probability]);

#if USE_GWP_ASAN
        GWPAsan::initFinished();
#endif

        try
        {
            global_context->startClusterDiscovery();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Caught exception while starting cluster discovery");
        }

#if defined(OS_LINUX)
        /// Tell the service manager that service startup is finished.
        /// NOTE: the parent clickhouse-watchdog process must do systemdNotify("MAINPID={}\n", child_pid); before
        /// the child process notifies 'READY=1'.
        systemdNotify("READY=1\n");
#endif

        SCOPE_EXIT_SAFE({
            LOG_DEBUG(log, "Received termination signal.");

            CurrentMetrics::set(CurrentMetrics::IsServerShuttingDown, 1);

            /// Stop reloading of the main config. This must be done before everything else because it
            /// can try to access/modify already deleted objects.
            /// E.g. it can recreate new servers or it may pass a changed config to some destroyed parts of ContextSharedPart.
            main_config_reloader.reset();
            access_control.stopPeriodicReloading();

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

            global_context->getRefreshSet().setRefreshesStopped(true);

            if (current_connections)
                LOG_WARNING(log, "Closed all listening sockets. Waiting for {} outstanding connections.", current_connections);
            else
                LOG_INFO(log, "Closed all listening sockets.");

            /// Wait for unfinished backups and restores.
            /// This must be done after closing listening sockets (no more backups/restores) but before ProcessList::killAllQueries
            /// (because killAllQueries() will cancel all running backups/restores).
            if (server_settings[ServerSetting::shutdown_wait_backups_and_restores])
                global_context->waitAllBackupsAndRestores();

            /// Killing remaining queries.
            if (!server_settings[ServerSetting::shutdown_wait_unfinished_queries])
                global_context->getProcessList().killAllQueries();

            size_t wait_limit_seconds = server_settings[ServerSetting::shutdown_wait_unfinished];
            auto wait_start = std::chrono::steady_clock::now();

            if (current_connections)
                current_connections = waitServersToFinish(servers, servers_lock, wait_limit_seconds);

            if (current_connections)
                LOG_WARNING(log, "Closed connections. But {} remain."
                    " Tip: To increase wait time add to config: <shutdown_wait_unfinished>60</shutdown_wait_unfinished>", current_connections);
            else
                LOG_INFO(log, "Closed connections.");

            global_context->getRefreshSet().joinBackgroundTasks(wait_start + std::chrono::milliseconds(wait_limit_seconds * 1000));

            dns_cache_updater.reset();

            if (current_connections)
            {
                /// There is no better way to force connections to close in Poco.
                /// Otherwise connection handlers will continue to live
                /// (they are effectively dangling objects, but they use global thread pool
                ///  and global thread pool destructor will wait for threads, preventing server shutdown).

                /// Dump coverage here, because std::atexit callback would not be called.
                dumpCoverageReportIfPossible();
                LOG_WARNING(log, "Will shutdown forcefully.");
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
catch (...)
{
    /// Poco does not provide stacktrace.
    tryLogCurrentException("Application");
    auto code = getCurrentExceptionCode();
    return static_cast<UInt8>(code) ? code : -1;
}

std::unique_ptr<TCPProtocolStackFactory> Server::buildProtocolStackFromConfig(
    const Poco::Util::AbstractConfiguration & config,
    const std::string & protocol,
    Poco::Net::HTTPServerParams::Ptr http_params,
    AsynchronousMetrics & async_metrics,
    bool & is_secure)
{
    auto create_factory = [&](const std::string & type, const std::string & conf_name) -> TCPServerConnectionFactory::Ptr
    {
        if (type == "tcp")
            return TCPServerConnectionFactory::Ptr(new TCPHandlerFactory(*this, false, false, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes));

        if (type == "tls")
#if USE_SSL
            return TCPServerConnectionFactory::Ptr(new TLSHandlerFactory(*this, conf_name));
#else
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif

        if (type == "proxy1")
            return TCPServerConnectionFactory::Ptr(new ProxyV1HandlerFactory(*this, conf_name));
        if (type == "mysql")
            return TCPServerConnectionFactory::Ptr(new MySQLHandlerFactory(*this, ProfileEvents::InterfaceMySQLReceiveBytes, ProfileEvents::InterfaceMySQLSendBytes));
        if (type == "postgres")
#if USE_SSL
            return TCPServerConnectionFactory::Ptr(new PostgreSQLHandlerFactory(*this, conf_name + ".", ProfileEvents::InterfacePostgreSQLReceiveBytes, ProfileEvents::InterfacePostgreSQLSendBytes));
#else
            return TCPServerConnectionFactory::Ptr(new PostgreSQLHandlerFactory(*this, ProfileEvents::InterfacePostgreSQLReceiveBytes, ProfileEvents::InterfacePostgreSQLSendBytes));
#endif
        if (type == "http")
            return TCPServerConnectionFactory::Ptr(
                new HTTPServerConnectionFactory(httpContext(), http_params, createHandlerFactory(*this, config, async_metrics, "HTTPHandler-factory"), ProfileEvents::InterfaceHTTPReceiveBytes, ProfileEvents::InterfaceHTTPSendBytes)
            );
        if (type == "prometheus")
            return TCPServerConnectionFactory::Ptr(
                new HTTPServerConnectionFactory(httpContext(), http_params, createHandlerFactory(*this, config, async_metrics, "PrometheusHandler-factory"), ProfileEvents::InterfacePrometheusReceiveBytes, ProfileEvents::InterfacePrometheusSendBytes)
            );
        if (type == "interserver")
            return TCPServerConnectionFactory::Ptr(
                new HTTPServerConnectionFactory(httpContext(), http_params, createHandlerFactory(*this, config, async_metrics, "InterserverIOHTTPHandler-factory"), ProfileEvents::InterfaceInterserverReceiveBytes, ProfileEvents::InterfaceInterserverSendBytes)
            );

        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol configuration error, unknown protocol name '{}'", type);
    };

    std::string conf_name = "protocols." + protocol;
    std::string prefix = conf_name + ".";
    std::unordered_set<std::string> pset {conf_name};

    auto stack = std::make_unique<TCPProtocolStackFactory>(*this, conf_name);

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
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' configuration contains a loop on '{}'", protocol, conf_name);
    }

    return stack;
}

HTTPContextPtr Server::httpContext() const
{
    return std::make_shared<HTTPContext>(context());
}

void Server::createServers(
    Poco::Util::AbstractConfiguration & config,
    const Strings & listen_hosts,
    bool listen_try,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    std::vector<ProtocolServerAdapter> & servers,
    bool start_servers,
    const ServerType & server_type)
{
    const Settings & settings = global_context->getSettingsRef();

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings[Setting::http_receive_timeout]);
    http_params->setKeepAliveTimeout(global_context->getServerSettings()[ServerSetting::keep_alive_timeout]);
    http_params->setMaxKeepAliveRequests(static_cast<int>(global_context->getServerSettings()[ServerSetting::max_keep_alive_requests]));
    http_params->setMaxQueued(config.getUInt("listen_backlog", DEFAULT_LISTEN_BACKLOG));

    Poco::Util::AbstractConfiguration::Keys protocols;
    config.keys("protocols", protocols);

    const TCPServerConnectionFilter::Ptr & connection_filter = new TCPServerConnectionFilter{[&]()
    {
        return !ProfileEvents::checkCPUOverload(global_context->getServerSettings()[ServerSetting::os_cpu_busy_time_threshold],
                global_context->getMinOSCPUWaitTimeRatioToDropConnection(),
                global_context->getMaxOSCPUWaitTimeRatioToDropConnection(),
                /*should_throw*/ false);
    }};

    for (const auto & protocol : protocols)
    {
        if (!server_type.shouldStart(ServerType::Type::CUSTOM, protocol))
            continue;

        std::string prefix = "protocols." + protocol + ".";
        std::string port_name = prefix + "port";
        std::string description {"<undefined> protocol"};
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
            auto stack = buildProtocolStackFromConfig(config, protocol, http_params, async_metrics, is_secure);

            if (stack->empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' stack empty", protocol);

            createServer(config, host, port_name.c_str(), listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(config, socket, host, port, is_secure);
                socket.setReceiveTimeout(settings[Setting::receive_timeout]);
                socket.setSendTimeout(settings[Setting::send_timeout]);

                return ProtocolServerAdapter(
                    host,
                    port_name.c_str(),
                    description + ": " + address.toString(),
                    std::make_unique<TCPServer>(
                        stack.release(),
                        server_pool,
                        socket,
                        makeServerParams(config),
                        connection_filter));
            });
        }
    }

    for (const auto & listen_host : listen_hosts)
    {
        const char * port_name;

        if (server_type.shouldStart(ServerType::Type::HTTP))
        {
            /// HTTP
            port_name = "http_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
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
                        httpContext(), createHandlerFactory(*this, config, async_metrics, "HTTPHandler-factory"), server_pool, socket, http_params, connection_filter, ProfileEvents::InterfaceHTTPReceiveBytes, ProfileEvents::InterfaceHTTPSendBytes));
            });
        }

        if (server_type.shouldStart(ServerType::Type::HTTPS))
        {
            /// HTTPS
            port_name = "https_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                socket.setSendTimeout(settings[Setting::http_send_timeout]);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "https://" + address.toString(),
                    std::make_unique<HTTPServer>(
                        httpContext(), createHandlerFactory(*this, config, async_metrics, "HTTPSHandler-factory"), server_pool, socket, http_params, connection_filter, ProfileEvents::InterfaceHTTPReceiveBytes, ProfileEvents::InterfaceHTTPSendBytes));
#else
                UNUSED(port);
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "HTTPS protocol is disabled because Poco library was built without NetSSL support.");
#endif
            });
        }

        if (server_type.shouldStart(ServerType::Type::TCP))
        {
            /// TCP
            port_name = "tcp_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
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
                        new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ false, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes),
                        server_pool,
                        socket,
                        makeServerParams(config),
                        connection_filter));
            });
        }

        if (server_type.shouldStart(ServerType::Type::TCP_WITH_PROXY))
        {
            /// TCP with PROXY protocol, see https://github.com/wolfeidau/proxyv2/blob/master/docs/proxy-protocol.txt
            port_name = "tcp_with_proxy_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
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
                        new TCPHandlerFactory(*this, /* secure */ false, /* proxy protocol */ true, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes),
                        server_pool,
                        socket,
                        makeServerParams(config),
                        connection_filter));
            });
        }

        if (server_type.shouldStart(ServerType::Type::TCP_SECURE))
        {
            /// TCP with SSL
            port_name = "tcp_port_secure";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
    #if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings[Setting::receive_timeout]);
                socket.setSendTimeout(settings[Setting::send_timeout]);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "secure native protocol (tcp_secure): " + address.toString(),
                    std::make_unique<TCPServer>(
                        new TCPHandlerFactory(*this, /* secure */ true, /* proxy protocol */ false, ProfileEvents::InterfaceNativeReceiveBytes, ProfileEvents::InterfaceNativeSendBytes),
                        server_pool,
                        socket,
                        makeServerParams(config),
                        connection_filter));
    #else
                UNUSED(port);
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
    #endif
            });
        }

        if (server_type.shouldStart(ServerType::Type::TCP_SSH))
        {
            port_name = "tcp_ssh_port";
            createServer(
                config,
                listen_host,
                port_name,
                listen_try,
                start_servers,
                servers,
                [&](UInt16 port) -> ProtocolServerAdapter
                {
#if USE_SSH && defined(OS_LINUX)
                    Poco::Net::ServerSocket socket;
                    auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ false);
                    return ProtocolServerAdapter(
                        listen_host,
                        port_name,
                        "SSH PTY: " + address.toString(),
                        std::make_unique<TCPServer>(
                            new SSHPtyHandlerFactory(*this, config),
                            server_pool,
                            socket,
                            makeServerParams(config),
                            connection_filter));
#else
                UNUSED(port);
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSH protocol is disabled for ClickHouse, as it has been either built without libssh or not for Linux");
#endif
                });
        }

        if (server_type.shouldStart(ServerType::Type::MYSQL))
        {
            port_name = "mysql_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(Poco::Timespan());
                socket.setSendTimeout(settings[Setting::send_timeout]);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "MySQL compatibility protocol: " + address.toString(),
                    std::make_unique<TCPServer>(
                         new MySQLHandlerFactory(*this, ProfileEvents::InterfaceMySQLReceiveBytes, ProfileEvents::InterfaceMySQLSendBytes),
                         server_pool,
                         socket,
                         makeServerParams(config),
                         connection_filter));
            });
        }

        if (server_type.shouldStart(ServerType::Type::POSTGRESQL))
        {
            port_name = "postgresql_port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(config, socket, listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(Poco::Timespan());
                socket.setSendTimeout(settings[Setting::send_timeout]);
                return ProtocolServerAdapter(
                    listen_host,
                    port_name,
                    "PostgreSQL compatibility protocol: " + address.toString(),
#if USE_SSL
                    std::make_unique<TCPServer>(
                         new PostgreSQLHandlerFactory(*this, Poco::Net::SSLManager::CFG_SERVER_PREFIX, ProfileEvents::InterfacePostgreSQLReceiveBytes, ProfileEvents::InterfacePostgreSQLSendBytes),
                         server_pool,
                         socket,
                         makeServerParams(config),
                         connection_filter));
#else
                    std::make_unique<TCPServer>(
                         new PostgreSQLHandlerFactory(*this, ProfileEvents::InterfacePostgreSQLReceiveBytes, ProfileEvents::InterfacePostgreSQLSendBytes),
                         server_pool,
                         socket,
                         makeServerParams(config),
                         connection_filter));
#endif
            });
        }

#if USE_GRPC
        if (server_type.shouldStart(ServerType::Type::GRPC))
        {
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
        }
#endif
        if (server_type.shouldStart(ServerType::Type::PROMETHEUS))
        {
            /// Prometheus (if defined and not setup yet with http_port)
            port_name = "prometheus.port";
            createServer(config, listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
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
                        httpContext(), createHandlerFactory(*this, config, async_metrics, "PrometheusHandler-factory"), server_pool, socket, http_params, nullptr, ProfileEvents::InterfacePrometheusReceiveBytes, ProfileEvents::InterfacePrometheusSendBytes));
            });
        }
    }
}

void Server::createInterserverServers(
    Poco::Util::AbstractConfiguration & config,
    const Strings & interserver_listen_hosts,
    bool listen_try,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    std::vector<ProtocolServerAdapter> & servers,
    bool start_servers,
    const ServerType & server_type)
{
    const Settings & settings = global_context->getSettingsRef();

    Poco::Net::HTTPServerParams::Ptr http_params = new Poco::Net::HTTPServerParams;
    http_params->setTimeout(settings[Setting::http_receive_timeout]);
    http_params->setKeepAliveTimeout(global_context->getServerSettings()[ServerSetting::keep_alive_timeout]);
    http_params->setMaxQueued(config.getUInt("listen_backlog", DEFAULT_LISTEN_BACKLOG));

    /// Now iterate over interserver_listen_hosts
    for (const auto & interserver_listen_host : interserver_listen_hosts)
    {
        const char * port_name;

        if (server_type.shouldStart(ServerType::Type::INTERSERVER_HTTP))
        {
            /// Interserver IO HTTP
            port_name = "interserver_http_port";
            createServer(config, interserver_listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
                Poco::Net::ServerSocket socket;
                auto address = socketBindListen(config, socket, interserver_listen_host, port);
                socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                socket.setSendTimeout(settings[Setting::http_send_timeout]);
                return ProtocolServerAdapter(
                    interserver_listen_host,
                    port_name,
                    "replica communication (interserver): http://" + address.toString(),
                    std::make_unique<HTTPServer>(
                        httpContext(),
                        createHandlerFactory(*this, config, async_metrics, "InterserverIOHTTPHandler-factory"),
                        server_pool,
                        socket,
                        http_params,
                        nullptr,
                        ProfileEvents::InterfaceInterserverReceiveBytes,
                        ProfileEvents::InterfaceInterserverSendBytes));
            });
        }

        if (server_type.shouldStart(ServerType::Type::INTERSERVER_HTTPS))
        {
            port_name = "interserver_https_port";
            createServer(config, interserver_listen_host, port_name, listen_try, start_servers, servers, [&](UInt16 port) -> ProtocolServerAdapter
            {
#if USE_SSL
                Poco::Net::SecureServerSocket socket;
                auto address = socketBindListen(config, socket, interserver_listen_host, port, /* secure = */ true);
                socket.setReceiveTimeout(settings[Setting::http_receive_timeout]);
                socket.setSendTimeout(settings[Setting::http_send_timeout]);
                return ProtocolServerAdapter(
                    interserver_listen_host,
                    port_name,
                    "secure replica communication (interserver): https://" + address.toString(),
                    std::make_unique<HTTPServer>(
                        httpContext(),
                        createHandlerFactory(*this, config, async_metrics, "InterserverIOHTTPSHandler-factory"),
                        server_pool,
                        socket,
                        http_params,
                        nullptr,
                        ProfileEvents::InterfaceInterserverReceiveBytes,
                        ProfileEvents::InterfaceInterserverSendBytes));
#else
                UNUSED(port);
                throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support for TCP protocol is disabled because Poco library was built without NetSSL support.");
#endif
            });
        }
    }
}

void Server::stopServers(
    std::vector<ProtocolServerAdapter> & servers,
    const ServerType & server_type) const
{
    LoggerRawPtr log = &logger();

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

    std::erase_if(servers, std::bind_front(check_server, " (from one of previous remove)"));

    for (auto & server : servers)
    {
        if (!server.isStopping())
        {
            const std::string server_port_name = server.getPortName();

            if (server_type.shouldStop(server_port_name))
                server.stop();
        }
    }

    std::erase_if(servers, std::bind_front(check_server, ""));
}

void Server::updateServers(
    Poco::Util::AbstractConfiguration & config,
    Poco::ThreadPool & server_pool,
    AsynchronousMetrics & async_metrics,
    std::vector<ProtocolServerAdapter> & servers,
    std::vector<ProtocolServerAdapter> & servers_to_start_before_tables)
{
    LoggerRawPtr log = &logger();

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

    Poco::Util::AbstractConfiguration & previous_config = latest_config ? *latest_config : this->config();

    std::vector<ProtocolServerAdapter *> all_servers;
    all_servers.reserve(servers.size() + servers_to_start_before_tables.size());
    for (auto & server : servers)
        all_servers.push_back(&server);

    for (auto & server : servers_to_start_before_tables)
        all_servers.push_back(&server);

    for (auto * server : all_servers)
    {
        if (server->supportsRuntimeReconfiguration() && !server->isStopping())
        {
            std::string port_name = server->getPortName();
            bool has_host = false;
            bool is_http = false;
            if (port_name.starts_with("protocols."))
            {
                std::string protocol = port_name.substr(0, port_name.find_last_of('.'));
                has_host = config.has(protocol + ".host");

                std::string conf_name = protocol;
                std::string prefix = protocol + ".";
                std::unordered_set<std::string> pset {conf_name};
                while (true)
                {
                    if (config.has(prefix + "type"))
                    {
                        std::string type = config.getString(prefix + "type");
                        if (type == "http")
                        {
                            is_http = true;
                            break;
                        }
                    }

                    if (!config.has(prefix + "impl"))
                        break;

                    conf_name = "protocols." + config.getString(prefix + "impl");
                    prefix = conf_name + ".";

                    if (!pset.insert(conf_name).second)
                        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Protocol '{}' configuration contains a loop on '{}'", protocol, conf_name);
                }
            }
            else
            {
                /// NOTE: better to compare using getPortName() over using
                /// dynamic_cast<> since HTTPServer is also used for prometheus and
                /// internal replication communications.
                is_http = server->getPortName() == "http_port" || server->getPortName() == "https_port";
            }

            if (!has_host)
                has_host = std::find(listen_hosts.begin(), listen_hosts.end(), server->getListenHost()) != listen_hosts.end();
            bool has_port = !config.getString(port_name, "").empty();
            bool force_restart = is_http && !isSameConfiguration(previous_config, config, "http_handlers");
            if (force_restart)
                LOG_TRACE(log, "<http_handlers> had been changed, will reload {}", server->getDescription());

            if (!has_host || !has_port || config.getInt(server->getPortName()) != server->portNumber() || force_restart)
            {
                server->stop();
                LOG_INFO(log, "Stopped listening for {}", server->getDescription());
            }
        }
    }

    createServers(config, listen_hosts, listen_try, server_pool, async_metrics, servers, /* start_servers= */ true);
    createInterserverServers(config, interserver_listen_hosts, listen_try, server_pool, async_metrics, servers_to_start_before_tables, /* start_servers= */ true);

    std::erase_if(servers, std::bind_front(check_server, ""));
    std::erase_if(servers_to_start_before_tables, std::bind_front(check_server, ""));
}

}
