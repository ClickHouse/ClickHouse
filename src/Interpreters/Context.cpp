#include <map>
#include <set>
#include <optional>
#include <memory>
#include <Poco/Mutex.h>
#include <Poco/UUID.h>
#include <Poco/Net/IPAddress.h>
#include <Poco/Util/Application.h>
#include <Common/Macros.h>
#include <Common/escapeForFileName.h>
#include <Common/EventNotifier.h>
#include <Common/setThreadName.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/Throttler.h>
#include <Common/thread_local_rng.h>
#include <Common/FieldVisitorToString.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Coordination/KeeperDispatcher.h>
#include <Compression/ICompressionCodec.h>
#include <Core/BackgroundSchedulePool.h>
#include <Formats/FormatFactory.h>
#include <Databases/IDatabase.h>
#include <Storages/IStorage.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MovesList.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/CompressionCodecSelector.h>
#include <Storages/StorageS3Settings.h>
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/IO/ThreadPoolRemoteFSReader.h>
#include <Disks/IO/ThreadPoolReader.h>
#include <Disks/StoragePolicy.h>
#include <IO/SynchronousReader.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/Cache/QueryCache.h>
#include <Core/ServerSettings.h>
#include <Core/Settings.h>
#include <Core/SettingsQuirks.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/Credentials.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <IO/ResourceManagerFactory.h>
#include <Backups/BackupsWorker.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsLoader.h>
#include <Functions/UserDefined/createUserDefinedSQLObjectsLoader.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/SessionLog.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/Session.h>
#include <Interpreters/TraceCollector.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>
#include <IO/WriteSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/StackTrace.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <base/EnumReflection.h>
#include <Common/RemoteHostFilter.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/MergeTree/MergeTreeMetadataCache.h>
#include <Interpreters/SynonymsExtensions.h>
#include <Interpreters/Lemmatizers.h>
#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/TransactionLog.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <filesystem>
#include <re2/re2.h>
#include <Storages/StorageView.h>
#include <Parsers/ASTFunction.h>
#include <base/find_symbols.h>

#include <Interpreters/Cache/FileCache.h>

#if USE_ROCKSDB
#include <rocksdb/table.h>
#endif

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event ContextLock;
}

namespace CurrentMetrics
{
    extern const Metric ContextLockWait;
    extern const Metric BackgroundMovePoolTask;
    extern const Metric BackgroundMovePoolSize;
    extern const Metric BackgroundSchedulePoolTask;
    extern const Metric BackgroundSchedulePoolSize;
    extern const Metric BackgroundBufferFlushSchedulePoolTask;
    extern const Metric BackgroundBufferFlushSchedulePoolSize;
    extern const Metric BackgroundDistributedSchedulePoolTask;
    extern const Metric BackgroundDistributedSchedulePoolSize;
    extern const Metric BackgroundMessageBrokerSchedulePoolTask;
    extern const Metric BackgroundMessageBrokerSchedulePoolSize;
    extern const Metric BackgroundMergesAndMutationsPoolTask;
    extern const Metric BackgroundMergesAndMutationsPoolSize;
    extern const Metric BackgroundFetchesPoolTask;
    extern const Metric BackgroundFetchesPoolSize;
    extern const Metric BackgroundCommonPoolTask;
    extern const Metric BackgroundCommonPoolSize;
    extern const Metric MarksLoaderThreads;
    extern const Metric MarksLoaderThreadsActive;
    extern const Metric IOPrefetchThreads;
    extern const Metric IOPrefetchThreadsActive;
    extern const Metric IOWriterThreads;
    extern const Metric IOWriterThreadsActive;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int BAD_GET;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int THERE_IS_NO_SESSION;
    extern const int THERE_IS_NO_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SETTING_VALUE;
    extern const int UNKNOWN_READ_METHOD;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_FUNCTION;
}


/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextSharedPart : boost::noncopyable
{
    Poco::Logger * log = &Poco::Logger::get("Context");

    /// For access of most of shared objects. Recursive mutex.
    mutable std::recursive_mutex mutex;
    /// Separate mutex for access of dictionaries. Separate mutex to avoid locks when server doing request to itself.
    mutable std::mutex embedded_dictionaries_mutex;
    mutable std::mutex external_dictionaries_mutex;
    mutable std::mutex external_user_defined_executable_functions_mutex;
    /// Separate mutex for storage policies. During server startup we may
    /// initialize some important storages (system logs with MergeTree engine)
    /// under context lock.
    mutable std::mutex storage_policies_mutex;
    /// Separate mutex for re-initialization of zookeeper session. This operation could take a long time and must not interfere with another operations.
    mutable std::mutex zookeeper_mutex;

    mutable zkutil::ZooKeeperPtr zookeeper;                 /// Client for ZooKeeper.
    ConfigurationPtr zookeeper_config;                      /// Stores zookeeper configs

#if USE_NURAFT
    mutable std::mutex keeper_dispatcher_mutex;
    mutable std::shared_ptr<KeeperDispatcher> keeper_dispatcher;
#endif
    mutable std::mutex auxiliary_zookeepers_mutex;
    mutable std::map<String, zkutil::ZooKeeperPtr> auxiliary_zookeepers;    /// Map for auxiliary ZooKeeper clients.
    ConfigurationPtr auxiliary_zookeepers_config;           /// Stores auxiliary zookeepers configs

    String interserver_io_host;                             /// The host name by which this server is available for other servers.
    UInt16 interserver_io_port = 0;                         /// and port.
    String interserver_scheme;                              /// http or https
    MultiVersion<InterserverCredentials> interserver_io_credentials;

    String path;                                            /// Path to the data directory, with a slash at the end.
    String flags_path;                                      /// Path to the directory with some control flags for server maintenance.
    String user_files_path;                                 /// Path to the directory with user provided files, usable by 'file' table function.
    String dictionaries_lib_path;                           /// Path to the directory with user provided binaries and libraries for external dictionaries.
    String user_scripts_path;                               /// Path to the directory with user provided scripts.
    ConfigurationPtr config;                                /// Global configuration settings.

    String tmp_path;                                        /// Path to the temporary files that occur when processing the request.
    TemporaryDataOnDiskScopePtr temp_data_on_disk;          /// Temporary files that occur when processing the request accounted here.

    mutable std::unique_ptr<EmbeddedDictionaries> embedded_dictionaries;    /// Metrica's dictionaries. Have lazy initialization.
    mutable std::unique_ptr<ExternalDictionariesLoader> external_dictionaries_loader;

    scope_guard models_repository_guard;

    ExternalLoaderXMLConfigRepository * external_dictionaries_config_repository = nullptr;
    scope_guard dictionaries_xmls;

    mutable std::unique_ptr<ExternalUserDefinedExecutableFunctionsLoader> external_user_defined_executable_functions_loader;
    ExternalLoaderXMLConfigRepository * user_defined_executable_functions_config_repository = nullptr;
    scope_guard user_defined_executable_functions_xmls;

    mutable std::unique_ptr<IUserDefinedSQLObjectsLoader> user_defined_sql_objects_loader;

#if USE_NLP
    mutable std::optional<SynonymsExtensions> synonyms_extensions;
    mutable std::optional<Lemmatizers> lemmatizers;
#endif

    std::optional<BackupsWorker> backups_worker;

    String default_profile_name;                            /// Default profile name used for default values.
    String system_profile_name;                             /// Profile used by system processes
    String buffer_profile_name;                             /// Profile used by Buffer engine for flushing to the underlying
    std::unique_ptr<AccessControl> access_control;
    mutable ResourceManagerPtr resource_manager;
    mutable UncompressedCachePtr uncompressed_cache;        /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache;                        /// Cache of marks in compressed files.
    mutable std::unique_ptr<ThreadPool> load_marks_threadpool; /// Threadpool for loading marks cache.
    mutable std::unique_ptr<ThreadPool> prefetch_threadpool; /// Threadpool for loading marks cache.
    mutable UncompressedCachePtr index_uncompressed_cache;  /// The cache of decompressed blocks for MergeTree indices.
    mutable MarkCachePtr index_mark_cache;                  /// Cache of marks in compressed files of MergeTree indices.
    mutable QueryCachePtr query_cache;         /// Cache of query results.
    mutable MMappedFileCachePtr mmap_cache; /// Cache of mmapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
    ProcessList process_list;                               /// Executing queries at the moment.
    GlobalOvercommitTracker global_overcommit_tracker;
    MergeList merge_list;                                   /// The list of executable merge (for (Replicated)?MergeTree)
    MovesList moves_list;                                   /// The list of executing moves (for (Replicated)?MergeTree)
    ReplicatedFetchList replicated_fetch_list;
    ConfigurationPtr users_config;                          /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;            /// Handler for interserver communication.

    mutable std::unique_ptr<BackgroundSchedulePool> buffer_flush_schedule_pool; /// A thread pool that can do background flush for Buffer tables.
    mutable std::unique_ptr<BackgroundSchedulePool> schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    mutable std::unique_ptr<BackgroundSchedulePool> distributed_schedule_pool; /// A thread pool that can run different jobs in background (used for distributed sends)
    mutable std::unique_ptr<BackgroundSchedulePool> message_broker_schedule_pool; /// A thread pool that can run different jobs in background (used for message brokers, like RabbitMQ and Kafka)

    mutable std::unique_ptr<IAsynchronousReader> asynchronous_remote_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_local_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> synchronous_local_fs_reader;

    mutable std::unique_ptr<ThreadPool> threadpool_writer;

    mutable ThrottlerPtr replicated_fetches_throttler;      /// A server-wide throttler for replicated fetches
    mutable ThrottlerPtr replicated_sends_throttler;        /// A server-wide throttler for replicated sends
    mutable ThrottlerPtr remote_read_throttler;             /// A server-wide throttler for remote IO reads
    mutable ThrottlerPtr remote_write_throttler;            /// A server-wide throttler for remote IO writes

    MultiVersion<Macros> macros;                            /// Substitutions extracted from config.
    std::unique_ptr<DDLWorker> ddl_worker;                  /// Process ddl commands from zk.
    /// Rules for selecting the compression settings, depending on the size of the part.
    mutable std::unique_ptr<CompressionCodecSelector> compression_codec_selector;
    /// Storage disk chooser for MergeTree engines
    mutable std::shared_ptr<const DiskSelector> merge_tree_disk_selector;
    /// Storage policy chooser for MergeTree engines
    mutable std::shared_ptr<const StoragePolicySelector> merge_tree_storage_policy_selector;

    std::optional<MergeTreeSettings> merge_tree_settings;   /// Settings of MergeTree* engines.
    std::optional<MergeTreeSettings> replicated_merge_tree_settings;   /// Settings of ReplicatedMergeTree* engines.
    std::atomic_size_t max_table_size_to_drop = 50000000000lu; /// Protects MergeTree tables from accidental DROP (50GB by default)
    std::atomic_size_t max_partition_size_to_drop = 50000000000lu; /// Protects MergeTree partitions from accidental DROP (50GB by default)
    String format_schema_path;                              /// Path to a directory that contains schema files used by input formats.
    ActionLocksManagerPtr action_locks_manager;             /// Set of storages' action lockers
    std::unique_ptr<SystemLogs> system_logs;                /// Used to log queries and operations on parts
    std::optional<StorageS3Settings> storage_s3_settings;   /// Settings of S3 storage
    std::vector<String> warnings;                           /// Store warning messages about server configuration.

    /// Background executors for *MergeTree tables
    MergeMutateBackgroundExecutorPtr merge_mutate_executor;
    OrdinaryBackgroundExecutorPtr moves_executor;
    OrdinaryBackgroundExecutorPtr fetch_executor;
    OrdinaryBackgroundExecutorPtr common_executor;

    RemoteHostFilter remote_host_filter; /// Allowed URL from config.xml

    std::optional<TraceCollector> trace_collector;        /// Thread collecting traces from threads executing queries

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    std::shared_ptr<Clusters> clusters;
    ConfigurationPtr clusters_config;                        /// Stores updated configs
    mutable std::mutex clusters_mutex;                       /// Guards clusters and clusters_config
    std::unique_ptr<ClusterDiscovery> cluster_discovery;

    std::shared_ptr<AsynchronousInsertQueue> async_insert_queue;
    std::map<String, UInt16> server_ports;

    bool shutdown_called = false;

    /// Has background executors for MergeTree tables been initialized?
    bool are_background_executors_initialized = false;

    Stopwatch uptime_watch;

    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    /// vector of xdbc-bridge commands, they will be killed when Context will be destroyed
    std::vector<std::unique_ptr<ShellCommand>> bridge_commands;

    Context::ConfigReloadCallback config_reload_callback;

    bool is_server_completely_started = false;

#if USE_ROCKSDB
    /// Global merge tree metadata cache, stored in rocksdb.
    MergeTreeMetadataCachePtr merge_tree_metadata_cache;
#endif

    ContextSharedPart()
        : access_control(std::make_unique<AccessControl>())
        , global_overcommit_tracker(&process_list)
        , macros(std::make_unique<Macros>())
    {
        /// TODO: make it singleton (?)
        static std::atomic<size_t> num_calls{0};
        if (++num_calls > 1)
        {
            std::cerr << "Attempting to create multiple ContextShared instances. Stack trace:\n" << StackTrace().toString();
            std::cerr.flush();
            std::terminate();
        }
    }


    ~ContextSharedPart()
    {
        /// Wait for thread pool for background reads and writes,
        /// since it may use per-user MemoryTracker which will be destroyed here.
        if (asynchronous_remote_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing remote fs threadpool reader");
                asynchronous_remote_fs_reader->wait();
                asynchronous_remote_fs_reader.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (asynchronous_local_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing local fs threadpool reader");
                asynchronous_local_fs_reader->wait();
                asynchronous_local_fs_reader.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (synchronous_local_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing local fs threadpool reader");
                synchronous_local_fs_reader->wait();
                synchronous_local_fs_reader.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (threadpool_writer)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing threadpool writer");
                threadpool_writer->wait();
                threadpool_writer.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (load_marks_threadpool)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing marks loader");
                load_marks_threadpool->wait();
                load_marks_threadpool.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        if (prefetch_threadpool)
        {
            try
            {
                LOG_DEBUG(log, "Desctructing prefetch threadpool");
                prefetch_threadpool->wait();
                prefetch_threadpool.reset();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }

        try
        {
            shutdown();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }


    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown()
    {
        if (shutdown_called)
            return;
        shutdown_called = true;

        /// Stop periodic reloading of the configuration files.
        /// This must be done first because otherwise the reloading may pass a changed config
        /// to some destroyed parts of ContextSharedPart.
        if (external_dictionaries_loader)
            external_dictionaries_loader->enablePeriodicUpdates(false);
        if (external_user_defined_executable_functions_loader)
            external_user_defined_executable_functions_loader->enablePeriodicUpdates(false);
        if (user_defined_sql_objects_loader)
            user_defined_sql_objects_loader->stopWatching();

        Session::shutdownNamedSessions();

        /// Waiting for current backups/restores to be finished. This must be done before `DatabaseCatalog::shutdown()`.
        if (backups_worker)
            backups_worker->shutdown();

        /**  After system_logs have been shut down it is guaranteed that no system table gets created or written to.
          *  Note that part changes at shutdown won't be logged to part log.
          */
        if (system_logs)
            system_logs->shutdown();

        DatabaseCatalog::shutdown();

        if (merge_mutate_executor)
            merge_mutate_executor->wait();
        if (fetch_executor)
            fetch_executor->wait();
        if (moves_executor)
            moves_executor->wait();
        if (common_executor)
            common_executor->wait();

        TransactionLog::shutdownIfAny();

        std::unique_ptr<SystemLogs> delete_system_logs;
        std::unique_ptr<EmbeddedDictionaries> delete_embedded_dictionaries;
        std::unique_ptr<ExternalDictionariesLoader> delete_external_dictionaries_loader;
        std::unique_ptr<ExternalUserDefinedExecutableFunctionsLoader> delete_external_user_defined_executable_functions_loader;
        std::unique_ptr<IUserDefinedSQLObjectsLoader> delete_user_defined_sql_objects_loader;
        std::unique_ptr<BackgroundSchedulePool> delete_buffer_flush_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_distributed_schedule_pool;
        std::unique_ptr<BackgroundSchedulePool> delete_message_broker_schedule_pool;
        std::unique_ptr<DDLWorker> delete_ddl_worker;
        std::unique_ptr<AccessControl> delete_access_control;

        /// Delete DDLWorker before zookeeper.
        /// Cause it can call Context::getZooKeeper and resurrect it.

        {
            auto lock = std::lock_guard(mutex);
            delete_ddl_worker = std::move(ddl_worker);
        }

        /// DDLWorker should be deleted without lock, cause its internal thread can
        /// take it as well, which will cause deadlock.
        delete_ddl_worker.reset();

        {
            auto lock = std::lock_guard(mutex);

            /** Compiled expressions stored in cache need to be destroyed before destruction of static objects.
              * Because CHJIT instance can be static object.
              */
#if USE_EMBEDDED_COMPILER
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->reset();
#endif

            /// Preemptive destruction is important, because these objects may have a refcount to ContextShared (cyclic reference).
            /// TODO: Get rid of this.

            /// Dictionaries may be required:
            /// - for storage shutdown (during final flush of the Buffer engine)
            /// - before storage startup (because of some streaming of, i.e. Kafka, to
            ///   the table with materialized column that has dictGet)
            ///
            /// So they should be created before any storages and preserved until storages will be terminated.
            ///
            /// But they cannot be created before storages since they may required table as a source,
            /// but at least they can be preserved for storage termination.
            dictionaries_xmls.reset();
            user_defined_executable_functions_xmls.reset();
            models_repository_guard.reset();

            delete_system_logs = std::move(system_logs);
            delete_embedded_dictionaries = std::move(embedded_dictionaries);
            delete_external_dictionaries_loader = std::move(external_dictionaries_loader);
            delete_external_user_defined_executable_functions_loader = std::move(external_user_defined_executable_functions_loader);
            delete_user_defined_sql_objects_loader = std::move(user_defined_sql_objects_loader);
            delete_buffer_flush_schedule_pool = std::move(buffer_flush_schedule_pool);
            delete_schedule_pool = std::move(schedule_pool);
            delete_distributed_schedule_pool = std::move(distributed_schedule_pool);
            delete_message_broker_schedule_pool = std::move(message_broker_schedule_pool);
            delete_access_control = std::move(access_control);

            /// Stop trace collector if any
            trace_collector.reset();
            /// Stop zookeeper connection
            zookeeper.reset();

#if USE_ROCKSDB
            /// Shutdown merge tree metadata cache
            if (merge_tree_metadata_cache)
            {
                merge_tree_metadata_cache->shutdown();
                merge_tree_metadata_cache.reset();
            }
#endif
        }

        /// Can be removed without context lock
        delete_system_logs.reset();
        delete_embedded_dictionaries.reset();
        delete_external_dictionaries_loader.reset();
        delete_external_user_defined_executable_functions_loader.reset();
        delete_user_defined_sql_objects_loader.reset();
        delete_ddl_worker.reset();
        delete_buffer_flush_schedule_pool.reset();
        delete_schedule_pool.reset();
        delete_distributed_schedule_pool.reset();
        delete_message_broker_schedule_pool.reset();
        delete_access_control.reset();

        total_memory_tracker.resetOvercommitTracker();
    }

    bool hasTraceCollector() const
    {
        return trace_collector.has_value();
    }

    void initializeTraceCollector(std::shared_ptr<TraceLog> trace_log)
    {
        if (!trace_log)
            return;
        if (hasTraceCollector())
            return;

        trace_collector.emplace(std::move(trace_log));
    }

    void addWarningMessage(const String & message)
    {
        /// A warning goes both: into server's log; stored to be placed in `system.warnings` table.
        log->warning(message);
        warnings.push_back(message);
    }
};


Context::Context() = default;
Context::Context(const Context &) = default;
Context & Context::operator=(const Context &) = default;

SharedContextHolder::SharedContextHolder(SharedContextHolder &&) noexcept = default;
SharedContextHolder & SharedContextHolder::operator=(SharedContextHolder &&) noexcept = default;
SharedContextHolder::SharedContextHolder() = default;
SharedContextHolder::~SharedContextHolder() = default;
SharedContextHolder::SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context)
    : shared(std::move(shared_context)) {}

void SharedContextHolder::reset() { shared.reset(); }

ContextMutablePtr Context::createGlobal(ContextSharedPart * shared)
{
    auto res = std::shared_ptr<Context>(new Context);
    res->shared = shared;
    return res;
}

void Context::initGlobal()
{
    assert(!global_context_instance);
    global_context_instance = shared_from_this();
    DatabaseCatalog::init(shared_from_this());
    EventNotifier::init();
}

SharedContextHolder Context::createShared()
{
    return SharedContextHolder(std::make_unique<ContextSharedPart>());
}

ContextMutablePtr Context::createCopy(const ContextPtr & other)
{
    return std::shared_ptr<Context>(new Context(*other));
}

ContextMutablePtr Context::createCopy(const ContextWeakPtr & other)
{
    auto ptr = other.lock();
    if (!ptr) throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't copy an expired context");
    return createCopy(ptr);
}

ContextMutablePtr Context::createCopy(const ContextMutablePtr & other)
{
    return createCopy(std::const_pointer_cast<const Context>(other));
}

Context::~Context() = default;

InterserverIOHandler & Context::getInterserverIOHandler() { return shared->interserver_io_handler; }
const InterserverIOHandler & Context::getInterserverIOHandler() const { return shared->interserver_io_handler; }

std::unique_lock<std::recursive_mutex> Context::getLock() const
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    return std::unique_lock(shared->mutex);
}

ProcessList & Context::getProcessList() { return shared->process_list; }
const ProcessList & Context::getProcessList() const { return shared->process_list; }
OvercommitTracker * Context::getGlobalOvercommitTracker() const { return &shared->global_overcommit_tracker; }
MergeList & Context::getMergeList() { return shared->merge_list; }
const MergeList & Context::getMergeList() const { return shared->merge_list; }
MovesList & Context::getMovesList() { return shared->moves_list; }
const MovesList & Context::getMovesList() const { return shared->moves_list; }
ReplicatedFetchList & Context::getReplicatedFetchList() { return shared->replicated_fetch_list; }
const ReplicatedFetchList & Context::getReplicatedFetchList() const { return shared->replicated_fetch_list; }

String Context::resolveDatabase(const String & database_name) const
{
    String res = database_name.empty() ? getCurrentDatabase() : database_name;
    if (res.empty())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected");
    return res;
}

String Context::getPath() const
{
    auto lock = getLock();
    return shared->path;
}

String Context::getFlagsPath() const
{
    auto lock = getLock();
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    auto lock = getLock();
    return shared->user_files_path;
}

String Context::getDictionariesLibPath() const
{
    auto lock = getLock();
    return shared->dictionaries_lib_path;
}

String Context::getUserScriptsPath() const
{
    auto lock = getLock();
    return shared->user_scripts_path;
}

Strings Context::getWarnings() const
{
    Strings common_warnings;
    {
        auto lock = getLock();
        common_warnings = shared->warnings;
    }
    for (const auto & setting : settings)
    {
        if (setting.isValueChanged() && setting.isObsolete())
        {
            common_warnings.emplace_back("Some obsolete setting is changed. "
                                         "Check 'select * from system.settings where changed' and read the changelog.");
            break;
        }
    }
    return common_warnings;
}

/// TODO: remove, use `getTempDataOnDisk`
VolumePtr Context::getTemporaryVolume() const
{
    auto lock = getLock();
    if (shared->temp_data_on_disk)
        return shared->temp_data_on_disk->getVolume();
    return nullptr;
}

TemporaryDataOnDiskScopePtr Context::getTempDataOnDisk() const
{
    auto lock = getLock();
    if (this->temp_data_on_disk)
        return this->temp_data_on_disk;
    return shared->temp_data_on_disk;
}

void Context::setTempDataOnDisk(TemporaryDataOnDiskScopePtr temp_data_on_disk_)
{
    auto lock = getLock();
    this->temp_data_on_disk = std::move(temp_data_on_disk_);
}

void Context::setPath(const String & path)
{
    auto lock = getLock();

    shared->path = path;

    if (shared->tmp_path.empty() && !shared->temp_data_on_disk)
        shared->tmp_path = shared->path + "tmp/";

    if (shared->flags_path.empty())
        shared->flags_path = shared->path + "flags/";

    if (shared->user_files_path.empty())
        shared->user_files_path = shared->path + "user_files/";

    if (shared->dictionaries_lib_path.empty())
        shared->dictionaries_lib_path = shared->path + "dictionaries_lib/";

    if (shared->user_scripts_path.empty())
        shared->user_scripts_path = shared->path + "user_scripts/";
}

static void setupTmpPath(Poco::Logger * log, const std::string & path)
try
{
    LOG_DEBUG(log, "Setting up {} to store temporary data in it", path);

    fs::create_directories(path);

    /// Clearing old temporary files.
    fs::directory_iterator dir_end;
    for (fs::directory_iterator it(path); it != dir_end; ++it)
    {
        if (it->is_regular_file())
        {
            if (startsWith(it->path().filename(), "tmp"))
            {
                LOG_DEBUG(log, "Removing old temporary file {}", it->path().string());
                fs::remove(it->path());
            }
            else
                LOG_DEBUG(log, "Found unknown file in temporary path {}", it->path().string());
        }
        /// We skip directories (for example, 'http_buffers' - it's used for buffering of the results) and all other file types.
    }
}
catch (...)
{
    DB::tryLogCurrentException(log, fmt::format(
        "Caught exception while setup temporary path: {}. "
        "It is ok to skip this exception as cleaning old temporary files is not necessary", path));
}

static VolumePtr createLocalSingleDiskVolume(const std::string & path)
{
    auto disk = std::make_shared<DiskLocal>("_tmp_default", path, 0);
    VolumePtr volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    return volume;
}

void Context::setTemporaryStoragePath(const String & path, size_t max_size)
{
    shared->tmp_path = path;
    if (!shared->tmp_path.ends_with('/'))
        shared->tmp_path += '/';

    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path);

    for (const auto & disk : volume->getDisks())
    {
        setupTmpPath(shared->log, disk->getPath());
    }

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);
}

void Context::setTemporaryStoragePolicy(const String & policy_name, size_t max_size)
{
    std::lock_guard lock(shared->storage_policies_mutex);

     StoragePolicyPtr tmp_policy = getStoragePolicySelector(lock)->get(policy_name);
    if (tmp_policy->getVolumes().size() != 1)
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
            "Policy '{}' is used temporary files, such policy should have exactly one volume", policy_name);
    VolumePtr volume = tmp_policy->getVolume(0);

    if (volume->getDisks().empty())
         throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "No disks volume for temporary files");

    for (const auto & disk : volume->getDisks())
    {
        if (!disk)
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Temporary disk is null");

        /// Check that underlying disk is local (can be wrapped in decorator)
        DiskPtr disk_ptr = disk;

        if (dynamic_cast<const DiskLocal *>(disk_ptr.get()) == nullptr)
        {
            const auto * disk_raw_ptr = disk_ptr.get();
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                "Disk '{}' ({}) is not local and can't be used for temporary files",
                disk_ptr->getName(), typeid(*disk_raw_ptr).name());
        }

        setupTmpPath(shared->log, disk->getPath());
    }

    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, max_size);
}


void Context::setTemporaryStorageInCache(const String & cache_disk_name, size_t max_size)
{
    auto disk_ptr = getDisk(cache_disk_name);
    if (!disk_ptr)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Disk '{}' is not found", cache_disk_name);

    const auto * disk_object_storage_ptr = dynamic_cast<const DiskObjectStorage *>(disk_ptr.get());
    if (!disk_object_storage_ptr)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Disk '{}' does not use cache", cache_disk_name);

    auto file_cache = disk_object_storage_ptr->getCache();
    if (!file_cache)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Cache '{}' is not found", file_cache->getBasePath());

    LOG_DEBUG(shared->log, "Using file cache ({}) for temporary files", file_cache->getBasePath());

    shared->tmp_path = file_cache->getBasePath();
    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path);
    shared->temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, file_cache.get(), max_size);
}

void Context::setFlagsPath(const String & path)
{
    auto lock = getLock();
    shared->flags_path = path;
}

void Context::setUserFilesPath(const String & path)
{
    auto lock = getLock();
    shared->user_files_path = path;
}

void Context::setDictionariesLibPath(const String & path)
{
    auto lock = getLock();
    shared->dictionaries_lib_path = path;
}

void Context::setUserScriptsPath(const String & path)
{
    auto lock = getLock();
    shared->user_scripts_path = path;
}

void Context::addWarningMessage(const String & msg) const
{
    auto lock = getLock();
    auto suppress_re = getConfigRef().getString("warning_supress_regexp", "");
    bool is_supressed = !suppress_re.empty() && re2::RE2::PartialMatch(msg, suppress_re);
    if (!is_supressed)
        shared->addWarningMessage(msg);
}

void Context::setConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->config = config;
    shared->access_control->setExternalAuthenticatorsConfig(*shared->config);
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    auto lock = getLock();
    return shared->config ? *shared->config : Poco::Util::Application::instance().config();
}


AccessControl & Context::getAccessControl()
{
    return *shared->access_control;
}

const AccessControl & Context::getAccessControl() const
{
    return *shared->access_control;
}

void Context::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock();
    shared->access_control->setExternalAuthenticatorsConfig(config);
}

std::unique_ptr<GSSAcceptorContext> Context::makeGSSAcceptorContext() const
{
    auto lock = getLock();
    return std::make_unique<GSSAcceptorContext>(shared->access_control->getExternalAuthenticators().getKerberosParams());
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    auto lock = getLock();
    shared->users_config = config;
    shared->access_control->setUsersConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    auto lock = getLock();
    return shared->users_config;
}

void Context::setUser(const UUID & user_id_)
{
    auto lock = getLock();

    user_id = user_id_;

    access = getAccessControl().getContextAccess(
        user_id_, /* current_roles = */ {}, /* use_default_roles = */ true, settings, current_database, client_info);

    auto user = access->getUser();

    current_roles = std::make_shared<std::vector<UUID>>(user->granted_roles.findGranted(user->default_roles));

    auto default_profile_info = access->getDefaultProfileInfo();
    settings_constraints_and_current_profiles = default_profile_info->getConstraintsAndProfileIDs();
    applySettingsChanges(default_profile_info->settings);

    if (!user->default_database.empty())
        setCurrentDatabase(user->default_database);
}

std::shared_ptr<const User> Context::getUser() const
{
    return getAccess()->getUser();
}

String Context::getUserName() const
{
    return getAccess()->getUserName();
}

std::optional<UUID> Context::getUserID() const
{
    auto lock = getLock();
    return user_id;
}


void Context::setQuotaKey(String quota_key_)
{
    auto lock = getLock();
    client_info.quota_key = std::move(quota_key_);
}


void Context::setCurrentRoles(const std::vector<UUID> & current_roles_)
{
    auto lock = getLock();
    if (current_roles ? (*current_roles == current_roles_) : current_roles_.empty())
       return;
    current_roles = std::make_shared<std::vector<UUID>>(current_roles_);
    calculateAccessRights();
}

void Context::setCurrentRolesDefault()
{
    auto user = getUser();
    setCurrentRoles(user->granted_roles.findGranted(user->default_roles));
}

boost::container::flat_set<UUID> Context::getCurrentRoles() const
{
    return getRolesInfo()->current_roles;
}

boost::container::flat_set<UUID> Context::getEnabledRoles() const
{
    return getRolesInfo()->enabled_roles;
}

std::shared_ptr<const EnabledRolesInfo> Context::getRolesInfo() const
{
    return getAccess()->getRolesInfo();
}


void Context::calculateAccessRights()
{
    auto lock = getLock();
    if (user_id)
        access = getAccessControl().getContextAccess(
            *user_id,
            current_roles ? *current_roles : std::vector<UUID>{},
            /* use_default_roles = */ false,
            settings,
            current_database,
            client_info);
}


template <typename... Args>
void Context::checkAccessImpl(const Args &... args) const
{
    return getAccess()->checkAccess(args...);
}

void Context::checkAccess(const AccessFlags & flags) const { return checkAccessImpl(flags); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database) const { return checkAccessImpl(flags, database); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table) const { return checkAccessImpl(flags, database, table); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { return checkAccessImpl(flags, database, table, column); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { return checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { return checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName()); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, std::string_view column) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), column); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessRightsElement & element) const { return checkAccessImpl(element); }
void Context::checkAccess(const AccessRightsElements & elements) const { return checkAccessImpl(elements); }


std::shared_ptr<const ContextAccess> Context::getAccess() const
{
    auto lock = getLock();
    return access ? access : ContextAccess::getFullAccess();
}

RowPolicyFilterPtr Context::getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    auto lock = getLock();
    RowPolicyFilterPtr row_filter_of_initial_user;
    if (row_policies_of_initial_user)
        row_filter_of_initial_user = row_policies_of_initial_user->getFilter(database, table_name, filter_type);
    return getAccess()->getRowPolicyFilter(database, table_name, filter_type, row_filter_of_initial_user);
}

void Context::enableRowPoliciesOfInitialUser()
{
    auto lock = getLock();
    row_policies_of_initial_user = nullptr;
    if (client_info.initial_user == client_info.current_user)
        return;
    auto initial_user_id = getAccessControl().find<User>(client_info.initial_user);
    if (!initial_user_id)
        return;
    row_policies_of_initial_user = getAccessControl().tryGetDefaultRowPolicies(*initial_user_id);
}


std::shared_ptr<const EnabledQuota> Context::getQuota() const
{
    return getAccess()->getQuota();
}


std::optional<QuotaUsage> Context::getQuotaUsage() const
{
    return getAccess()->getQuotaUsage();
}


void Context::setCurrentProfile(const String & profile_name)
{
    auto lock = getLock();
    try
    {
        UUID profile_id = getAccessControl().getID<SettingsProfile>(profile_name);
        setCurrentProfile(profile_id);
    }
    catch (Exception & e)
    {
        e.addMessage(", while trying to set settings profile {}", profile_name);
        throw;
    }
}

void Context::setCurrentProfile(const UUID & profile_id)
{
    auto lock = getLock();
    auto profile_info = getAccessControl().getSettingsProfileInfo(profile_id);
    checkSettingsConstraints(profile_info->settings);
    applySettingsChanges(profile_info->settings);
    settings_constraints_and_current_profiles = profile_info->getConstraintsAndProfileIDs(settings_constraints_and_current_profiles);
}


std::vector<UUID> Context::getCurrentProfiles() const
{
    auto lock = getLock();
    return settings_constraints_and_current_profiles->current_profiles;
}

std::vector<UUID> Context::getEnabledProfiles() const
{
    auto lock = getLock();
    return settings_constraints_and_current_profiles->enabled_profiles;
}


ResourceManagerPtr Context::getResourceManager() const
{
    auto lock = getLock();
    if (!shared->resource_manager)
        shared->resource_manager = ResourceManagerFactory::instance().get(getConfigRef().getString("resource_manager", "static"));
    return shared->resource_manager;
}

ClassifierPtr Context::getClassifier() const
{
    auto lock = getLock();
    return getResourceManager()->acquire(getSettingsRef().workload);
}


const Scalars & Context::getScalars() const
{
    return scalars;
}


const Block & Context::getScalar(const String & name) const
{
    auto it = scalars.find(name);
    if (scalars.end() == it)
    {
        // This should be a logical error, but it fails the sql_fuzz test too
        // often, so 'bad arguments' for now.
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Scalar {} doesn't exist (internal bug)", backQuoteIfNeed(name));
    }
    return it->second;
}

const Block * Context::tryGetSpecialScalar(const String & name) const
{
    auto it = special_scalars.find(name);
    if (special_scalars.end() == it)
        return nullptr;
    return &it->second;
}

Tables Context::getExternalTables() const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    auto lock = getLock();

    Tables res;
    for (const auto & table : external_tables_mapping)
        res[table.first] = table.second->getTable();

    auto query_context_ptr = query_context.lock();
    auto session_context_ptr = session_context.lock();
    if (query_context_ptr && query_context_ptr.get() != this)
    {
        Tables buf = query_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    else if (session_context_ptr && session_context_ptr.get() != this)
    {
        Tables buf = session_context_ptr->getExternalTables();
        res.insert(buf.begin(), buf.end());
    }
    return res;
}


void Context::addExternalTable(const String & table_name, TemporaryTableHolder && temporary_table)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    auto lock = getLock();
    if (external_tables_mapping.end() != external_tables_mapping.find(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Temporary table {} already exists.", backQuoteIfNeed(table_name));
    external_tables_mapping.emplace(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}


std::shared_ptr<TemporaryTableHolder> Context::removeExternalTable(const String & table_name)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::shared_ptr<TemporaryTableHolder> holder;
    {
        auto lock = getLock();
        auto iter = external_tables_mapping.find(table_name);
        if (iter == external_tables_mapping.end())
            return {};
        holder = iter->second;
        external_tables_mapping.erase(iter);
    }
    return holder;
}


void Context::addScalar(const String & name, const Block & block)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have scalars");

    scalars[name] = block;
}


void Context::addSpecialScalar(const String & name, const Block & block)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have local scalars");

    special_scalars[name] = block;
}


bool Context::hasScalar(const String & name) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have scalars");

    return scalars.contains(name);
}


void Context::addQueryAccessInfo(
    const String & quoted_database_name,
    const String & full_quoted_table_name,
    const Names & column_names,
    const String & projection_name,
    const String & view_name)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query access info");

    std::lock_guard lock(query_access_info.mutex);
    query_access_info.databases.emplace(quoted_database_name);
    query_access_info.tables.emplace(full_quoted_table_name);
    for (const auto & column_name : column_names)
        query_access_info.columns.emplace(full_quoted_table_name + "." + backQuoteIfNeed(column_name));
    if (!projection_name.empty())
        query_access_info.projections.emplace(full_quoted_table_name + "." + backQuoteIfNeed(projection_name));
    if (!view_name.empty())
        query_access_info.views.emplace(view_name);
}

void Context::addQueryFactoriesInfo(QueryLogFactories factory_type, const String & created_object) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query factories info");

    std::lock_guard lock(query_factories_info.mutex);

    switch (factory_type)
    {
        case QueryLogFactories::AggregateFunction:
            query_factories_info.aggregate_functions.emplace(created_object);
            break;
        case QueryLogFactories::AggregateFunctionCombinator:
            query_factories_info.aggregate_function_combinators.emplace(created_object);
            break;
        case QueryLogFactories::Database:
            query_factories_info.database_engines.emplace(created_object);
            break;
        case QueryLogFactories::DataType:
            query_factories_info.data_type_families.emplace(created_object);
            break;
        case QueryLogFactories::Dictionary:
            query_factories_info.dictionaries.emplace(created_object);
            break;
        case QueryLogFactories::Format:
            query_factories_info.formats.emplace(created_object);
            break;
        case QueryLogFactories::Function:
            query_factories_info.functions.emplace(created_object);
            break;
        case QueryLogFactories::Storage:
            query_factories_info.storages.emplace(created_object);
            break;
        case QueryLogFactories::TableFunction:
            query_factories_info.table_functions.emplace(created_object);
    }
}


StoragePtr Context::executeTableFunction(const ASTPtr & table_expression, const ASTSelectQuery * select_query_hint)
{
    ASTFunction * function = assert_cast<ASTFunction *>(table_expression.get());
    String database_name = getCurrentDatabase();
    String table_name = function->name;

    if (function->is_compound_name)
    {
        std::vector<std::string> parts;
        splitInto<'.'>(parts, function->name);

        if (parts.size() == 2)
        {
            database_name = parts[0];
            table_name = parts[1];
        }
    }

    StoragePtr table = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, getQueryContext());
    if (table)
    {
        if (table.get()->isView() && table->as<StorageView>()->isParameterizedView())
        {
            function->prefer_subquery_to_function_formatting = true;
            return table;
        }
    }
    auto hash = table_expression->getTreeHash();
    String key = toString(hash.first) + '_' + toString(hash.second);
    StoragePtr & res = table_function_results[key];
    if (!res)
    {
        TableFunctionPtr table_function_ptr;
        try
        {
            table_function_ptr = TableFunctionFactory::instance().get(table_expression, shared_from_this());
        }
        catch (Exception & e)
        {
            if (e.code() == ErrorCodes::UNKNOWN_FUNCTION)
            {
                e.addMessage(" or incorrect parameterized view");
            }
            throw;
        }
        if (getSettingsRef().use_structure_from_insertion_table_in_table_functions && table_function_ptr->needStructureHint() && hasInsertionTable())
        {
            const auto & structure_hint = DatabaseCatalog::instance().getTable(getInsertionTable(), shared_from_this())->getInMemoryMetadataPtr()->getColumns();
            bool use_columns_from_insert_query = true;

            /// use_structure_from_insertion_table_in_table_functions=2 means `auto`
            if (select_query_hint && getSettingsRef().use_structure_from_insertion_table_in_table_functions == 2)
            {
                const auto * expression_list = select_query_hint->select()->as<ASTExpressionList>();
                std::unordered_set<String> virtual_column_names = table_function_ptr->getVirtualsToCheckBeforeUsingStructureHint();
                Names columns_names;
                bool have_asterisk = false;
                /// First, check if we have only identifiers, asterisk and literals in select expression,
                /// and if no, we cannot use the structure from insertion table.
                for (const auto & expression : expression_list->children)
                {
                    if (auto * identifier = expression->as<ASTIdentifier>())
                    {
                        columns_names.push_back(identifier->name());
                    }
                    else if (expression->as<ASTAsterisk>())
                    {
                        have_asterisk = true;
                    }
                    else if (!expression->as<ASTLiteral>())
                    {
                        use_columns_from_insert_query = false;
                        break;
                    }
                }

                /// Check that all identifiers are column names from insertion table and not virtual column names from storage.
                for (const auto & column_name : columns_names)
                {
                    if (!structure_hint.has(column_name) || virtual_column_names.contains(column_name))
                    {
                        use_columns_from_insert_query = false;
                        break;
                    }
                }

                /// If we don't have asterisk but only subset of columns, we should use
                /// structure from insertion table only in case when table function
                /// supports reading subset of columns from data.
                if (use_columns_from_insert_query && !have_asterisk && !columns_names.empty())
                {
                    /// For input function we should check if input format supports reading subset of columns.
                    if (table_function_ptr->getName() == "input")
                        use_columns_from_insert_query = FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(getInsertFormat(), shared_from_this());
                    else
                        use_columns_from_insert_query = table_function_ptr->supportsReadingSubsetOfColumns(shared_from_this());
                }
            }

            if (use_columns_from_insert_query)
                table_function_ptr->setStructureHint(structure_hint);
        }

        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());

        /// Since ITableFunction::parseArguments() may change table_expression, i.e.:
        ///
        ///     remote('127.1', system.one) -> remote('127.1', 'system.one'),
        ///
        auto new_hash = table_expression->getTreeHash();
        if (hash != new_hash)
        {
            key = toString(new_hash.first) + '_' + toString(new_hash.second);
            table_function_results[key] = res;
        }
    }
    return res;
}


void Context::addViewSource(const StoragePtr & storage)
{
    if (view_source)
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Temporary view source storage {} already exists.",
            backQuoteIfNeed(view_source->getName()));
    view_source = storage;
}


StoragePtr Context::getViewSource() const
{
    return view_source;
}

Settings Context::getSettings() const
{
    auto lock = getLock();
    return settings;
}


void Context::setSettings(const Settings & settings_)
{
    auto lock = getLock();
    auto old_readonly = settings.readonly;
    auto old_allow_ddl = settings.allow_ddl;
    auto old_allow_introspection_functions = settings.allow_introspection_functions;

    settings = settings_;

    if ((settings.readonly != old_readonly) || (settings.allow_ddl != old_allow_ddl) || (settings.allow_introspection_functions != old_allow_introspection_functions))
        calculateAccessRights();
}


void Context::setSetting(std::string_view name, const String & value)
{
    auto lock = getLock();
    if (name == "profile")
    {
        setCurrentProfile(value);
        return;
    }
    settings.set(name, value);

    if (name == "readonly" || name == "allow_ddl" || name == "allow_introspection_functions")
        calculateAccessRights();
}


void Context::setSetting(std::string_view name, const Field & value)
{
    auto lock = getLock();
    if (name == "profile")
    {
        setCurrentProfile(value.safeGet<String>());
        return;
    }
    settings.set(name, value);

    if (name == "readonly" || name == "allow_ddl" || name == "allow_introspection_functions")
        calculateAccessRights();
}


void Context::applySettingChange(const SettingChange & change)
{
    try
    {
        setSetting(change.name, change.value);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
                         "in attempt to set the value of setting '{}' to {}",
                         change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}


void Context::applySettingsChanges(const SettingsChanges & changes)
{
    auto lock = getLock();
    for (const SettingChange & change : changes)
        applySettingChange(change);
    applySettingsQuirks(settings);
}


void Context::checkSettingsConstraints(const SettingsProfileElements & profile_elements) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(settings, profile_elements);
}

void Context::checkSettingsConstraints(const SettingChange & change) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(settings, change);
}

void Context::checkSettingsConstraints(const SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(settings, changes);
}

void Context::checkSettingsConstraints(SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(settings, changes);
}

void Context::clampToSettingsConstraints(SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.clamp(settings, changes);
}

void Context::checkMergeTreeSettingsConstraints(const MergeTreeSettings & merge_tree_settings, const SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfiles()->constraints.check(merge_tree_settings, changes);
}

void Context::resetSettingsToDefaultValue(const std::vector<String> & names)
{
    auto lock = getLock();
    for (const String & name: names)
    {
        settings.setDefaultValue(name);
    }
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfiles() const
{
    auto lock = getLock();
    if (settings_constraints_and_current_profiles)
        return settings_constraints_and_current_profiles;
    static auto no_constraints_or_profiles = std::make_shared<SettingsConstraintsAndProfileIDs>(getAccessControl());
    return no_constraints_or_profiles;
}


String Context::getCurrentDatabase() const
{
    auto lock = getLock();
    return current_database;
}


String Context::getInitialQueryId() const
{
    return client_info.initial_query_id;
}


void Context::setCurrentDatabaseNameInGlobalContext(const String & name)
{
    if (!isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot set current database for non global context, this method should "
                        "be used during server initialization");
    auto lock = getLock();

    if (!current_database.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Default database name cannot be changed in global context without server restart");

    current_database = name;
}

void Context::setCurrentDatabase(const String & name)
{
    DatabaseCatalog::instance().assertDatabaseExists(name);
    auto lock = getLock();
    current_database = name;
    calculateAccessRights();
}

void Context::setCurrentQueryId(const String & query_id)
{
    /// Generate random UUID, but using lower quality RNG,
    ///  because Poco::UUIDGenerator::generateRandom method is using /dev/random, that is very expensive.
    /// NOTE: Actually we don't need to use UUIDs for query identifiers.
    /// We could use any suitable string instead.
    union
    {
        char bytes[16];
        struct
        {
            UInt64 a;
            UInt64 b;
        } words;
        UUID uuid{};
    } random;

    random.words.a = thread_local_rng();
    random.words.b = thread_local_rng();


    String query_id_to_set = query_id;
    if (query_id_to_set.empty())    /// If the user did not submit his query_id, then we generate it ourselves.
    {
        /// Use protected constructor.
        struct QueryUUID : Poco::UUID
        {
            QueryUUID(const char * bytes, Poco::UUID::Version version)
                : Poco::UUID(bytes, version) {}
        };

        query_id_to_set = QueryUUID(random.bytes, Poco::UUID::UUID_RANDOM).toString();
    }

    client_info.current_query_id = query_id_to_set;

    if (client_info.query_kind == ClientInfo::QueryKind::INITIAL_QUERY)
        client_info.initial_query_id = client_info.current_query_id;
}

void Context::killCurrentQuery() const
{
    if (auto elem = getProcessListElement())
        elem->cancelQuery(true);
}

String Context::getDefaultFormat() const
{
    return default_format.empty() ? "TabSeparated" : default_format;
}

void Context::setDefaultFormat(const String & name)
{
    default_format = name;
}

String Context::getInsertFormat() const
{
    return insert_format;
}

void Context::setInsertFormat(const String & name)
{
    insert_format = name;
}

MultiVersion<Macros>::Version Context::getMacros() const
{
    return shared->macros.get();
}

void Context::setMacros(std::unique_ptr<Macros> && macros)
{
    shared->macros.set(std::move(macros));
}

ContextMutablePtr Context::getQueryContext() const
{
    auto ptr = query_context.lock();
    if (!ptr) throw Exception(ErrorCodes::THERE_IS_NO_QUERY, "There is no query or query context has expired");
    return ptr;
}

bool Context::isInternalSubquery() const
{
    auto ptr = query_context.lock();
    return ptr && ptr.get() != this;
}

ContextMutablePtr Context::getSessionContext() const
{
    auto ptr = session_context.lock();
    if (!ptr) throw Exception(ErrorCodes::THERE_IS_NO_SESSION, "There is no session or session context has expired");
    return ptr;
}

ContextMutablePtr Context::getGlobalContext() const
{
    auto ptr = global_context.lock();
    if (!ptr) throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no global context or global context has expired");
    return ptr;
}

ContextMutablePtr Context::getBufferContext() const
{
    if (!buffer_context) throw Exception(ErrorCodes::LOGICAL_ERROR, "There is no buffer context");
    return buffer_context;
}


const EmbeddedDictionaries & Context::getEmbeddedDictionaries() const
{
    return getEmbeddedDictionariesImpl(false);
}

EmbeddedDictionaries & Context::getEmbeddedDictionaries()
{
    return getEmbeddedDictionariesImpl(false);
}


const ExternalDictionariesLoader & Context::getExternalDictionariesLoader() const
{
    return const_cast<Context *>(this)->getExternalDictionariesLoader();
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoader()
{
    std::lock_guard lock(shared->external_dictionaries_mutex);
    return getExternalDictionariesLoaderUnlocked();
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoaderUnlocked()
{
    if (!shared->external_dictionaries_loader)
        shared->external_dictionaries_loader =
            std::make_unique<ExternalDictionariesLoader>(getGlobalContext());
    return *shared->external_dictionaries_loader;
}

const ExternalUserDefinedExecutableFunctionsLoader & Context::getExternalUserDefinedExecutableFunctionsLoader() const
{
    return const_cast<Context *>(this)->getExternalUserDefinedExecutableFunctionsLoader();
}

ExternalUserDefinedExecutableFunctionsLoader & Context::getExternalUserDefinedExecutableFunctionsLoader()
{
    std::lock_guard lock(shared->external_user_defined_executable_functions_mutex);
    return getExternalUserDefinedExecutableFunctionsLoaderUnlocked();
}

ExternalUserDefinedExecutableFunctionsLoader & Context::getExternalUserDefinedExecutableFunctionsLoaderUnlocked()
{
    if (!shared->external_user_defined_executable_functions_loader)
        shared->external_user_defined_executable_functions_loader =
            std::make_unique<ExternalUserDefinedExecutableFunctionsLoader>(getGlobalContext());
    return *shared->external_user_defined_executable_functions_loader;
}

EmbeddedDictionaries & Context::getEmbeddedDictionariesImpl(const bool throw_on_error) const
{
    std::lock_guard lock(shared->embedded_dictionaries_mutex);

    if (!shared->embedded_dictionaries)
    {
        auto geo_dictionaries_loader = std::make_unique<GeoDictionariesLoader>();

        shared->embedded_dictionaries = std::make_unique<EmbeddedDictionaries>(
            std::move(geo_dictionaries_loader),
            getGlobalContext(),
            throw_on_error);
    }

    return *shared->embedded_dictionaries;
}


void Context::tryCreateEmbeddedDictionaries(const Poco::Util::AbstractConfiguration & config) const
{
    if (!config.getBool("dictionaries_lazy_load", true))
        static_cast<void>(getEmbeddedDictionariesImpl(true));
}

void Context::loadOrReloadDictionaries(const Poco::Util::AbstractConfiguration & config)
{
    bool dictionaries_lazy_load = config.getBool("dictionaries_lazy_load", true);
    auto patterns_values = getMultipleValuesFromConfig(config, "", "dictionaries_config");
    std::unordered_set<std::string> patterns(patterns_values.begin(), patterns_values.end());

    std::lock_guard lock(shared->external_dictionaries_mutex);

    auto & external_dictionaries_loader = getExternalDictionariesLoaderUnlocked();
    external_dictionaries_loader.enableAlwaysLoadEverything(!dictionaries_lazy_load);

    if (shared->external_dictionaries_config_repository)
    {
        shared->external_dictionaries_config_repository->updatePatterns(patterns);
        external_dictionaries_loader.reloadConfig(shared->external_dictionaries_config_repository->getName());
        return;
    }

    auto app_path = getPath();
    auto config_path = getConfigRef().getString("config-file", "config.xml");
    auto repository = std::make_unique<ExternalLoaderXMLConfigRepository>(app_path, config_path, patterns);
    shared->external_dictionaries_config_repository = repository.get();
    shared->dictionaries_xmls = external_dictionaries_loader.addConfigRepository(std::move(repository));
}

void Context::loadOrReloadUserDefinedExecutableFunctions(const Poco::Util::AbstractConfiguration & config)
{
    auto patterns_values = getMultipleValuesFromConfig(config, "", "user_defined_executable_functions_config");
    std::unordered_set<std::string> patterns(patterns_values.begin(), patterns_values.end());

    std::lock_guard lock(shared->external_user_defined_executable_functions_mutex);

    auto & external_user_defined_executable_functions_loader = getExternalUserDefinedExecutableFunctionsLoaderUnlocked();

    if (shared->user_defined_executable_functions_config_repository)
    {
        shared->user_defined_executable_functions_config_repository->updatePatterns(patterns);
        external_user_defined_executable_functions_loader.reloadConfig(shared->user_defined_executable_functions_config_repository->getName());
        return;
    }

    auto app_path = getPath();
    auto config_path = getConfigRef().getString("config-file", "config.xml");
    auto repository = std::make_unique<ExternalLoaderXMLConfigRepository>(app_path, config_path, patterns);
    shared->user_defined_executable_functions_config_repository = repository.get();
    shared->user_defined_executable_functions_xmls = external_user_defined_executable_functions_loader.addConfigRepository(std::move(repository));
}

const IUserDefinedSQLObjectsLoader & Context::getUserDefinedSQLObjectsLoader() const
{
    auto lock = getLock();
    if (!shared->user_defined_sql_objects_loader)
        shared->user_defined_sql_objects_loader = createUserDefinedSQLObjectsLoader(getGlobalContext());
    return *shared->user_defined_sql_objects_loader;
}

IUserDefinedSQLObjectsLoader & Context::getUserDefinedSQLObjectsLoader()
{
    auto lock = getLock();
    if (!shared->user_defined_sql_objects_loader)
        shared->user_defined_sql_objects_loader = createUserDefinedSQLObjectsLoader(getGlobalContext());
    return *shared->user_defined_sql_objects_loader;
}

#if USE_NLP

SynonymsExtensions & Context::getSynonymsExtensions() const
{
    auto lock = getLock();

    if (!shared->synonyms_extensions)
        shared->synonyms_extensions.emplace(getConfigRef());

    return *shared->synonyms_extensions;
}

Lemmatizers & Context::getLemmatizers() const
{
    auto lock = getLock();

    if (!shared->lemmatizers)
        shared->lemmatizers.emplace(getConfigRef());

    return *shared->lemmatizers;
}
#endif

BackupsWorker & Context::getBackupsWorker() const
{
    auto lock = getLock();

    const bool allow_concurrent_backups = this->getConfigRef().getBool("backups.allow_concurrent_backups", true);
    const bool allow_concurrent_restores = this->getConfigRef().getBool("backups.allow_concurrent_restores", true);

    const auto & config = getConfigRef();
    const auto & settings = getSettingsRef();
    UInt64 backup_threads = config.getUInt64("backup_threads", settings.backup_threads);
    UInt64 restore_threads = config.getUInt64("restore_threads", settings.restore_threads);

    if (!shared->backups_worker)
        shared->backups_worker.emplace(backup_threads, restore_threads, allow_concurrent_backups, allow_concurrent_restores);

    return *shared->backups_worker;
}


void Context::setProgressCallback(ProgressCallback callback)
{
    /// Callback is set to a session or to a query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    progress_callback = callback;
}

ProgressCallback Context::getProgressCallback() const
{
    return progress_callback;
}


void Context::setProcessListElement(QueryStatusPtr elem)
{
    /// Set to a session or query. In the session, only one query is processed at a time. Therefore, the lock is not needed.
    process_list_elem = elem;
    has_process_list_elem = elem.get();
}

QueryStatusPtr Context::getProcessListElement() const
{
    if (!has_process_list_elem)
        return {};
    if (auto res = process_list_elem.lock())
        return res;
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Weak pointer to process_list_elem expired during query execution, it's a bug");
}


void Context::setUncompressedCache(const String & uncompressed_cache_policy, size_t max_size_in_bytes)
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Uncompressed cache has been already created.");

    shared->uncompressed_cache = std::make_shared<UncompressedCache>(uncompressed_cache_policy, max_size_in_bytes);
}


UncompressedCachePtr Context::getUncompressedCache() const
{
    auto lock = getLock();
    return shared->uncompressed_cache;
}


void Context::dropUncompressedCache() const
{
    auto lock = getLock();
    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();
}


void Context::setMarkCache(const String & mark_cache_policy, size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark cache has been already created.");

    shared->mark_cache = std::make_shared<MarkCache>(mark_cache_policy, cache_size_in_bytes);
}

MarkCachePtr Context::getMarkCache() const
{
    auto lock = getLock();
    return shared->mark_cache;
}

void Context::dropMarkCache() const
{
    auto lock = getLock();
    if (shared->mark_cache)
        shared->mark_cache->reset();
}

ThreadPool & Context::getLoadMarksThreadpool() const
{
    const auto & config = getConfigRef();

    auto lock = getLock();
    if (!shared->load_marks_threadpool)
    {
        auto pool_size = config.getUInt(".load_marks_threadpool_pool_size", 50);
        auto queue_size = config.getUInt(".load_marks_threadpool_queue_size", 1000000);
        shared->load_marks_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::MarksLoaderThreads, CurrentMetrics::MarksLoaderThreadsActive, pool_size, pool_size, queue_size);
    }
    return *shared->load_marks_threadpool;
}

static size_t getPrefetchThreadpoolSizeFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    return config.getUInt(".prefetch_threadpool_pool_size", 100);
}

size_t Context::getPrefetchThreadpoolSize() const
{
    const auto & config = getConfigRef();
    return getPrefetchThreadpoolSizeFromConfig(config);
}

ThreadPool & Context::getPrefetchThreadpool() const
{
    const auto & config = getConfigRef();

    auto lock = getLock();
    if (!shared->prefetch_threadpool)
    {
        auto pool_size = getPrefetchThreadpoolSize();
        auto queue_size = config.getUInt(".prefetch_threadpool_queue_size", 1000000);
        shared->prefetch_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::IOPrefetchThreads, CurrentMetrics::IOPrefetchThreadsActive, pool_size, pool_size, queue_size);
    }
    return *shared->prefetch_threadpool;
}

void Context::setIndexUncompressedCache(size_t max_size_in_bytes)
{
    auto lock = getLock();

    if (shared->index_uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index uncompressed cache has been already created.");

    shared->index_uncompressed_cache = std::make_shared<UncompressedCache>(max_size_in_bytes);
}


UncompressedCachePtr Context::getIndexUncompressedCache() const
{
    auto lock = getLock();
    return shared->index_uncompressed_cache;
}


void Context::dropIndexUncompressedCache() const
{
    auto lock = getLock();
    if (shared->index_uncompressed_cache)
        shared->index_uncompressed_cache->reset();
}


void Context::setIndexMarkCache(size_t cache_size_in_bytes)
{
    auto lock = getLock();

    if (shared->index_mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index mark cache has been already created.");

    shared->index_mark_cache = std::make_shared<MarkCache>(cache_size_in_bytes);
}

MarkCachePtr Context::getIndexMarkCache() const
{
    auto lock = getLock();
    return shared->index_mark_cache;
}

void Context::dropIndexMarkCache() const
{
    auto lock = getLock();
    if (shared->index_mark_cache)
        shared->index_mark_cache->reset();
}

void Context::setQueryCache(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock();

    if (shared->query_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query cache has been already created.");

    shared->query_cache = std::make_shared<QueryCache>();
    shared->query_cache->updateConfiguration(config);
}

void Context::updateQueryCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = getLock();
    if (shared->query_cache)
        shared->query_cache->updateConfiguration(config);
}

QueryCachePtr Context::getQueryCache() const
{
    auto lock = getLock();
    return shared->query_cache;
}

void Context::dropQueryCache() const
{
    auto lock = getLock();
    if (shared->query_cache)
        shared->query_cache->reset();
}

void Context::setMMappedFileCache(size_t cache_size_in_num_entries)
{
    auto lock = getLock();

    if (shared->mmap_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped file cache has been already created.");

    shared->mmap_cache = std::make_shared<MMappedFileCache>(cache_size_in_num_entries);
}

MMappedFileCachePtr Context::getMMappedFileCache() const
{
    auto lock = getLock();
    return shared->mmap_cache;
}

void Context::dropMMappedFileCache() const
{
    auto lock = getLock();
    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}


void Context::dropCaches() const
{
    auto lock = getLock();

    if (shared->uncompressed_cache)
        shared->uncompressed_cache->reset();

    if (shared->mark_cache)
        shared->mark_cache->reset();

    if (shared->index_uncompressed_cache)
        shared->index_uncompressed_cache->reset();

    if (shared->index_mark_cache)
        shared->index_mark_cache->reset();

    if (shared->query_cache)
        shared->query_cache->reset();

    if (shared->mmap_cache)
        shared->mmap_cache->reset();
}

BackgroundSchedulePool & Context::getBufferFlushSchedulePool() const
{
    auto lock = getLock();
    if (!shared->buffer_flush_schedule_pool)
    {
        ServerSettings server_settings;
        server_settings.loadSettingsFromConfig(getConfigRef());

        shared->buffer_flush_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            server_settings.background_buffer_flush_schedule_pool_size,
            CurrentMetrics::BackgroundBufferFlushSchedulePoolTask,
            CurrentMetrics::BackgroundBufferFlushSchedulePoolSize,
            "BgBufSchPool");
    }

    return *shared->buffer_flush_schedule_pool;
}

BackgroundTaskSchedulingSettings Context::getBackgroundProcessingTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);
    return task_settings;
}

BackgroundTaskSchedulingSettings Context::getBackgroundMoveTaskSchedulingSettings() const
{
    BackgroundTaskSchedulingSettings task_settings;

    const auto & config = getConfigRef();
    task_settings.thread_sleep_seconds = config.getDouble("background_move_processing_pool_thread_sleep_seconds", 10);
    task_settings.thread_sleep_seconds_random_part = config.getDouble("background_move_processing_pool_thread_sleep_seconds_random_part", 1.0);
    task_settings.thread_sleep_seconds_if_nothing_to_do = config.getDouble("background_move_processing_pool_thread_sleep_seconds_if_nothing_to_do", 0.1);
    task_settings.task_sleep_seconds_when_no_work_min = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_min", 10);
    task_settings.task_sleep_seconds_when_no_work_max = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_max", 600);
    task_settings.task_sleep_seconds_when_no_work_multiplier = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_multiplier", 1.1);
    task_settings.task_sleep_seconds_when_no_work_random_part = config.getDouble("background_move_processing_pool_task_sleep_seconds_when_no_work_random_part", 1.0);

    return task_settings;
}

BackgroundSchedulePool & Context::getSchedulePool() const
{
    auto lock = getLock();
    if (!shared->schedule_pool)
    {
        ServerSettings server_settings;
        server_settings.loadSettingsFromConfig(getConfigRef());

        shared->schedule_pool = std::make_unique<BackgroundSchedulePool>(
            server_settings.background_schedule_pool_size,
            CurrentMetrics::BackgroundSchedulePoolTask,
            CurrentMetrics::BackgroundSchedulePoolSize,
            "BgSchPool");
    }

    return *shared->schedule_pool;
}

BackgroundSchedulePool & Context::getDistributedSchedulePool() const
{
    auto lock = getLock();
    if (!shared->distributed_schedule_pool)
    {
        ServerSettings server_settings;
        server_settings.loadSettingsFromConfig(getConfigRef());

        shared->distributed_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            server_settings.background_distributed_schedule_pool_size,
            CurrentMetrics::BackgroundDistributedSchedulePoolTask,
            CurrentMetrics::BackgroundDistributedSchedulePoolSize,
            "BgDistSchPool");
    }

    return *shared->distributed_schedule_pool;
}

BackgroundSchedulePool & Context::getMessageBrokerSchedulePool() const
{
    auto lock = getLock();
    if (!shared->message_broker_schedule_pool)
    {
        ServerSettings server_settings;
        server_settings.loadSettingsFromConfig(getConfigRef());

        shared->message_broker_schedule_pool = std::make_unique<BackgroundSchedulePool>(
            server_settings.background_message_broker_schedule_pool_size,
            CurrentMetrics::BackgroundMessageBrokerSchedulePoolTask,
            CurrentMetrics::BackgroundMessageBrokerSchedulePoolSize,
            "BgMBSchPool");
    }

    return *shared->message_broker_schedule_pool;
}

ThrottlerPtr Context::getReplicatedFetchesThrottler() const
{
    auto lock = getLock();
    if (!shared->replicated_fetches_throttler)
        shared->replicated_fetches_throttler = std::make_shared<Throttler>(
            settings.max_replicated_fetches_network_bandwidth_for_server);

    return shared->replicated_fetches_throttler;
}

ThrottlerPtr Context::getReplicatedSendsThrottler() const
{
    auto lock = getLock();
    if (!shared->replicated_sends_throttler)
        shared->replicated_sends_throttler = std::make_shared<Throttler>(
            settings.max_replicated_sends_network_bandwidth_for_server);

    return shared->replicated_sends_throttler;
}

ThrottlerPtr Context::getRemoteReadThrottler() const
{
    auto lock = getLock();
    if (!shared->remote_read_throttler)
        shared->remote_read_throttler = std::make_shared<Throttler>(
            settings.max_remote_read_network_bandwidth_for_server);

    return shared->remote_read_throttler;
}

ThrottlerPtr Context::getRemoteWriteThrottler() const
{
    auto lock = getLock();
    if (!shared->remote_write_throttler)
        shared->remote_write_throttler = std::make_shared<Throttler>(
            settings.max_remote_write_network_bandwidth_for_server);

    return shared->remote_write_throttler;
}

bool Context::hasDistributedDDL() const
{
    return getConfigRef().has("distributed_ddl");
}

void Context::setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker)
{
    auto lock = getLock();
    if (shared->ddl_worker)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DDL background thread has already been initialized");
    ddl_worker->startup();
    shared->ddl_worker = std::move(ddl_worker);
}

DDLWorker & Context::getDDLWorker() const
{
    auto lock = getLock();
    if (!shared->ddl_worker)
    {
        if (!hasZooKeeper())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no Zookeeper configuration in server config");

        if (!hasDistributedDDL())
            throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no DistributedDDL configuration in server config");

        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "DDL background thread is not initialized");
    }
    return *shared->ddl_worker;
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);

    const auto & config = shared->zookeeper_config ? *shared->zookeeper_config : getConfigRef();
    if (!shared->zookeeper)
        shared->zookeeper = std::make_shared<zkutil::ZooKeeper>(config, "zookeeper", getZooKeeperLog());
    else if (shared->zookeeper->expired())
    {
        Stopwatch watch;
        LOG_DEBUG(shared->log, "Trying to establish a new connection with ZooKeeper");
        shared->zookeeper = shared->zookeeper->startNewSession();
        LOG_DEBUG(shared->log, "Establishing a new connection with ZooKeeper took {} ms", watch.elapsedMilliseconds());
    }

    return shared->zookeeper;
}

namespace
{

bool checkZooKeeperConfigIsLocal(const Poco::Util::AbstractConfiguration & config, const std::string & config_name)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(config_name, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "node"))
        {
            String host = config.getString(config_name + "." + key + ".host");
            if (isLocalAddress(DNSResolver::instance().resolveHost(host)))
                return true;
        }
    }
    return false;
}

}


bool Context::tryCheckClientConnectionToMyKeeperCluster() const
{
    try
    {
        /// If our server is part of main Keeper cluster
        if (checkZooKeeperConfigIsLocal(getConfigRef(), "zookeeper"))
        {
            LOG_DEBUG(shared->log, "Keeper server is participant of the main zookeeper cluster, will try to connect to it");
            getZooKeeper();
            /// Connected, return true
            return true;
        }
        else
        {
            Poco::Util::AbstractConfiguration::Keys keys;
            getConfigRef().keys("auxiliary_zookeepers", keys);

            /// If our server is part of some auxiliary_zookeeper
            for (const auto & aux_zk_name : keys)
            {
                if (checkZooKeeperConfigIsLocal(getConfigRef(), "auxiliary_zookeepers." + aux_zk_name))
                {
                    LOG_DEBUG(shared->log, "Our Keeper server is participant of the auxiliary zookeeper cluster ({}), will try to connect to it", aux_zk_name);
                    getAuxiliaryZooKeeper(aux_zk_name);
                    /// Connected, return true
                    return true;
                }
            }
        }

        /// Our server doesn't depend on our Keeper cluster
        return true;
    }
    catch (...)
    {
        return false;
    }
}

UInt32 Context::getZooKeeperSessionUptime() const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    if (!shared->zookeeper || shared->zookeeper->expired())
        return 0;
    return shared->zookeeper->getSessionUptime();
}

void Context::setSystemZooKeeperLogAfterInitializationIfNeeded()
{
    /// It can be nearly impossible to understand in which order global objects are initialized on server startup.
    /// If getZooKeeper() is called before initializeSystemLogs(), then zkutil::ZooKeeper gets nullptr
    /// instead of pointer to system table and it logs nothing.
    /// This method explicitly sets correct pointer to system log after its initialization.
    /// TODO get rid of this if possible

    std::lock_guard lock(shared->zookeeper_mutex);
    if (!shared->system_logs || !shared->system_logs->zookeeper_log)
        return;

    if (shared->zookeeper)
        shared->zookeeper->setZooKeeperLog(shared->system_logs->zookeeper_log);

    for (auto & zk : shared->auxiliary_zookeepers)
        zk.second->setZooKeeperLog(shared->system_logs->zookeeper_log);
}

void Context::initializeKeeperDispatcher([[maybe_unused]] bool start_async) const
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);

    if (shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to initialize Keeper multiple times");

    const auto & config = getConfigRef();
    if (config.has("keeper_server"))
    {
        bool is_standalone_app = getApplicationType() == ApplicationType::KEEPER;
        if (start_async)
        {
            assert(!is_standalone_app);
            LOG_INFO(shared->log, "Connected to ZooKeeper (or Keeper) before internal Keeper start or we don't depend on our Keeper cluster, "
                     "will wait for Keeper asynchronously");
        }
        else
        {
            LOG_INFO(shared->log, "Cannot connect to ZooKeeper (or Keeper) before internal Keeper start, "
                     "will wait for Keeper synchronously");
        }

        shared->keeper_dispatcher = std::make_shared<KeeperDispatcher>();
        shared->keeper_dispatcher->initialize(config, is_standalone_app, start_async, getMacros());
    }
#endif
}

#if USE_NURAFT
std::shared_ptr<KeeperDispatcher> & Context::getKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return shared->keeper_dispatcher;
}

std::shared_ptr<KeeperDispatcher> & Context::tryGetKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    return shared->keeper_dispatcher;
}
#endif

void Context::shutdownKeeperDispatcher() const
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (shared->keeper_dispatcher)
    {
        shared->keeper_dispatcher->shutdown();
        shared->keeper_dispatcher.reset();
    }
#endif
}


void Context::updateKeeperConfiguration([[maybe_unused]] const Poco::Util::AbstractConfiguration & config)
{
#if USE_NURAFT
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        return;

    shared->keeper_dispatcher->updateConfiguration(config, getMacros());
#endif
}


zkutil::ZooKeeperPtr Context::getAuxiliaryZooKeeper(const String & name) const
{
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);

    auto zookeeper = shared->auxiliary_zookeepers.find(name);
    if (zookeeper == shared->auxiliary_zookeepers.end())
    {
        if (name.find(':') != std::string::npos || name.find('/') != std::string::npos)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid auxiliary ZooKeeper name {}: ':' and '/' are not allowed", name);

        const auto & config = shared->auxiliary_zookeepers_config ? *shared->auxiliary_zookeepers_config : getConfigRef();
        if (!config.has("auxiliary_zookeepers." + name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown auxiliary ZooKeeper name '{}'. If it's required it can be added to the section <auxiliary_zookeepers> in "
                "config.xml",
                name);

        zookeeper = shared->auxiliary_zookeepers.emplace(name,
                        std::make_shared<zkutil::ZooKeeper>(config, "auxiliary_zookeepers." + name, getZooKeeperLog())).first;
    }
    else if (zookeeper->second->expired())
        zookeeper->second = zookeeper->second->startNewSession();

    return zookeeper->second;
}

#if USE_ROCKSDB
MergeTreeMetadataCachePtr Context::getMergeTreeMetadataCache() const
{
    auto cache = tryGetMergeTreeMetadataCache();
    if (!cache)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Merge tree metadata cache is not initialized, please add config merge_tree_metadata_cache in config.xml and restart");
    return cache;
}

MergeTreeMetadataCachePtr Context::tryGetMergeTreeMetadataCache() const
{
    return shared->merge_tree_metadata_cache;
}
#endif

void Context::resetZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper.reset();
}

static void reloadZooKeeperIfChangedImpl(
    const ConfigurationPtr & config,
    const std::string & config_name,
    zkutil::ZooKeeperPtr & zk,
    std::shared_ptr<ZooKeeperLog> zk_log,
    bool server_started)
{
    if (!zk || zk->configChanged(*config, config_name))
    {
        if (zk)
            zk->finalize("Config changed");

        zk = std::make_shared<zkutil::ZooKeeper>(*config, config_name, std::move(zk_log));
        if (server_started)
            zk->setServerCompletelyStarted();
    }
}

void Context::reloadZooKeeperIfChanged(const ConfigurationPtr & config) const
{
    bool server_started = isServerCompletelyStarted();
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper_config = config;
    reloadZooKeeperIfChangedImpl(config, "zookeeper", shared->zookeeper, getZooKeeperLog(), server_started);
}

void Context::reloadAuxiliaryZooKeepersConfigIfChanged(const ConfigurationPtr & config)
{
    bool server_started = isServerCompletelyStarted();
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);

    shared->auxiliary_zookeepers_config = config;

    for (auto it = shared->auxiliary_zookeepers.begin(); it != shared->auxiliary_zookeepers.end();)
    {
        if (!config->has("auxiliary_zookeepers." + it->first))
            it = shared->auxiliary_zookeepers.erase(it);
        else
        {
            reloadZooKeeperIfChangedImpl(config, "auxiliary_zookeepers." + it->first, it->second, getZooKeeperLog(), server_started);
            ++it;
        }
    }
}


bool Context::hasZooKeeper() const
{
    return getConfigRef().has("zookeeper");
}

bool Context::hasAuxiliaryZooKeeper(const String & name) const
{
    return getConfigRef().has("auxiliary_zookeepers." + name);
}

InterserverCredentialsPtr Context::getInterserverCredentials() const
{
    return shared->interserver_io_credentials.get();
}

void Context::updateInterserverCredentials(const Poco::Util::AbstractConfiguration & config)
{
    auto credentials = InterserverCredentials::make(config, "interserver_http_credentials");
    shared->interserver_io_credentials.set(std::move(credentials));
}

void Context::setInterserverIOAddress(const String & host, UInt16 port)
{
    shared->interserver_io_host = host;
    shared->interserver_io_port = port;
}

std::pair<String, UInt16> Context::getInterserverIOAddress() const
{
    if (shared->interserver_io_host.empty() || shared->interserver_io_port == 0)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG,
                        "Parameter 'interserver_http(s)_port' required for replication is not specified "
                        "in configuration file.");

    return { shared->interserver_io_host, shared->interserver_io_port };
}

void Context::setInterserverScheme(const String & scheme)
{
    shared->interserver_scheme = scheme;
}

String Context::getInterserverScheme() const
{
    return shared->interserver_scheme;
}

void Context::setRemoteHostFilter(const Poco::Util::AbstractConfiguration & config)
{
    shared->remote_host_filter.setValuesFromConfig(config);
}

const RemoteHostFilter & Context::getRemoteHostFilter() const
{
    return shared->remote_host_filter;
}

UInt16 Context::getTCPPort() const
{
    auto lock = getLock();

    const auto & config = getConfigRef();
    return config.getInt("tcp_port", DBMS_DEFAULT_PORT);
}

std::optional<UInt16> Context::getTCPPortSecure() const
{
    auto lock = getLock();

    const auto & config = getConfigRef();
    if (config.has("tcp_port_secure"))
        return config.getInt("tcp_port_secure");
    return {};
}

void Context::registerServerPort(String port_name, UInt16 port)
{
    shared->server_ports.emplace(std::move(port_name), port);
}

UInt16 Context::getServerPort(const String & port_name) const
{
    auto it = shared->server_ports.find(port_name);
    if (it == shared->server_ports.end())
        throw Exception(ErrorCodes::BAD_GET, "There is no port named {}", port_name);
    else
        return it->second;
}

std::shared_ptr<Cluster> Context::getCluster(const std::string & cluster_name) const
{
    if (auto res = tryGetCluster(cluster_name))
        return res;
    throw Exception(ErrorCodes::BAD_GET, "Requested cluster '{}' not found", cluster_name);
}


std::shared_ptr<Cluster> Context::tryGetCluster(const std::string & cluster_name) const
{
    auto res = getClusters()->getCluster(cluster_name);
    if (res)
        return res;
    if (!cluster_name.empty())
        res = tryGetReplicatedDatabaseCluster(cluster_name);
    return res;
}


void Context::reloadClusterConfig() const
{
    while (true)
    {
        ConfigurationPtr cluster_config;
        {
            std::lock_guard lock(shared->clusters_mutex);
            cluster_config = shared->clusters_config;
        }

        const auto & config = cluster_config ? *cluster_config : getConfigRef();
        auto new_clusters = std::make_shared<Clusters>(config, settings, getMacros());

        {
            std::lock_guard lock(shared->clusters_mutex);
            if (shared->clusters_config.get() == cluster_config.get())
            {
                shared->clusters = std::move(new_clusters);
                return;
            }

            // Clusters config has been suddenly changed, recompute clusters
        }
    }
}


std::shared_ptr<Clusters> Context::getClusters() const
{
    std::lock_guard lock(shared->clusters_mutex);
    if (!shared->clusters)
    {
        const auto & config = shared->clusters_config ? *shared->clusters_config : getConfigRef();
        shared->clusters = std::make_shared<Clusters>(config, settings, getMacros());
    }

    return shared->clusters;
}

void Context::startClusterDiscovery()
{
    if (!shared->cluster_discovery)
        return;
    shared->cluster_discovery->start();
}


/// On repeating calls updates existing clusters and adds new clusters, doesn't delete old clusters
void Context::setClustersConfig(const ConfigurationPtr & config, bool enable_discovery, const String & config_name)
{
    std::lock_guard lock(shared->clusters_mutex);
    if (ConfigHelper::getBool(*config, "allow_experimental_cluster_discovery") && enable_discovery && !shared->cluster_discovery)
    {
        shared->cluster_discovery = std::make_unique<ClusterDiscovery>(*config, getGlobalContext());
    }

    /// Do not update clusters if this part of config wasn't changed.
    if (shared->clusters && isSameConfiguration(*config, *shared->clusters_config, config_name))
        return;

    auto old_clusters_config = shared->clusters_config;
    shared->clusters_config = config;

    if (!shared->clusters)
        shared->clusters = std::make_shared<Clusters>(*shared->clusters_config, settings, getMacros(), config_name);
    else
        shared->clusters->updateClusters(*shared->clusters_config, settings, config_name, old_clusters_config);
}


void Context::setCluster(const String & cluster_name, const std::shared_ptr<Cluster> & cluster)
{
    std::lock_guard lock(shared->clusters_mutex);

    if (!shared->clusters)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Clusters are not set");

    shared->clusters->setCluster(cluster_name, cluster);
}


void Context::initializeSystemLogs()
{
    auto lock = getLock();
    shared->system_logs = std::make_unique<SystemLogs>(getGlobalContext(), getConfigRef());
}

void Context::initializeTraceCollector()
{
    shared->initializeTraceCollector(getTraceLog());
}

#if USE_ROCKSDB
void Context::initializeMergeTreeMetadataCache(const String & dir, size_t size)
{
    shared->merge_tree_metadata_cache = MergeTreeMetadataCache::create(dir, size);
}
#endif

bool Context::hasTraceCollector() const
{
    return shared->hasTraceCollector();
}


std::shared_ptr<QueryLog> Context::getQueryLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_log;
}

std::shared_ptr<QueryThreadLog> Context::getQueryThreadLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_thread_log;
}

std::shared_ptr<QueryViewsLog> Context::getQueryViewsLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_views_log;
}

std::shared_ptr<PartLog> Context::getPartLog(const String & part_database) const
{
    auto lock = getLock();

    /// No part log or system logs are shutting down.
    if (!shared->system_logs)
        return {};

    /// Will not log operations on system tables (including part_log itself).
    /// It doesn't make sense and not allow to destruct PartLog correctly due to infinite logging and flushing,
    /// and also make troubles on startup.
    if (part_database == DatabaseCatalog::SYSTEM_DATABASE)
        return {};

    return shared->system_logs->part_log;
}


std::shared_ptr<TraceLog> Context::getTraceLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->trace_log;
}


std::shared_ptr<TextLog> Context::getTextLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->text_log;
}


std::shared_ptr<MetricLog> Context::getMetricLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->metric_log;
}


std::shared_ptr<AsynchronousMetricLog> Context::getAsynchronousMetricLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_metric_log;
}


std::shared_ptr<OpenTelemetrySpanLog> Context::getOpenTelemetrySpanLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->opentelemetry_span_log;
}

std::shared_ptr<SessionLog> Context::getSessionLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->session_log;
}


std::shared_ptr<ZooKeeperLog> Context::getZooKeeperLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->zookeeper_log;
}


std::shared_ptr<TransactionsInfoLog> Context::getTransactionsInfoLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->transactions_info_log;
}


std::shared_ptr<ProcessorsProfileLog> Context::getProcessorsProfileLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->processors_profile_log;
}

std::shared_ptr<FilesystemCacheLog> Context::getFilesystemCacheLog() const
{
    auto lock = getLock();
    if (!shared->system_logs)
        return {};

    return shared->system_logs->filesystem_cache_log;
}

std::shared_ptr<FilesystemReadPrefetchesLog> Context::getFilesystemReadPrefetchesLog() const
{
    auto lock = getLock();
    if (!shared->system_logs)
        return {};

    return shared->system_logs->filesystem_read_prefetches_log;
}

std::shared_ptr<AsynchronousInsertLog> Context::getAsynchronousInsertLog() const
{
    auto lock = getLock();

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_insert_log;
}

CompressionCodecPtr Context::chooseCompressionCodec(size_t part_size, double part_size_ratio) const
{
    auto lock = getLock();

    if (!shared->compression_codec_selector)
    {
        constexpr auto config_name = "compression";
        const auto & config = getConfigRef();

        if (config.has(config_name))
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>(config, "compression");
        else
            shared->compression_codec_selector = std::make_unique<CompressionCodecSelector>();
    }

    return shared->compression_codec_selector->choose(part_size, part_size_ratio);
}


DiskPtr Context::getDisk(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    return disk_selector->get(name);
}

DiskPtr Context::getOrCreateDisk(const String & name, DiskCreator creator) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto disk_selector = getDiskSelector(lock);

    auto disk = disk_selector->tryGet(name);
    if (!disk)
    {
        disk = creator(getDisksMap(lock));
        const_cast<DiskSelector *>(disk_selector.get())->addToDiskMap(name, disk);
    }

    return disk;
}

StoragePolicyPtr Context::getStoragePolicy(const String & name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto policy_selector = getStoragePolicySelector(lock);

    return policy_selector->get(name);
}

StoragePolicyPtr Context::getStoragePolicyFromDisk(const String & disk_name) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    const std::string storage_policy_name = StoragePolicySelector::TMP_STORAGE_POLICY_PREFIX + disk_name;
    auto storage_policy_selector = getStoragePolicySelector(lock);
    StoragePolicyPtr storage_policy = storage_policy_selector->tryGet(storage_policy_name);

    if (!storage_policy)
    {
        auto disk_selector = getDiskSelector(lock);
        auto disk = disk_selector->get(disk_name);
        auto volume = std::make_shared<SingleDiskVolume>("_volume_" + disk_name, disk);

        static const auto move_factor_for_single_disk_volume = 0.0;
        storage_policy = std::make_shared<StoragePolicy>(storage_policy_name, Volumes{volume}, move_factor_for_single_disk_volume);
        const_cast<StoragePolicySelector *>(storage_policy_selector.get())->add(storage_policy);
    }
    /// Note: it is important to put storage policy into disk selector (and not recreate it on each call)
    /// because in some places there are checks that storage policy pointers are the same from different tables.
    /// (We can assume that tables with the same `disk` setting are on the same storage policy).

    return storage_policy;
}

DisksMap Context::getDisksMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getDisksMap(lock);
}

DisksMap Context::getDisksMap(std::lock_guard<std::mutex> & lock) const
{
    return getDiskSelector(lock)->getDisksMap();
}

StoragePoliciesMap Context::getPoliciesMap() const
{
    std::lock_guard lock(shared->storage_policies_mutex);
    return getStoragePolicySelector(lock)->getPoliciesMap();
}

DiskSelectorPtr Context::getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const
{
    if (!shared->merge_tree_disk_selector)
    {
        constexpr auto config_name = "storage_configuration.disks";
        const auto & config = getConfigRef();

        auto disk_selector = std::make_shared<DiskSelector>();
        disk_selector->initialize(config, config_name, shared_from_this());
        shared->merge_tree_disk_selector = disk_selector;
    }
    return shared->merge_tree_disk_selector;
}

StoragePolicySelectorPtr Context::getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const
{
    if (!shared->merge_tree_storage_policy_selector)
    {
        constexpr auto config_name = "storage_configuration.policies";
        const auto & config = getConfigRef();

        shared->merge_tree_storage_policy_selector = std::make_shared<StoragePolicySelector>(config, config_name, getDiskSelector(lock));
    }
    return shared->merge_tree_storage_policy_selector;
}


void Context::updateStorageConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->storage_policies_mutex);

    if (shared->merge_tree_disk_selector)
        shared->merge_tree_disk_selector
            = shared->merge_tree_disk_selector->updateFromConfig(config, "storage_configuration.disks", shared_from_this());

    if (shared->merge_tree_storage_policy_selector)
    {
        try
        {
            shared->merge_tree_storage_policy_selector = shared->merge_tree_storage_policy_selector->updateFromConfig(
                config, "storage_configuration.policies", shared->merge_tree_disk_selector);
        }
        catch (Exception & e)
        {
            LOG_ERROR(
                shared->log, "An error has occurred while reloading storage policies, storage policies were not applied: {}", e.message());
        }
    }

    if (shared->storage_s3_settings)
    {
        shared->storage_s3_settings->loadFromConfig("s3", config, getSettingsRef());
    }
}


const MergeTreeSettings & Context::getMergeTreeSettings() const
{
    auto lock = getLock();

    if (!shared->merge_tree_settings)
    {
        const auto & config = getConfigRef();
        MergeTreeSettings mt_settings;
        mt_settings.loadFromConfig("merge_tree", config);
        shared->merge_tree_settings.emplace(mt_settings);
    }

    return *shared->merge_tree_settings;
}

const MergeTreeSettings & Context::getReplicatedMergeTreeSettings() const
{
    auto lock = getLock();

    if (!shared->replicated_merge_tree_settings)
    {
        const auto & config = getConfigRef();
        MergeTreeSettings mt_settings;
        mt_settings.loadFromConfig("merge_tree", config);
        mt_settings.loadFromConfig("replicated_merge_tree", config);
        shared->replicated_merge_tree_settings.emplace(mt_settings);
    }

    return *shared->replicated_merge_tree_settings;
}

const StorageS3Settings & Context::getStorageS3Settings() const
{
    auto lock = getLock();

    if (!shared->storage_s3_settings)
    {
        const auto & config = getConfigRef();
        shared->storage_s3_settings.emplace().loadFromConfig("s3", config, getSettingsRef());
    }

    return *shared->storage_s3_settings;
}

void Context::checkCanBeDropped(const String & database, const String & table, const size_t & size, const size_t & max_size_to_drop) const
{
    if (!max_size_to_drop || size <= max_size_to_drop)
        return;

    fs::path force_file(getFlagsPath() + "force_drop_table");
    bool force_file_exists = fs::exists(force_file);

    if (force_file_exists)
    {
        try
        {
            fs::remove(force_file);
            return;
        }
        catch (...)
        {
            /// User should recreate force file on each drop, it shouldn't be protected
            tryLogCurrentException("Drop table check", "Can't remove force file to enable table or partition drop");
        }
    }

    String size_str = formatReadableSizeWithDecimalSuffix(size);
    String max_size_to_drop_str = formatReadableSizeWithDecimalSuffix(max_size_to_drop);
    throw Exception(ErrorCodes::TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT,
                    "Table or Partition in {}.{} was not dropped.\nReason:\n"
                    "1. Size ({}) is greater than max_[table/partition]_size_to_drop ({})\n"
                    "2. File '{}' intended to force DROP {}\n"
                    "How to fix this:\n"
                    "1. Either increase (or set to zero) max_[table/partition]_size_to_drop in server config\n"
                    "2. Either create forcing file {} and make sure that ClickHouse has write permission for it.\n"
                    "Example:\nsudo touch '{}' && sudo chmod 666 '{}'",
                    backQuoteIfNeed(database), backQuoteIfNeed(table),
                    size_str, max_size_to_drop_str,
                    force_file.string(), force_file_exists ? "exists but not writeable (could not be removed)" : "doesn't exist",
                    force_file.string(),
                    force_file.string(), force_file.string());
}


void Context::setMaxTableSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_table_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_table_size_to_drop = shared->max_table_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, table_size, max_table_size_to_drop);
}


void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_partition_size_to_drop.store(max_size, std::memory_order_relaxed);
}


void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop.load(std::memory_order_relaxed);

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}


InputFormatPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size, format_settings);
}

OutputFormatPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormat(name, buf, sample, shared_from_this());
}

OutputFormatPtr Context::getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample) const
{
    return FormatFactory::instance().getOutputFormatParallelIfPossible(name, buf, sample, shared_from_this());
}


double Context::getUptimeSeconds() const
{
    auto lock = getLock();
    return shared->uptime_watch.elapsedSeconds();
}


void Context::setConfigReloadCallback(ConfigReloadCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->config_reload_callback = std::move(callback);
}

void Context::reloadConfig() const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->config_reload_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't reload config because config_reload_callback is not set.");

    shared->config_reload_callback();
}


void Context::shutdown()
{
    // Disk selector might not be initialized if there was some error during
    // its initialization. Don't try to initialize it again on shutdown.
    if (shared->merge_tree_disk_selector)
    {
        for (auto & [disk_name, disk] : getDisksMap())
        {
            LOG_INFO(shared->log, "Shutdown disk {}", disk_name);
            disk->shutdown();
        }
    }

    /// Special volumes might also use disks that require shutdown.
    auto & tmp_data = shared->temp_data_on_disk;
    if (tmp_data && tmp_data->getVolume())
    {
        auto & disks = tmp_data->getVolume()->getDisks();
        for (auto & disk : disks)
            disk->shutdown();
    }

    shared->shutdown();
}


Context::ApplicationType Context::getApplicationType() const
{
    return shared->application_type;
}

void Context::setApplicationType(ApplicationType type)
{
    /// Lock isn't required, you should set it at start
    shared->application_type = type;
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    getAccessControl().setDefaultProfileName(shared->default_profile_name);

    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);
    setCurrentProfile(shared->system_profile_name);

    applySettingsQuirks(settings, &Poco::Logger::get("SettingsQuirks"));

    shared->buffer_profile_name = config.getString("buffer_profile", shared->system_profile_name);
    buffer_context = Context::createCopy(shared_from_this());
    buffer_context->setCurrentProfile(shared->buffer_profile_name);
}

String Context::getDefaultProfileName() const
{
    return shared->default_profile_name;
}

String Context::getSystemProfileName() const
{
    return shared->system_profile_name;
}

String Context::getFormatSchemaPath() const
{
    return shared->format_schema_path;
}

void Context::setFormatSchemaPath(const String & path)
{
    shared->format_schema_path = path;
}

Context::SampleBlockCache & Context::getSampleBlockCache() const
{
    assert(hasQueryContext());
    return getQueryContext()->sample_block_cache;
}


bool Context::hasQueryParameters() const
{
    return !query_parameters.empty();
}


const NameToNameMap & Context::getQueryParameters() const
{
    return query_parameters;
}


void Context::setQueryParameter(const String & name, const String & value)
{
    if (!query_parameters.emplace(name, value).second)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Duplicate name {} of query parameter", backQuote(name));
}

void Context::addQueryParameters(const NameToNameMap & parameters)
{
    for (const auto & [name, value] : parameters)
        query_parameters.insert_or_assign(name, value);
}

void Context::addBridgeCommand(std::unique_ptr<ShellCommand> cmd) const
{
    auto lock = getLock();
    shared->bridge_commands.emplace_back(std::move(cmd));
}


IHostContextPtr & Context::getHostContext()
{
    return host_context;
}


const IHostContextPtr & Context::getHostContext() const
{
    return host_context;
}


std::shared_ptr<ActionLocksManager> Context::getActionLocksManager() const
{
    auto lock = getLock();

    if (!shared->action_locks_manager)
        shared->action_locks_manager = std::make_shared<ActionLocksManager>(shared_from_this());

    return shared->action_locks_manager;
}


void Context::setExternalTablesInitializer(ExternalTablesInitializer && initializer)
{
    if (external_tables_initializer_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "External tables initializer is already set");

    external_tables_initializer_callback = std::move(initializer);
}

void Context::initializeExternalTablesIfSet()
{
    if (external_tables_initializer_callback)
    {
        external_tables_initializer_callback(shared_from_this());
        /// Reset callback
        external_tables_initializer_callback = {};
    }
}


void Context::setInputInitializer(InputInitializer && initializer)
{
    if (input_initializer_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input initializer is already set");

    input_initializer_callback = std::move(initializer);
}


void Context::initializeInput(const StoragePtr & input_storage)
{
    if (!input_initializer_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input initializer is not set");

    input_initializer_callback(shared_from_this(), input_storage);
    /// Reset callback
    input_initializer_callback = {};
}


void Context::setInputBlocksReaderCallback(InputBlocksReader && reader)
{
    if (input_blocks_reader)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Input blocks reader is already set");

    input_blocks_reader = std::move(reader);
}


InputBlocksReader Context::getInputBlocksReaderCallback() const
{
    return input_blocks_reader;
}


void Context::resetInputCallbacks()
{
    if (input_initializer_callback)
        input_initializer_callback = {};

    if (input_blocks_reader)
        input_blocks_reader = {};
}


StorageID Context::resolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    std::optional<Exception> exc;
    {
        auto lock = getLock();
        resolved = resolveStorageIDImpl(std::move(storage_id), where, &exc);
    }
    if (exc)
        throw Exception(*exc);
    if (!resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
        resolved.uuid = DatabaseCatalog::instance().getDatabase(resolved.database_name)->tryGetTableUUID(resolved.table_name);
    return resolved;
}

StorageID Context::tryResolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    {
        auto lock = getLock();
        resolved = resolveStorageIDImpl(std::move(storage_id), where, nullptr);
    }
    if (resolved && !resolved.hasUUID() && resolved.database_name != DatabaseCatalog::TEMPORARY_DATABASE)
    {
        auto db = DatabaseCatalog::instance().tryGetDatabase(resolved.database_name);
        if (db)
            resolved.uuid = db->tryGetTableUUID(resolved.table_name);
    }
    return resolved;
}

StorageID Context::resolveStorageIDImpl(StorageID storage_id, StorageNamespace where, std::optional<Exception> * exception) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    if (!storage_id)
    {
        if (exception)
            exception->emplace(ErrorCodes::UNKNOWN_TABLE, "Both table name and UUID are empty");
        return storage_id;
    }

    bool look_for_external_table = where & StorageNamespace::ResolveExternal;
    /// Global context should not contain temporary tables
    if (isGlobalContext())
        look_for_external_table = false;

    bool in_current_database = where & StorageNamespace::ResolveCurrentDatabase;
    bool in_specified_database = where & StorageNamespace::ResolveGlobal;

    if (!storage_id.database_name.empty())
    {
        if (in_specified_database)
            return storage_id;     /// NOTE There is no guarantees that table actually exists in database.
        if (exception)
            exception->emplace(Exception(ErrorCodes::UNKNOWN_TABLE, "External and temporary tables have no database, but {} is specified",
                               storage_id.database_name));
        return StorageID::createEmpty();
    }

    /// Database name is not specified. It's temporary table or table in current database.

    if (look_for_external_table)
    {
        auto resolved_id = StorageID::createEmpty();
        auto try_resolve = [&](ContextPtr context) -> bool
        {
            const auto & tables = context->external_tables_mapping;
            auto it = tables.find(storage_id.getTableName());
            if (it == tables.end())
                return false;
            resolved_id = it->second->getGlobalTableID();
            return true;
        };

        /// Firstly look for temporary table in current context
        if (try_resolve(shared_from_this()))
            return resolved_id;

        /// If not found and current context was created from some query context, look for temporary table in query context
        auto query_context_ptr = query_context.lock();
        bool is_local_context = query_context_ptr && query_context_ptr.get() != this;
        if (is_local_context && try_resolve(query_context_ptr))
            return resolved_id;

        /// If not found and current context was created from some session context, look for temporary table in session context
        auto session_context_ptr = session_context.lock();
        bool is_local_or_query_context = session_context_ptr && session_context_ptr.get() != this;
        if (is_local_or_query_context && try_resolve(session_context_ptr))
            return resolved_id;
    }

    /// Temporary table not found. It's table in current database.

    if (in_current_database)
    {
        if (current_database.empty())
        {
            if (exception)
                exception->emplace(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected");
            return StorageID::createEmpty();
        }
        storage_id.database_name = current_database;
        /// NOTE There is no guarantees that table actually exists in database.
        return storage_id;
    }

    if (exception)
        exception->emplace(Exception(ErrorCodes::UNKNOWN_TABLE, "Cannot resolve database name for table {}", storage_id.getNameForLogs()));
    return StorageID::createEmpty();
}

void Context::initZooKeeperMetadataTransaction(ZooKeeperMetadataTransactionPtr txn, [[maybe_unused]] bool attach_existing)
{
    assert(!metadata_transaction);
    assert(attach_existing || query_context.lock().get() == this);
    metadata_transaction = std::move(txn);
}

ZooKeeperMetadataTransactionPtr Context::getZooKeeperMetadataTransaction() const
{
    assert(!metadata_transaction || hasQueryContext());
    return metadata_transaction;
}

void Context::resetZooKeeperMetadataTransaction()
{
    assert(metadata_transaction);
    assert(hasQueryContext());
    metadata_transaction = nullptr;
}


void Context::checkTransactionsAreAllowed(bool explicit_tcl_query /* = false */) const
{
    if (getConfigRef().getInt("allow_experimental_transactions", 0))
        return;

    if (explicit_tcl_query)
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Transactions are not supported");

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Experimental support for transactions is disabled, "
                    "however, some query or background task tried to access TransactionLog. "
                    "If you have not enabled this feature explicitly, then it's a bug.");
}

void Context::initCurrentTransaction(MergeTreeTransactionPtr txn)
{
    merge_tree_transaction_holder = MergeTreeTransactionHolder(txn, false, this);
    setCurrentTransaction(std::move(txn));
}

void Context::setCurrentTransaction(MergeTreeTransactionPtr txn)
{
    assert(!merge_tree_transaction || !txn);
    assert(this == session_context.lock().get() || this == query_context.lock().get());
    merge_tree_transaction = std::move(txn);
    if (!merge_tree_transaction)
        merge_tree_transaction_holder = {};
}

MergeTreeTransactionPtr Context::getCurrentTransaction() const
{
    return merge_tree_transaction;
}

bool Context::isServerCompletelyStarted() const
{
    auto lock = getLock();
    assert(getApplicationType() == ApplicationType::SERVER);
    return shared->is_server_completely_started;
}

void Context::setServerCompletelyStarted()
{
    {
        std::lock_guard lock(shared->zookeeper_mutex);
        if (shared->zookeeper)
            shared->zookeeper->setServerCompletelyStarted();

        for (auto & zk : shared->auxiliary_zookeepers)
            zk.second->setServerCompletelyStarted();
    }

    auto lock = getLock();
    assert(global_context.lock().get() == this);
    assert(!shared->is_server_completely_started);
    assert(getApplicationType() == ApplicationType::SERVER);
    shared->is_server_completely_started = true;
}

PartUUIDsPtr Context::getPartUUIDs() const
{
    auto lock = getLock();
    if (!part_uuids)
        /// For context itself, only this initialization is not const.
        /// We could have done in constructor.
        /// TODO: probably, remove this from Context.
        const_cast<PartUUIDsPtr &>(part_uuids) = std::make_shared<PartUUIDs>();

    return part_uuids;
}


ReadTaskCallback Context::getReadTaskCallback() const
{
    if (!next_task_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback is not set for query {}", getInitialQueryId());
    return next_task_callback.value();
}


void Context::setReadTaskCallback(ReadTaskCallback && callback)
{
    next_task_callback = callback;
}


MergeTreeReadTaskCallback Context::getMergeTreeReadTaskCallback() const
{
    if (!merge_tree_read_task_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback for is not set for query {}", getInitialQueryId());

    return merge_tree_read_task_callback.value();
}

void Context::setMergeTreeReadTaskCallback(MergeTreeReadTaskCallback && callback)
{
    merge_tree_read_task_callback = callback;
}


MergeTreeAllRangesCallback Context::getMergeTreeAllRangesCallback() const
{
    if (!merge_tree_all_ranges_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback is not set for query with id: {}", getInitialQueryId());

    return merge_tree_all_ranges_callback.value();
}


void Context::setMergeTreeAllRangesCallback(MergeTreeAllRangesCallback && callback)
{
    merge_tree_all_ranges_callback = callback;
}


void Context::setParallelReplicasGroupUUID(UUID uuid)
{
    parallel_replicas_group_uuid = uuid;
}

UUID Context::getParallelReplicasGroupUUID() const
{
    return parallel_replicas_group_uuid;
}

PartUUIDsPtr Context::getIgnoredPartUUIDs() const
{
    auto lock = getLock();
    if (!ignored_part_uuids)
        const_cast<PartUUIDsPtr &>(ignored_part_uuids) = std::make_shared<PartUUIDs>();

    return ignored_part_uuids;
}

AsynchronousInsertQueue * Context::getAsynchronousInsertQueue() const
{
    return shared->async_insert_queue.get();
}

void Context::setAsynchronousInsertQueue(const std::shared_ptr<AsynchronousInsertQueue> & ptr)
{
    using namespace std::chrono;

    if (std::chrono::milliseconds(settings.async_insert_busy_timeout_ms) == 0ms)
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting async_insert_busy_timeout_ms can't be zero");

    shared->async_insert_queue = ptr;
}

void Context::initializeBackgroundExecutorsIfNeeded()
{
    auto lock = getLock();
    if (shared->are_background_executors_initialized)
        return;

    const auto & config = getConfigRef();

    ServerSettings server_settings;
    server_settings.loadSettingsFromConfig(config);

    size_t background_pool_size = server_settings.background_pool_size;
    auto background_merges_mutations_concurrency_ratio = server_settings.background_merges_mutations_concurrency_ratio;
    size_t background_pool_max_tasks_count = static_cast<size_t>(background_pool_size * background_merges_mutations_concurrency_ratio);
    String background_merges_mutations_scheduling_policy = server_settings.background_merges_mutations_scheduling_policy;
    size_t background_move_pool_size = server_settings.background_move_pool_size;
    size_t background_fetches_pool_size = server_settings.background_fetches_pool_size;
    size_t background_common_pool_size = server_settings.background_common_pool_size;

    /// With this executor we can execute more tasks than threads we have
    shared->merge_mutate_executor = std::make_shared<MergeMutateBackgroundExecutor>
    (
        "MergeMutate",
        /*max_threads_count*/background_pool_size,
        /*max_tasks_count*/background_pool_max_tasks_count,
        CurrentMetrics::BackgroundMergesAndMutationsPoolTask,
        CurrentMetrics::BackgroundMergesAndMutationsPoolSize,
        background_merges_mutations_scheduling_policy
    );
    LOG_INFO(shared->log, "Initialized background executor for merges and mutations with num_threads={}, num_tasks={}, scheduling_policy={}",
        background_pool_size, background_pool_max_tasks_count, background_merges_mutations_scheduling_policy);

    shared->moves_executor = std::make_shared<OrdinaryBackgroundExecutor>
    (
        "Move",
        background_move_pool_size,
        background_move_pool_size,
        CurrentMetrics::BackgroundMovePoolTask,
        CurrentMetrics::BackgroundMovePoolSize
    );
    LOG_INFO(shared->log, "Initialized background executor for move operations with num_threads={}, num_tasks={}", background_move_pool_size, background_move_pool_size);

    shared->fetch_executor = std::make_shared<OrdinaryBackgroundExecutor>
    (
        "Fetch",
        background_fetches_pool_size,
        background_fetches_pool_size,
        CurrentMetrics::BackgroundFetchesPoolTask,
        CurrentMetrics::BackgroundFetchesPoolSize
    );
    LOG_INFO(shared->log, "Initialized background executor for fetches with num_threads={}, num_tasks={}", background_fetches_pool_size, background_fetches_pool_size);

    shared->common_executor = std::make_shared<OrdinaryBackgroundExecutor>
    (
        "Common",
        background_common_pool_size,
        background_common_pool_size,
        CurrentMetrics::BackgroundCommonPoolTask,
        CurrentMetrics::BackgroundCommonPoolSize
    );
    LOG_INFO(shared->log, "Initialized background executor for common operations (e.g. clearing old parts) with num_threads={}, num_tasks={}", background_common_pool_size, background_common_pool_size);

    shared->are_background_executors_initialized = true;
}

bool Context::areBackgroundExecutorsInitialized()
{
    auto lock = getLock();
    return shared->are_background_executors_initialized;
}

MergeMutateBackgroundExecutorPtr Context::getMergeMutateExecutor() const
{
    return shared->merge_mutate_executor;
}

OrdinaryBackgroundExecutorPtr Context::getMovesExecutor() const
{
    return shared->moves_executor;
}

OrdinaryBackgroundExecutorPtr Context::getFetchesExecutor() const
{
    return shared->fetch_executor;
}

OrdinaryBackgroundExecutorPtr Context::getCommonExecutor() const
{
    return shared->common_executor;
}

static size_t getThreadPoolReaderSizeFromConfig(Context::FilesystemReaderType type, const Poco::Util::AbstractConfiguration & config)
{
    switch (type)
    {
        case Context::FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
        {
            return config.getUInt(".threadpool_remote_fs_reader_pool_size", 250);
        }
        case Context::FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
        {
            return config.getUInt(".threadpool_local_fs_reader_pool_size", 100);
        }
        case Context::FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
        {
            return std::numeric_limits<std::size_t>::max();
        }
    }
}

size_t Context::getThreadPoolReaderSize(FilesystemReaderType type) const
{
    const auto & config = getConfigRef();
    return getThreadPoolReaderSizeFromConfig(type, config);
}

IAsynchronousReader & Context::getThreadPoolReader(FilesystemReaderType type) const
{
    const auto & config = getConfigRef();

    auto lock = getLock();

    switch (type)
    {
        case FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
        {
            if (!shared->asynchronous_remote_fs_reader)
            {
                auto pool_size = getThreadPoolReaderSizeFromConfig(type, config);
                auto queue_size = config.getUInt(".threadpool_remote_fs_reader_queue_size", 1000000);
                shared->asynchronous_remote_fs_reader = std::make_unique<ThreadPoolRemoteFSReader>(pool_size, queue_size);
            }

            return *shared->asynchronous_remote_fs_reader;
        }
        case FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
        {
            if (!shared->asynchronous_local_fs_reader)
            {
                auto pool_size = getThreadPoolReaderSizeFromConfig(type, config);
                auto queue_size = config.getUInt(".threadpool_local_fs_reader_queue_size", 1000000);
                shared->asynchronous_local_fs_reader = std::make_unique<ThreadPoolReader>(pool_size, queue_size);
            }

            return *shared->asynchronous_local_fs_reader;
        }
        case FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
        {
            if (!shared->synchronous_local_fs_reader)
            {
                shared->synchronous_local_fs_reader = std::make_unique<SynchronousReader>();
            }

            return *shared->synchronous_local_fs_reader;
        }
    }
}

ThreadPool & Context::getThreadPoolWriter() const
{
    const auto & config = getConfigRef();

    auto lock = getLock();

    if (!shared->threadpool_writer)
    {
        auto pool_size = config.getUInt(".threadpool_writer_pool_size", 100);
        auto queue_size = config.getUInt(".threadpool_writer_queue_size", 1000000);

        shared->threadpool_writer = std::make_unique<ThreadPool>(
            CurrentMetrics::IOWriterThreads, CurrentMetrics::IOWriterThreadsActive, pool_size, pool_size, queue_size);
    }

    return *shared->threadpool_writer;
}

ReadSettings Context::getReadSettings() const
{
    ReadSettings res;

    std::string_view read_method_str = settings.local_filesystem_read_method.value;

    if (auto opt_method = magic_enum::enum_cast<LocalFSReadMethod>(read_method_str))
        res.local_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for local filesystem", read_method_str);

    read_method_str = settings.remote_filesystem_read_method.value;

    if (auto opt_method = magic_enum::enum_cast<RemoteFSReadMethod>(read_method_str))
        res.remote_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for remote filesystem", read_method_str);

    res.local_fs_prefetch = settings.local_filesystem_read_prefetch;
    res.remote_fs_prefetch = settings.remote_filesystem_read_prefetch;

    res.load_marks_asynchronously = settings.load_marks_asynchronously;

    res.enable_filesystem_read_prefetches_log = settings.enable_filesystem_read_prefetches_log;

    res.remote_fs_read_max_backoff_ms = settings.remote_fs_read_max_backoff_ms;
    res.remote_fs_read_backoff_max_tries = settings.remote_fs_read_backoff_max_tries;
    res.enable_filesystem_cache = settings.enable_filesystem_cache;
    res.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    res.enable_filesystem_cache_log = settings.enable_filesystem_cache_log;
    res.enable_filesystem_cache_on_lower_level = settings.enable_filesystem_cache_on_lower_level;

    res.filesystem_cache_max_download_size = settings.filesystem_cache_max_download_size;
    res.skip_download_if_exceeds_query_cache = settings.skip_download_if_exceeds_query_cache;

    res.remote_read_min_bytes_for_seek = settings.remote_read_min_bytes_for_seek;

    /// Zero read buffer will not make progress.
    if (!settings.max_read_buffer_size)
    {
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE,
            "Invalid value '{}' for max_read_buffer_size", settings.max_read_buffer_size);
    }

    res.local_fs_buffer_size = settings.max_read_buffer_size;
    res.remote_fs_buffer_size = settings.max_read_buffer_size;
    res.direct_io_threshold = settings.min_bytes_to_use_direct_io;
    res.mmap_threshold = settings.min_bytes_to_use_mmap_io;
    res.priority = settings.read_priority;

    res.remote_throttler = getRemoteReadThrottler();

    res.http_max_tries = settings.http_max_tries;
    res.http_retry_initial_backoff_ms = settings.http_retry_initial_backoff_ms;
    res.http_retry_max_backoff_ms = settings.http_retry_max_backoff_ms;
    res.http_skip_not_found_url_for_globs = settings.http_skip_not_found_url_for_globs;

    res.mmap_cache = getMMappedFileCache().get();

    return res;
}

WriteSettings Context::getWriteSettings() const
{
    WriteSettings res;

    res.enable_filesystem_cache_on_write_operations = settings.enable_filesystem_cache_on_write_operations;
    res.enable_filesystem_cache_log = settings.enable_filesystem_cache_log;
    res.throw_on_error_from_cache = settings.throw_on_error_from_cache_on_write_operations;

    res.s3_allow_parallel_part_upload = settings.s3_allow_parallel_part_upload;

    res.remote_throttler = getRemoteWriteThrottler();

    return res;
}

std::shared_ptr<AsyncReadCounters> Context::getAsyncReadCounters() const
{
    auto lock = getLock();
    if (!async_read_counters)
        async_read_counters = std::make_shared<AsyncReadCounters>();
    return async_read_counters;
}

Context::ParallelReplicasMode Context::getParallelReplicasMode() const
{
    const auto & settings = getSettingsRef();

    using enum Context::ParallelReplicasMode;
    if (!settings.parallel_replicas_custom_key.value.empty())
        return CUSTOM_KEY;

    if (settings.allow_experimental_parallel_reading_from_replicas
        && !settings.use_hedged_requests)
        return READ_TASKS;

    return SAMPLE_KEY;
}

bool Context::canUseParallelReplicasOnInitiator() const
{
    const auto & settings = getSettingsRef();
    return getParallelReplicasMode() == ParallelReplicasMode::READ_TASKS
        && settings.max_parallel_replicas > 1
        && !getClientInfo().collaborate_with_initiator;
}

bool Context::canUseParallelReplicasOnFollower() const
{
    const auto & settings = getSettingsRef();
    return getParallelReplicasMode() == ParallelReplicasMode::READ_TASKS
        && settings.max_parallel_replicas > 1
        && getClientInfo().collaborate_with_initiator;
}

UInt64 Context::getClientProtocolVersion() const
{
    return client_protocol_version;
}

void Context::setClientProtocolVersion(UInt64 version)
{
    client_protocol_version = version;
}

}
