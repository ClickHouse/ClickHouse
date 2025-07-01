#include <map>
#include <set>
#include <optional>
#include <memory>
#include <Poco/UUID.h>
#include <Poco/Util/Application.h>
#include <Common/ISlotControl.h>
#include <Common/Scheduler/IResourceManager.h>
#include <Common/AsyncLoader.h>
#include <Common/CgroupsMemoryUsageObserver.h>
#include <Common/PoolId.h>
#include <Common/SensitiveDataMasker.h>
#include <Common/Macros.h>
#include <Common/EventNotifier.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/Stopwatch.h>
#include <Common/formatReadable.h>
#include <Common/Throttler.h>
#include <Common/thread_local_rng.h>
#include <Common/FieldVisitorToString.h>
#include <Common/getMultipleKeysFromConfig.h>
#include <Common/callOnce.h>
#include <Common/SharedLockGuard.h>
#include <Common/PageCache.h>
#include <Common/NamedCollections/NamedCollectionsFactory.h>
#include <Common/isLocalAddress.h>
#include <Common/ConcurrencyControl.h>
#include <Coordination/KeeperDispatcher.h>
#include <Core/BackgroundSchedulePool.h>
#include <Core/Settings.h>
#include <Formats/FormatFactory.h>
#include <Databases/IDatabase.h>
#include <Server/ServerType.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeList.h>
#include <Storages/MergeTree/MovesList.h>
#include <Storages/MergeTree/ReplicatedFetchList.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <Storages/Distributed/DistributedSettings.h>
#include <Storages/CompressionCodecSelector.h>
#include <IO/S3Settings.h>
#include <Disks/ObjectStorages/AzureBlobStorage/AzureBlobStorageCommon.h>
#include <Disks/DiskLocal.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Disks/SingleDiskVolume.h>
#include <Disks/StoragePolicy.h>
#include <Disks/IO/IOUringReader.h>
#include <Disks/IO/getIOUringReader.h>
#include <IO/SynchronousReader.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/ActionLocksManager.h>
#include <Interpreters/ExternalLoaderXMLConfigRepository.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Interpreters/Cache/FileCacheFactory.h>
#include <Interpreters/Cache/FileCache.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Cache/QueryResultCache.h>
#include <Interpreters/SessionTracker.h>
#include <Core/ServerSettings.h>
#include <Interpreters/PreparedSets.h>
#include <Core/SettingsQuirks.h>
#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/EnabledRolesInfo.h>
#include <Access/EnabledRowPolicies.h>
#include <Access/QuotaUsage.h>
#include <Access/User.h>
#include <Access/Role.h>
#include <Access/SettingsProfile.h>
#include <Access/SettingsProfilesInfo.h>
#include <Access/SettingsConstraintsAndProfileIDs.h>
#include <Access/ExternalAuthenticators.h>
#include <Access/GSSAcceptor.h>
#include <Backups/BackupsWorker.h>
#include <Dictionaries/Embedded/GeoDictionariesLoader.h>
#include <Interpreters/EmbeddedDictionaries.h>
#include <Interpreters/ExternalDictionariesLoader.h>
#include <Functions/UserDefined/ExternalUserDefinedExecutableFunctionsLoader.h>
#include <Functions/UserDefined/IUserDefinedSQLObjectsStorage.h>
#include <Functions/UserDefined/createUserDefinedSQLObjectsStorage.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/InterserverCredentials.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterserverIOHandler.h>
#include <Interpreters/Context.h>
#include <Interpreters/DDLWorker.h>
#include <Interpreters/DDLTask.h>
#include <Interpreters/Session.h>
#include <Interpreters/TraceCollector.h>
#include <IO/AsyncReadCounters.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadWriteBufferFromHTTP.h>
#include <IO/UncompressedCache.h>
#include <IO/MMappedFileCache.h>
#include <IO/WriteSettings.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTIdentifier.h>
#include <Common/Scheduler/createResourceManager.h>
#include <Common/Scheduler/Workload/createWorkloadEntityStorage.h>
#include <Common/StackTrace.h>
#include <Common/Config/ConfigHelper.h>
#include <Common/Config/ConfigProcessor.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/Config/AbstractConfigurationComparison.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/ShellCommand.h>
#include <Common/logger_useful.h>
#include <Common/RemoteHostFilter.h>
#include <Common/HTTPHeaderFilter.h>
#include <Interpreters/StorageID.h>
#include <Interpreters/SystemLog.h>
#include <Interpreters/InterpreterSelectQueryAnalyzer.h>
#include <Interpreters/AsynchronousInsertQueue.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/JIT/CompiledExpressionCache.h>
#include <Storages/MergeTree/BackgroundJobsAssignee.h>
#include <Storages/MergeTree/MergeTreeDataPartUUID.h>
#include <Storages/MaterializedView/RefreshSet.h>
#include <Interpreters/SynonymsExtensions.h>
#include <Interpreters/Lemmatizers.h>
#include <Interpreters/ClusterDiscovery.h>
#include <Interpreters/TransactionLog.h>
#include <filesystem>
#include <re2/re2.h>
#include <Storages/StorageView.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/FunctionParameterValuesVisitor.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Interpreters/InterpreterSelectWithUnionQuery.h>
#include <base/defines.h>

namespace fs = std::filesystem;

namespace ProfileEvents
{
    extern const Event ContextLock;
    extern const Event ContextLockWaitMicroseconds;
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
    extern const Metric MarksLoaderThreadsScheduled;
    extern const Metric IOPrefetchThreads;
    extern const Metric IOPrefetchThreadsActive;
    extern const Metric IOPrefetchThreadsScheduled;
    extern const Metric IOWriterThreads;
    extern const Metric IOWriterThreadsActive;
    extern const Metric TablesLoaderBackgroundThreads;
    extern const Metric TablesLoaderBackgroundThreadsActive;
    extern const Metric TablesLoaderBackgroundThreadsScheduled;
    extern const Metric TablesLoaderForegroundThreads;
    extern const Metric TablesLoaderForegroundThreadsActive;
    extern const Metric TablesLoaderForegroundThreadsScheduled;
    extern const Metric IOWriterThreadsScheduled;
    extern const Metric BuildVectorSimilarityIndexThreads;
    extern const Metric BuildVectorSimilarityIndexThreadsActive;
    extern const Metric BuildVectorSimilarityIndexThreadsScheduled;
    extern const Metric AttachedTable;
    extern const Metric AttachedView;
    extern const Metric AttachedDictionary;
    extern const Metric AttachedDatabase;
    extern const Metric PartsActive;
    extern const Metric IcebergCatalogThreads;
    extern const Metric IcebergCatalogThreadsActive;
    extern const Metric IcebergCatalogThreadsScheduled;
}


namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsMilliseconds async_insert_poll_timeout_ms;
    extern const SettingsBool azure_allow_parallel_part_upload;
    extern const SettingsString cluster_for_parallel_replicas;
    extern const SettingsBool enable_filesystem_cache;
    extern const SettingsBool enable_filesystem_cache_log;
    extern const SettingsBool enable_filesystem_cache_on_write_operations;
    extern const SettingsBool enable_filesystem_read_prefetches_log;
    extern const SettingsBool enable_blob_storage_log;
    extern const SettingsUInt64 filesystem_cache_max_download_size;
    extern const SettingsUInt64 filesystem_cache_reserve_space_wait_lock_timeout_milliseconds;
    extern const SettingsUInt64 filesystem_cache_segments_batch_size;
    extern const SettingsBool filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage;
    extern const SettingsBool filesystem_cache_enable_background_download_during_fetch;
    extern const SettingsBool filesystem_cache_prefer_bigger_buffer_size;
    extern const SettingsBool http_make_head_request;
    extern const SettingsUInt64 http_max_fields;
    extern const SettingsUInt64 http_max_field_name_size;
    extern const SettingsUInt64 http_max_field_value_size;
    extern const SettingsUInt64 http_max_tries;
    extern const SettingsUInt64 http_max_uri_size;
    extern const SettingsSeconds http_receive_timeout;
    extern const SettingsUInt64 http_retry_initial_backoff_ms;
    extern const SettingsUInt64 http_retry_max_backoff_ms;
    extern const SettingsSeconds http_send_timeout;
    extern const SettingsBool http_skip_not_found_url_for_globs;
    extern const SettingsUInt64 hsts_max_age;
    extern const SettingsString local_filesystem_read_method;
    extern const SettingsBool local_filesystem_read_prefetch;
    extern const SettingsBool load_marks_asynchronously;
    extern const SettingsUInt64 max_backup_bandwidth;
    extern const SettingsUInt64 max_local_read_bandwidth;
    extern const SettingsUInt64 max_local_write_bandwidth;
    extern const SettingsNonZeroUInt64 max_parallel_replicas;
    extern const SettingsNonZeroUInt64 max_read_buffer_size;
    extern const SettingsUInt64 max_read_buffer_size_local_fs;
    extern const SettingsUInt64 max_read_buffer_size_remote_fs;
    extern const SettingsUInt64 max_remote_read_network_bandwidth;
    extern const SettingsUInt64 max_remote_write_network_bandwidth;
    extern const SettingsUInt64 min_bytes_to_use_direct_io;
    extern const SettingsUInt64 min_bytes_to_use_mmap_io;
    extern const SettingsBool page_cache_inject_eviction;
    extern const SettingsParallelReplicasMode parallel_replicas_mode;
    extern const SettingsString parallel_replicas_custom_key;
    extern const SettingsUInt64 prefetch_buffer_size;
    extern const SettingsBool read_from_filesystem_cache_if_exists_otherwise_bypass_cache;
    extern const SettingsBool read_from_page_cache_if_exists_otherwise_bypass_cache;
    extern const SettingsUInt64 page_cache_block_size;
    extern const SettingsUInt64 page_cache_lookahead_blocks;
    extern const SettingsInt64 read_priority;
    extern const SettingsString remote_filesystem_read_method;
    extern const SettingsBool remote_filesystem_read_prefetch;
    extern const SettingsUInt64 remote_fs_read_max_backoff_ms;
    extern const SettingsUInt64 remote_fs_read_backoff_max_tries;
    extern const SettingsUInt64 remote_read_min_bytes_for_seek;
    extern const SettingsBool throw_on_error_from_cache_on_write_operations;
    extern const SettingsBool filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit;
    extern const SettingsBool s3_allow_parallel_part_upload;
    extern const SettingsBool use_page_cache_for_disks_without_file_cache;
    extern const SettingsBool use_page_cache_with_distributed_cache;
    extern const SettingsUInt64 use_structure_from_insertion_table_in_table_functions;
    extern const SettingsString workload;
    extern const SettingsString compatibility;
    extern const SettingsBool allow_experimental_analyzer;
    extern const SettingsBool parallel_replicas_only_with_analyzer;
    extern const SettingsBool enable_hdfs_pread;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsString merge_workload;
    extern const MergeTreeSettingsString mutation_workload;
}

namespace ServerSetting
{
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
    extern const ServerSettingsBool display_secrets_in_show_and_select;
    extern const ServerSettingsUInt64 max_backup_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_build_vector_similarity_index_thread_pool_size;
    extern const ServerSettingsUInt64 max_local_read_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_local_write_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_merges_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_mutations_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_remote_read_network_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_remote_write_network_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_replicated_fetches_network_bandwidth_for_server;
    extern const ServerSettingsUInt64 max_replicated_sends_network_bandwidth_for_server;
    extern const ServerSettingsUInt64 tables_loader_background_pool_size;
    extern const ServerSettingsUInt64 tables_loader_foreground_pool_size;
    extern const ServerSettingsUInt64 prefetch_threadpool_pool_size;
    extern const ServerSettingsUInt64 prefetch_threadpool_queue_size;
    extern const ServerSettingsUInt64 load_marks_threadpool_pool_size;
    extern const ServerSettingsUInt64 load_marks_threadpool_queue_size;
    extern const ServerSettingsNonZeroUInt64 threadpool_writer_pool_size;
    extern const ServerSettingsUInt64 threadpool_writer_queue_size;
    extern const ServerSettingsUInt64 iceberg_catalog_threadpool_pool_size;
    extern const ServerSettingsUInt64 iceberg_catalog_threadpool_queue_size;
    extern const ServerSettingsBool dictionaries_lazy_load;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_DATABASE;
    extern const int UNKNOWN_TABLE;
    extern const int TABLE_ALREADY_EXISTS;
    extern const int THERE_IS_NO_SESSION;
    extern const int THERE_IS_NO_QUERY;
    extern const int NO_ELEMENTS_IN_CONFIG;
    extern const int TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT;
    extern const int LOGICAL_ERROR;
    extern const int INVALID_SETTING_VALUE;
    extern const int NOT_IMPLEMENTED;
    extern const int UNKNOWN_FUNCTION;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_COLUMNS_DOESNT_MATCH;
    extern const int CLUSTER_DOESNT_EXIST;
    extern const int SET_NON_GRANTED_ROLE;
    extern const int UNKNOWN_DISK;
    extern const int UNKNOWN_READ_METHOD;
}

#define SHUTDOWN(log, desc, ptr, method) do             \
{                                                       \
    if (ptr)                                            \
    {                                                   \
        LOG_DEBUG(log, "Shutting down " desc);          \
        (ptr)->method;                                  \
    }                                                   \
} while (false)                                         \

/** Set of known objects (environment), that could be used in query.
  * Shared (global) part. Order of members (especially, order of destruction) is very important.
  */
struct ContextSharedPart : boost::noncopyable
{
    LoggerPtr log = getLogger("Context");

    /// For access of most of shared objects.
    mutable ContextSharedMutex mutex;
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

    mutable zkutil::ZooKeeperPtr zookeeper TSA_GUARDED_BY(zookeeper_mutex);                 /// Client for ZooKeeper.
    ConfigurationPtr zookeeper_config TSA_GUARDED_BY(zookeeper_mutex);                      /// Stores zookeeper configs

    ConfigurationPtr sensitive_data_masker_config;

    mutable std::mutex auxiliary_zookeepers_mutex;
    mutable std::map<String, zkutil::ZooKeeperPtr> auxiliary_zookeepers TSA_GUARDED_BY(auxiliary_zookeepers_mutex);    /// Map for auxiliary ZooKeeper clients.
    ConfigurationPtr auxiliary_zookeepers_config TSA_GUARDED_BY(auxiliary_zookeepers_mutex);           /// Stores auxiliary zookeepers configs

    /// No lock required for interserver_io_host, interserver_io_port, interserver_scheme modified only during initialization
    String interserver_io_host;                             /// The host name by which this server is available for other servers.
    UInt16 interserver_io_port = 0;                         /// and port.
    String interserver_scheme;                              /// http or https
    MultiVersion<InterserverCredentials> interserver_io_credentials;

    String path TSA_GUARDED_BY(mutex);                       /// Path to the data directory, with a slash at the end.
    String flags_path TSA_GUARDED_BY(mutex);                 /// Path to the directory with some control flags for server maintenance.
    String user_files_path TSA_GUARDED_BY(mutex);            /// Path to the directory with user provided files, usable by 'file' table function.
    String dictionaries_lib_path TSA_GUARDED_BY(mutex);      /// Path to the directory with user provided binaries and libraries for external dictionaries.
    String user_scripts_path TSA_GUARDED_BY(mutex);          /// Path to the directory with user provided scripts.
    String filesystem_caches_path TSA_GUARDED_BY(mutex);     /// Path to the directory with filesystem caches.
    String filesystem_cache_user TSA_GUARDED_BY(mutex);
    ConfigurationPtr config TSA_GUARDED_BY(mutex);           /// Global configuration settings.
    String tmp_path TSA_GUARDED_BY(mutex);                   /// Path to the temporary files that occur when processing the request.

    /// The default disk storing metadata files for databases: database metadata files and table metadata files.
    /// For DBs which have `disk` setting in the create query, the table metadata files of these DBs are stored on that disk.
    /// However, the DB metadata files are still stored on this `default_db_disk`. So the instance can load its DBs during starting up.
    std::shared_ptr<IDisk> default_db_disk TSA_GUARDED_BY(mutex);

    /// All temporary files that occur when processing the requests accounted here.
    /// Child scopes for more fine-grained accounting are created per user/query/etc.
    /// Initialized once during server startup.
    TemporaryDataOnDiskScopePtr root_temp_data_on_disk TSA_GUARDED_BY(mutex);
    /// TODO: remove, use only root_temp_data_on_disk
    VolumePtr temporary_volume_legacy;

    mutable OnceFlag async_loader_initialized;
    mutable std::unique_ptr<AsyncLoader> async_loader; /// Thread pool for asynchronous initialization of arbitrary DAG of `LoadJob`s (used for tables loading)

    mutable std::unique_ptr<EmbeddedDictionaries> embedded_dictionaries TSA_GUARDED_BY(embedded_dictionaries_mutex);    /// Metrica's dictionaries. Have lazy initialization.
    mutable std::unique_ptr<ExternalDictionariesLoader> external_dictionaries_loader TSA_GUARDED_BY(external_dictionaries_mutex);

    ExternalLoaderXMLConfigRepository * external_dictionaries_config_repository TSA_GUARDED_BY(external_dictionaries_mutex) = nullptr;
    scope_guard dictionaries_xmls TSA_GUARDED_BY(external_dictionaries_mutex);

    mutable std::unique_ptr<ExternalUserDefinedExecutableFunctionsLoader> external_user_defined_executable_functions_loader TSA_GUARDED_BY(external_user_defined_executable_functions_mutex);
    ExternalLoaderXMLConfigRepository * user_defined_executable_functions_config_repository TSA_GUARDED_BY(external_user_defined_executable_functions_mutex) = nullptr;
    scope_guard user_defined_executable_functions_xmls TSA_GUARDED_BY(external_user_defined_executable_functions_mutex);

    mutable OnceFlag user_defined_sql_objects_storage_initialized;
    mutable std::unique_ptr<IUserDefinedSQLObjectsStorage> user_defined_sql_objects_storage;

    mutable OnceFlag workload_entity_storage_initialized;
    mutable std::unique_ptr<IWorkloadEntityStorage> workload_entity_storage;

#if USE_NLP
    mutable OnceFlag synonyms_extensions_initialized;
    mutable std::optional<SynonymsExtensions> synonyms_extensions;

    mutable OnceFlag lemmatizers_initialized;
    mutable std::optional<Lemmatizers> lemmatizers;
#endif

    mutable OnceFlag backups_worker_initialized;
    std::optional<BackupsWorker> backups_worker;

    /// No lock required for default_profile_name, system_profile_name, buffer_profile_name modified only during initialization
    String default_profile_name;                                /// Default profile name used for default values.
    String system_profile_name;                                 /// Profile used by system processes
    String buffer_profile_name;                                 /// Profile used by Buffer engine for flushing to the underlying
    String merge_workload TSA_GUARDED_BY(mutex);                /// Workload setting value that is used by all merges
    String mutation_workload TSA_GUARDED_BY(mutex);             /// Workload setting value that is used by all mutations
    bool throw_on_unknown_workload TSA_GUARDED_BY(mutex) = false;
    UInt64 concurrent_threads_soft_limit_num TSA_GUARDED_BY(mutex) = 0;
    UInt64 concurrent_threads_soft_limit_ratio_to_cores TSA_GUARDED_BY(mutex) = 0;
    String concurrent_threads_scheduler TSA_GUARDED_BY(mutex);
    std::unique_ptr<AccessControl> access_control TSA_GUARDED_BY(mutex);
    mutable OnceFlag resource_manager_initialized;
    mutable ResourceManagerPtr resource_manager;
    mutable UncompressedCachePtr uncompressed_cache TSA_GUARDED_BY(mutex);            /// The cache of decompressed blocks.
    mutable MarkCachePtr mark_cache TSA_GUARDED_BY(mutex);                            /// Cache of marks in compressed files.
    mutable PrimaryIndexCachePtr primary_index_cache TSA_GUARDED_BY(mutex);
    mutable OnceFlag load_marks_threadpool_initialized;
    mutable std::unique_ptr<ThreadPool> load_marks_threadpool;  /// Threadpool for loading marks cache.
    mutable OnceFlag prefetch_threadpool_initialized;
    mutable std::unique_ptr<ThreadPool> prefetch_threadpool;    /// Threadpool for loading marks cache.
    mutable std::unique_ptr<ThreadPool> iceberg_catalog_threadpool;
    mutable OnceFlag iceberg_catalog_threadpool_initialized;
    mutable OnceFlag build_vector_similarity_index_threadpool_initialized;
    mutable std::unique_ptr<ThreadPool> build_vector_similarity_index_threadpool; /// Threadpool for vector-similarity index creation.
    mutable UncompressedCachePtr index_uncompressed_cache TSA_GUARDED_BY(mutex);      /// The cache of decompressed blocks for MergeTree indices.
    mutable VectorSimilarityIndexCachePtr vector_similarity_index_cache TSA_GUARDED_BY(mutex);         /// Cache of deserialized secondary index granules.
    mutable QueryConditionCachePtr query_condition_cache TSA_GUARDED_BY(mutex);       /// Cache of matching marks for predicates
    mutable QueryResultCachePtr query_result_cache TSA_GUARDED_BY(mutex);             /// Cache of query results.
    mutable MarkCachePtr index_mark_cache TSA_GUARDED_BY(mutex);                      /// Cache of marks in compressed files of MergeTree indices.
    mutable MMappedFileCachePtr mmap_cache TSA_GUARDED_BY(mutex);                     /// Cache of mmapped files to avoid frequent open/map/unmap/close and to reuse from several threads.
#if USE_AVRO
    mutable IcebergMetadataFilesCachePtr iceberg_metadata_files_cache TSA_GUARDED_BY(mutex);   /// Cache of deserialized iceberg metadata files.
#endif
    AsynchronousMetrics * asynchronous_metrics TSA_GUARDED_BY(mutex) = nullptr;       /// Points to asynchronous metrics
    mutable PageCachePtr page_cache TSA_GUARDED_BY(mutex);                            /// Userspace page cache.
    ProcessList process_list;                                   /// Executing queries at the moment.
    SessionTracker session_tracker;
    GlobalOvercommitTracker global_overcommit_tracker;
    MergeList merge_list;                                       /// The list of executable merge (for (Replicated)?MergeTree)
    MovesList moves_list;                                       /// The list of executing moves (for (Replicated)?MergeTree)
    ReplicatedFetchList replicated_fetch_list;
    RefreshSet refresh_set;                                 /// The list of active refreshes (for MaterializedView)
    ConfigurationPtr users_config TSA_GUARDED_BY(mutex);                              /// Config with the users, profiles and quotas sections.
    InterserverIOHandler interserver_io_handler;                /// Handler for interserver communication.

    OnceFlag buffer_flush_schedule_pool_initialized;
    mutable BackgroundSchedulePoolPtr buffer_flush_schedule_pool; /// A thread pool that can do background flush for Buffer tables.
    OnceFlag schedule_pool_initialized;
    mutable BackgroundSchedulePoolPtr schedule_pool;    /// A thread pool that can run different jobs in background (used in replicated tables)
    OnceFlag distributed_schedule_pool_initialized;
    mutable BackgroundSchedulePoolPtr distributed_schedule_pool; /// A thread pool that can run different jobs in background (used for distributed sends)
    OnceFlag message_broker_schedule_pool_initialized;
    mutable BackgroundSchedulePoolPtr message_broker_schedule_pool; /// A thread pool that can run different jobs in background (used for message brokers, like RabbitMQ and Kafka)

    mutable OnceFlag readers_initialized;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_remote_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> asynchronous_local_fs_reader;
    mutable std::unique_ptr<IAsynchronousReader> synchronous_local_fs_reader;

    mutable OnceFlag threadpool_writer_initialized;
    mutable std::unique_ptr<ThreadPool> threadpool_writer;

#if USE_LIBURING
    mutable OnceFlag io_uring_reader_initialized;
    mutable std::unique_ptr<IOUringReader> io_uring_reader;
#endif

    mutable ThrottlerPtr replicated_fetches_throttler;      /// A server-wide throttler for replicated fetches
    mutable ThrottlerPtr replicated_sends_throttler;        /// A server-wide throttler for replicated sends

    mutable ThrottlerPtr remote_read_throttler;             /// A server-wide throttler for remote IO reads
    mutable ThrottlerPtr remote_write_throttler;            /// A server-wide throttler for remote IO writes

    mutable ThrottlerPtr local_read_throttler;              /// A server-wide throttler for local IO reads
    mutable ThrottlerPtr local_write_throttler;             /// A server-wide throttler for local IO writes

    mutable ThrottlerPtr backups_server_throttler;          /// A server-wide throttler for BACKUPs

    mutable ThrottlerPtr mutations_throttler;               /// A server-wide throttler for mutations
    mutable ThrottlerPtr merges_throttler;                  /// A server-wide throttler for merges

    MultiVersion<Macros> macros;                            /// Substitutions extracted from config.
    std::unique_ptr<DDLWorker> ddl_worker TSA_GUARDED_BY(mutex); /// Process ddl commands from zk.
    LoadTaskPtr ddl_worker_startup_task;                         /// To postpone `ddl_worker->startup()` after all tables startup
    /// Rules for selecting the compression settings, depending on the size of the part.
    mutable std::unique_ptr<CompressionCodecSelector> compression_codec_selector TSA_GUARDED_BY(mutex);
    /// Storage disk chooser for MergeTree engines
    mutable std::shared_ptr<const DiskSelector> merge_tree_disk_selector TSA_GUARDED_BY(storage_policies_mutex);
    /// Storage policy chooser for MergeTree engines
    mutable std::shared_ptr<const StoragePolicySelector> merge_tree_storage_policy_selector TSA_GUARDED_BY(storage_policies_mutex);

    ServerSettings server_settings;

    std::optional<MergeTreeSettings> merge_tree_settings TSA_GUARDED_BY(mutex);   /// Settings of MergeTree* engines.
    std::optional<MergeTreeSettings> replicated_merge_tree_settings TSA_GUARDED_BY(mutex);   /// Settings of ReplicatedMergeTree* engines.
    std::optional<DistributedSettings> distributed_settings TSA_GUARDED_BY(mutex);
    std::atomic_size_t max_table_size_to_drop = 50000000000lu; /// Protects MergeTree tables from accidental DROP (50GB by default)
    std::atomic_size_t max_partition_size_to_drop = 50000000000lu; /// Protects MergeTree partitions from accidental DROP (50GB by default)
    /// No lock required for format_schema_path modified only during initialization
    std::atomic_size_t max_database_num_to_warn = 1000lu;
    std::atomic_size_t max_table_num_to_warn = 5000lu;
    std::atomic_size_t max_view_num_to_warn = 10000lu;
    std::atomic_size_t max_dictionary_num_to_warn = 1000lu;
    std::atomic_size_t max_part_num_to_warn = 100000lu;
    // these variables are used in inserting warning message into system.warning table based on asynchronous metrics
    size_t max_pending_mutations_to_warn = 500lu;
    size_t max_pending_mutations_execution_time_to_warn = 86400lu;
    /// Only for system.server_settings, actually value stored in reloader itself
    std::atomic_size_t config_reload_interval_ms = ConfigReloader::DEFAULT_RELOAD_INTERVAL.count();

    double min_os_cpu_wait_time_ratio_to_drop_connection = 15.0;
    double max_os_cpu_wait_time_ratio_to_drop_connection = 30.0;

    String format_schema_path;                              /// Path to a directory that contains schema files used by input formats.
    String google_protos_path; /// Path to a directory that contains the proto files for the well-known Protobuf types.
    mutable OnceFlag action_locks_manager_initialized;
    ActionLocksManagerPtr action_locks_manager;             /// Set of storages' action lockers
    OnceFlag system_logs_initialized;
    std::unique_ptr<SystemLogs> system_logs TSA_GUARDED_BY(mutex);                /// Used to log queries and operations on parts

    mutable std::mutex dashboard_mutex;
    std::optional<Context::Dashboards> dashboards;

    std::optional<S3SettingsByEndpoint> storage_s3_settings TSA_GUARDED_BY(mutex);   /// Settings of S3 storage
    std::optional<AzureSettingsByEndpoint> storage_azure_settings TSA_GUARDED_BY(mutex);   /// Settings of AzureBlobStorage
    std::unordered_map<Context::WarningType, PreformattedMessage> warnings TSA_GUARDED_BY(mutex); /// Store warning messages about server.

    /// Background executors for *MergeTree tables
    /// Has background executors for MergeTree tables been initialized?
    mutable ContextSharedMutex background_executors_mutex;
    bool are_background_executors_initialized TSA_GUARDED_BY(background_executors_mutex) = false;
    MergeMutateBackgroundExecutorPtr merge_mutate_executor TSA_GUARDED_BY(background_executors_mutex);
    OrdinaryBackgroundExecutorPtr moves_executor TSA_GUARDED_BY(background_executors_mutex);
    OrdinaryBackgroundExecutorPtr fetch_executor TSA_GUARDED_BY(background_executors_mutex);
    OrdinaryBackgroundExecutorPtr common_executor TSA_GUARDED_BY(background_executors_mutex);

    RemoteHostFilter remote_host_filter;                    /// Allowed URL from config.xml
    HTTPHeaderFilter http_header_filter;                    /// Forbidden HTTP headers from config.xml

    /// No lock required for trace_collector modified only during initialization
    std::optional<TraceCollector> trace_collector;          /// Thread collecting traces from threads executing queries

    /// Clusters for distributed tables
    /// Initialized on demand (on distributed storages initialization) since Settings should be initialized
    mutable std::mutex clusters_mutex;                       /// Guards clusters, clusters_config and cluster_discovery
    std::shared_ptr<Clusters> clusters TSA_GUARDED_BY(clusters_mutex);
    ConfigurationPtr clusters_config TSA_GUARDED_BY(clusters_mutex);                        /// Stores updated configs
    std::unique_ptr<ClusterDiscovery> cluster_discovery TSA_GUARDED_BY(clusters_mutex);
    size_t clusters_version TSA_GUARDED_BY(clusters_mutex) = 0;

    /// No lock required for async_insert_queue modified only during initialization
    std::shared_ptr<AsynchronousInsertQueue> async_insert_queue;

    std::map<String, UInt16> server_ports;

    std::atomic<bool> shutdown_called = false;

    Stopwatch uptime_watch TSA_GUARDED_BY(mutex);

    /// No lock required for application_type modified only during initialization
    Context::ApplicationType application_type = Context::ApplicationType::SERVER;

    /// No lock required for config_reload_callback, start_servers_callback, stop_servers_callback modified only during initialization
    Context::ConfigReloadCallback config_reload_callback;
    Context::StartStopServersCallback start_servers_callback;
    Context::StartStopServersCallback stop_servers_callback;

    bool is_server_completely_started TSA_GUARDED_BY(mutex) = false;

#if USE_NURAFT
    mutable std::mutex keeper_dispatcher_mutex;
    mutable std::shared_ptr<KeeperDispatcher> keeper_dispatcher TSA_GUARDED_BY(keeper_dispatcher_mutex);
#endif

    ContextSharedPart()
        : access_control(std::make_unique<AccessControl>()), global_overcommit_tracker(&process_list), macros(std::make_unique<Macros>())
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
#if USE_NURAFT
        if (keeper_dispatcher)
        {
            try
            {
                keeper_dispatcher->shutdown();
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
#endif

        /// Wait for thread pool for background reads and writes,
        /// since it may use per-user MemoryTracker which will be destroyed here.
        if (asynchronous_remote_fs_reader)
        {
            try
            {
                LOG_DEBUG(log, "Destructing remote fs threadpool reader");
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
                LOG_DEBUG(log, "Destructing local fs threadpool reader");
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
                LOG_DEBUG(log, "Destructing local fs threadpool reader");
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
                LOG_DEBUG(log, "Destructing threadpool writer");
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
                LOG_DEBUG(log, "Destructing marks loader");
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
                LOG_DEBUG(log, "Destructing prefetch threadpool");
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

    void setConfig(const ConfigurationPtr & config_value)
    {
        if (!config_value)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Set nullptr config is invalid");

        std::lock_guard lock(mutex);
        config = config_value;
        access_control->setExternalAuthenticatorsConfig(*config_value);
    }

    const Poco::Util::AbstractConfiguration & getConfigRefWithLock(const std::lock_guard<ContextSharedMutex> &) const TSA_REQUIRES(this->mutex)
    {
        return config ? *config : Poco::Util::Application::instance().config();
    }

    const Poco::Util::AbstractConfiguration & getConfigRef() const
    {
        SharedLockGuard lock(mutex);
        return config ? *config : Poco::Util::Application::instance().config();
    }

    /** Perform a complex job of destroying objects in advance.
      */
    void shutdown() TSA_NO_THREAD_SAFETY_ANALYSIS
    {
        bool is_shutdown_called = shutdown_called.exchange(true);
        if (is_shutdown_called)
            return;

        /// Need to flush the async insert queue before shutting down the database catalog
        std::shared_ptr<AsynchronousInsertQueue> delete_async_insert_queue;
        {
            std::lock_guard lock(mutex);
            delete_async_insert_queue = std::move(async_insert_queue);
        }
        if (delete_async_insert_queue)
            delete_async_insert_queue->flushAndShutdown();

        /// Stop periodic reloading of the configuration files.
        /// This must be done first because otherwise the reloading may pass a changed config
        /// to some destroyed parts of ContextSharedPart.

        SHUTDOWN(log, "dictionaries loader", external_dictionaries_loader, enablePeriodicUpdates(false));
        SHUTDOWN(log, "UDFs loader", external_user_defined_executable_functions_loader, enablePeriodicUpdates(false));
        SHUTDOWN(log, "another UDFs storage", user_defined_sql_objects_storage, stopWatching());
        SHUTDOWN(log, "workload entity storage", workload_entity_storage, stopWatching());

        LOG_TRACE(log, "Shutting down named sessions");
        Session::shutdownNamedSessions();

        /// Waiting for current backups/restores to be finished. This must be done before `DatabaseCatalog::shutdown()`.
        SHUTDOWN(log, "backups worker", backups_worker, shutdown());

        /**  After system_logs have been shut down it is guaranteed that no system table gets created or written to.
          *  Note that part changes at shutdown won't be logged to part log.
          */
        SHUTDOWN(log, "system logs", system_logs, flushAndShutdown());

        LOG_TRACE(log, "Shutting down database catalog");
        DatabaseCatalog::shutdown();

        NamedCollectionFactory::instance().shutdown();

        delete_async_insert_queue.reset();

        SHUTDOWN(log, "merges executor", merge_mutate_executor, wait());
        SHUTDOWN(log, "fetches executor", fetch_executor, wait());
        SHUTDOWN(log, "moves executor", moves_executor, wait());
        SHUTDOWN(log, "common executor", common_executor, wait());

        TransactionLog::shutdownIfAny();

        std::unique_ptr<SystemLogs> delete_system_logs;
        std::unique_ptr<EmbeddedDictionaries> delete_embedded_dictionaries;
        std::unique_ptr<ExternalDictionariesLoader> delete_external_dictionaries_loader;
        std::unique_ptr<ExternalUserDefinedExecutableFunctionsLoader> delete_external_user_defined_executable_functions_loader;
        std::unique_ptr<IUserDefinedSQLObjectsStorage> delete_user_defined_sql_objects_storage;
        std::unique_ptr<IWorkloadEntityStorage> delete_workload_entity_storage;
        BackgroundSchedulePoolPtr delete_buffer_flush_schedule_pool;
        BackgroundSchedulePoolPtr delete_schedule_pool;
        BackgroundSchedulePoolPtr delete_distributed_schedule_pool;
        BackgroundSchedulePoolPtr delete_message_broker_schedule_pool;
        std::unique_ptr<DDLWorker> delete_ddl_worker;
        std::unique_ptr<AccessControl> delete_access_control;

        /// Delete DDLWorker before zookeeper.
        /// Cause it can call Context::getZooKeeper and resurrect it.

        {
            std::lock_guard lock(mutex);
            delete_ddl_worker = std::move(ddl_worker);
        }

        /// DDLWorker should be deleted without lock, cause its internal thread can
        /// take it as well, which will cause deadlock.
        LOG_TRACE(log, "Shutting down DDLWorker");
        delete_ddl_worker.reset();

        /// Background operations in cache use background schedule pool.
        /// Deactivate them before destructing it.
        LOG_TRACE(log, "Shutting down caches");
        const auto & caches = FileCacheFactory::instance().getAll();
        for (const auto & [_, cache] : caches)
            cache->cache->deactivateBackgroundOperations();
        FileCacheFactory::instance().clear();

        {
            std::lock_guard lock(clusters_mutex);
            if (cluster_discovery)
            {
                LOG_TRACE(log, "Shutting down ClusterDiscovery");
                /// Reset cluster_discovery if any.
                /// Some classes (such as ZooKeeper, ReplicatedAccessStorage) will finalize the keeper session while deconstructing,
                /// which will trigger the callback and make ClusterDiscovery reconnect to keeper again (unnecessary).
                cluster_discovery.reset();
            }
        }

        {
            // Disk selector might not be initialized if there was some error during
            // its initialization. Don't try to initialize it again on shutdown.
            if (merge_tree_disk_selector)
            {
                for (const auto & [disk_name, disk] : merge_tree_disk_selector->getDisksMap())
                {
                    LOG_INFO(log, "Shutdown disk {}", disk_name);
                    disk->shutdown();
                }
            }

            /// Special volumes might also use disks that require shutdown.
            if (temporary_volume_legacy)
            {
                auto & disks = temporary_volume_legacy->getDisks();
                for (auto & disk : disks)
                    disk->shutdown();
            }
        }

        LOG_TRACE(log, "Shutting down AccessControl");
        access_control->shutdown();

        {
            std::lock_guard lock(mutex);

            /** Compiled expressions stored in cache need to be destroyed before destruction of static objects.
              * Because CHJIT instance can be static object.
              */
#if USE_EMBEDDED_COMPILER
            if (auto * cache = CompiledExpressionCacheFactory::instance().tryGetCache())
                cache->clear();
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

            delete_system_logs = std::move(system_logs);
            delete_embedded_dictionaries = std::move(embedded_dictionaries);
            delete_external_dictionaries_loader = std::move(external_dictionaries_loader);
            delete_external_user_defined_executable_functions_loader = std::move(external_user_defined_executable_functions_loader);
            delete_user_defined_sql_objects_storage = std::move(user_defined_sql_objects_storage);
            delete_workload_entity_storage = std::move(workload_entity_storage);
            delete_buffer_flush_schedule_pool = std::move(buffer_flush_schedule_pool);
            delete_schedule_pool = std::move(schedule_pool);
            delete_distributed_schedule_pool = std::move(distributed_schedule_pool);
            delete_message_broker_schedule_pool = std::move(message_broker_schedule_pool);
            delete_access_control = std::move(access_control);

            /// Stop trace collector if any
            trace_collector.reset();
            /// Stop zookeeper connection
            zookeeper.reset();
        }

        /// Can be removed without context lock
        delete_system_logs.reset();
        delete_embedded_dictionaries.reset();
        delete_external_dictionaries_loader.reset();
        delete_external_user_defined_executable_functions_loader.reset();
        delete_user_defined_sql_objects_storage.reset();
        delete_workload_entity_storage.reset();
        delete_ddl_worker.reset();
        delete_buffer_flush_schedule_pool.reset();
        delete_schedule_pool.reset();
        delete_distributed_schedule_pool.reset();
        delete_message_broker_schedule_pool.reset();
        delete_access_control.reset();

        total_memory_tracker.resetOvercommitTracker();
        total_memory_tracker.resetPageCache();
    }

    bool hasTraceCollector() const
    {
        return trace_collector.has_value();
    }

    void initializeTraceCollector(std::shared_ptr<TraceLog> trace_log)
    {
        if (!trace_collector.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "TraceCollector needs to be first created before initialization");

        trace_collector->initialize(trace_log);
    }

    void createTraceCollector()
    {
        if (hasTraceCollector())
            return;

        trace_collector.emplace();
    }

    void addOrUpdateWarningMessage(Context::WarningType warning, const PreformattedMessage & message) TSA_REQUIRES(mutex)
    {
        /// A warning goes both: into server's log; stored to be placed in `system.warnings` table.
        LOG_WARNING(log, "{}", message.text);
        warnings[warning] = message;
    }

    void removeAllWarnings() TSA_REQUIRES(mutex)
    {
        warnings.clear();
    }

    void removeWarningMessage(Context::WarningType warning) TSA_REQUIRES(mutex)
    {
        if (warnings.contains(warning))
        {
            /// While removing the warning, log it with INFO level before it's removed from the `system.warnings` table.
            LOG_INFO(log, "Removing warning {}", warnings[warning].text);
            warnings.erase(warning);
        }
    }

    void configureServerWideThrottling()
    {
        if (auto bandwidth = server_settings[ServerSetting::max_replicated_fetches_network_bandwidth_for_server])
            replicated_fetches_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_replicated_sends_network_bandwidth_for_server])
            replicated_sends_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_remote_read_network_bandwidth_for_server])
            remote_read_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_remote_write_network_bandwidth_for_server])
            remote_write_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_local_read_bandwidth_for_server])
            local_read_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_local_write_bandwidth_for_server])
            local_write_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_backup_bandwidth_for_server])
            backups_server_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_mutations_bandwidth_for_server])
            mutations_throttler = std::make_shared<Throttler>(bandwidth);

        if (auto bandwidth = server_settings[ServerSetting::max_merges_bandwidth_for_server])
            merges_throttler = std::make_shared<Throttler>(bandwidth);
    }
};

void ContextSharedMutex::lockImpl()
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    Stopwatch watch;
    Base::lockImpl();
    ProfileEvents::increment(ProfileEvents::ContextLockWaitMicroseconds, watch.elapsedMicroseconds());
}

void ContextSharedMutex::lockSharedImpl()
{
    ProfileEvents::increment(ProfileEvents::ContextLock);
    CurrentMetrics::Increment increment{CurrentMetrics::ContextLockWait};
    Stopwatch watch;
    Base::lockSharedImpl();
    ProfileEvents::increment(ProfileEvents::ContextLockWaitMicroseconds, watch.elapsedMicroseconds());
}

ContextData::ContextData()
{
    settings = std::make_unique<Settings>();
}

ContextData::ContextData(const ContextData &o) :
    shared(o.shared),
    client_info(o.client_info),
    external_tables_initializer_callback(o.external_tables_initializer_callback),
    input_initializer_callback(o.input_initializer_callback),
    input_blocks_reader(o.input_blocks_reader),
    user_id(o.user_id),
    current_roles(o.current_roles),
    settings_constraints_and_current_profiles(o.settings_constraints_and_current_profiles),
    access(o.access),
    need_recalculate_access(o.need_recalculate_access),
    current_database(o.current_database),
    settings(std::make_unique<Settings>(*o.settings)),
    progress_callback(o.progress_callback),
    file_progress_callback(o.file_progress_callback),
    process_list_elem(o.process_list_elem),
    has_process_list_elem(o.has_process_list_elem),
    insertion_table_info(o.insertion_table_info),
    is_distributed(o.is_distributed),
    default_format(o.default_format),
    insert_format(o.insert_format),
    external_tables_mapping(o.external_tables_mapping),
    scalars(o.scalars),
    special_scalars(o.special_scalars),
    next_task_callback(o.next_task_callback),
    merge_tree_read_task_callback(o.merge_tree_read_task_callback),
    merge_tree_all_ranges_callback(o.merge_tree_all_ranges_callback),
    parallel_replicas_group_uuid(o.parallel_replicas_group_uuid),
    block_marshalling_callback(o.block_marshalling_callback),
    is_under_restore(o.is_under_restore),
    client_protocol_version(o.client_protocol_version),
    partition_id_to_max_block(o.partition_id_to_max_block),
    query_access_info(std::make_shared<QueryAccessInfo>(*o.query_access_info)),
    query_factories_info(o.query_factories_info),
    query_privileges_info(o.query_privileges_info),
    async_read_counters(o.async_read_counters),
    view_source(o.view_source),
    table_function_results(o.table_function_results),
    query_context(o.query_context),
    session_context(o.session_context),
    global_context(o.global_context),
    buffer_context(o.buffer_context),
    is_internal_query(o.is_internal_query),
    temp_data_on_disk(o.temp_data_on_disk),
    classifier(o.classifier),
    prepared_sets_cache(o.prepared_sets_cache),
    offset_parallel_replicas_enabled(o.offset_parallel_replicas_enabled),
    kitchen_sink(o.kitchen_sink),
    part_uuids(o.part_uuids),
    ignored_part_uuids(o.ignored_part_uuids),
    query_parameters(o.query_parameters),
    host_context(o.host_context),
    metadata_transaction(o.metadata_transaction),
    merge_tree_transaction(o.merge_tree_transaction),
    merge_tree_transaction_holder(o.merge_tree_transaction_holder),
    remote_read_query_throttler(o.remote_read_query_throttler),
    remote_write_query_throttler(o.remote_write_query_throttler),
    local_read_query_throttler(o.local_read_query_throttler),
    local_write_query_throttler(o.local_write_query_throttler),
    backups_query_throttler(o.backups_query_throttler)
{
}

void ContextData::resetSharedContext()
{
    std::lock_guard<std::mutex> lock(mutex_shared_context);
    shared = nullptr;
}

Context::Context() = default;
Context::Context(const Context & rhs) : ContextData(rhs), std::enable_shared_from_this<Context>(rhs) {}

SharedContextHolder::SharedContextHolder(SharedContextHolder &&) noexcept = default;
SharedContextHolder & SharedContextHolder::operator=(SharedContextHolder &&) noexcept = default;
SharedContextHolder::SharedContextHolder() = default;
SharedContextHolder::~SharedContextHolder() = default;
SharedContextHolder::SharedContextHolder(std::unique_ptr<ContextSharedPart> shared_context)
    : shared(std::move(shared_context)) {}

void SharedContextHolder::reset() { shared.reset(); }

ContextMutablePtr Context::createGlobal(ContextSharedPart * shared_part)
{
    auto res = std::shared_ptr<Context>(new Context);
    res->shared = shared_part;
    res->query_access_info = std::make_shared<QueryAccessInfo>();
    res->query_privileges_info = std::make_shared<QueryPrivilegesInfo>();
    return res;
}

SharedContextHolder Context::createShared()
{
    return SharedContextHolder(std::make_unique<ContextSharedPart>());
}

ContextMutablePtr Context::createCopy(const ContextPtr & other)
{
    SharedLockGuard lock(other->mutex);
    auto new_context = std::shared_ptr<Context>(new Context(*other));
    return new_context;
}

ContextMutablePtr Context::createCopy(const ContextWeakPtr & other)
{
    auto ptr = other.lock();
    if (!ptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't copy an expired context");
    return createCopy(ptr);
}

ContextMutablePtr Context::createCopy(const ContextMutablePtr & other)
{
    return createCopy(std::const_pointer_cast<const Context>(other));
}

Context::~Context() = default;

InterserverIOHandler & Context::getInterserverIOHandler() { return shared->interserver_io_handler; }
const InterserverIOHandler & Context::getInterserverIOHandler() const { return shared->interserver_io_handler; }

ProcessList & Context::getProcessList() { return shared->process_list; }
const ProcessList & Context::getProcessList() const { return shared->process_list; }
OvercommitTracker * Context::getGlobalOvercommitTracker() const { return &shared->global_overcommit_tracker; }

SessionTracker & Context::getSessionTracker() { return shared->session_tracker; }

MergeList & Context::getMergeList() { return shared->merge_list; }
const MergeList & Context::getMergeList() const { return shared->merge_list; }
MovesList & Context::getMovesList() { return shared->moves_list; }
const MovesList & Context::getMovesList() const { return shared->moves_list; }
ReplicatedFetchList & Context::getReplicatedFetchList() { return shared->replicated_fetch_list; }
const ReplicatedFetchList & Context::getReplicatedFetchList() const { return shared->replicated_fetch_list; }
RefreshSet & Context::getRefreshSet() { return shared->refresh_set; }
const RefreshSet & Context::getRefreshSet() const { return shared->refresh_set; }

String Context::resolveDatabase(const String & database_name) const
{
    String res = database_name.empty() ? getCurrentDatabase() : database_name;
    if (res.empty())
        throw Exception(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected");
    return res;
}

String Context::getPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->path;
}

String Context::getFlagsPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->flags_path;
}

String Context::getUserFilesPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->user_files_path;
}

String Context::getDictionariesLibPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->dictionaries_lib_path;
}

String Context::getUserScriptsPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->user_scripts_path;
}

String Context::getFilesystemCachesPath() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->filesystem_caches_path;
}

std::shared_ptr<IDisk> Context::getDatabaseDisk() const
{
    {
        SharedLockGuard lock(shared->mutex);
        if (shared->default_db_disk)
            return shared->default_db_disk;
    }

    // This is called first time early during the initialization.
    // Even if multiple threads try to get target_db_disk, only the first one will initialize the disks as there is another mutex in `getDiskMap()`
    // It is not necessary to introduce a mutex here.
    auto target_db_disk = [&]() -> std::shared_ptr<IDisk>
    {
        const auto & config = shared->getConfigRef();
        const auto & disk_map = getDisksMap();
        auto disk_name = config.getString("database_disk.disk", DiskSelector::DEFAULT_DISK_NAME);

        LOG_INFO(shared->log, "Database disk name: {}", disk_name);

        auto it = disk_map.find(disk_name);
        if (it == disk_map.end())
            throw Exception(ErrorCodes::UNKNOWN_DISK, "No disk {}", backQuote(disk_name));

        chassert(it->second);

        LOG_INFO(shared->log, "Database disk name: {}, path: {}", disk_name, it->second->getPath());
        return it->second;
    }();

    std::lock_guard lock(shared->mutex);
    if (shared->default_db_disk)
        return shared->default_db_disk;

    return shared->default_db_disk = target_db_disk;
}

String Context::getFilesystemCacheUser() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->filesystem_cache_user;
}

std::unordered_map<Context::WarningType, PreformattedMessage> Context::getWarnings() const
{
    std::unordered_map<Context::WarningType, PreformattedMessage> common_warnings;
    {
        SharedLockGuard lock(shared->mutex);
        common_warnings = shared->warnings;
        if (CurrentMetrics::get(CurrentMetrics::AttachedTable) > static_cast<Int64>(shared->max_table_num_to_warn))
            common_warnings[Context::WarningType::MAX_ATTACHED_TABLES] = PreformattedMessage::create("The number of attached tables is more than {}.", shared->max_table_num_to_warn.load());
        if (CurrentMetrics::get(CurrentMetrics::AttachedView) > static_cast<Int64>(shared->max_view_num_to_warn))
            common_warnings[Context::WarningType::MAX_ATTACHED_VIEWS] =  PreformattedMessage::create("The number of attached views is more than {}.", shared->max_view_num_to_warn.load());
        if (CurrentMetrics::get(CurrentMetrics::AttachedDictionary) > static_cast<Int64>(shared->max_dictionary_num_to_warn))
            common_warnings[Context::WarningType::MAX_ATTACHED_DICTIONARIES] =  PreformattedMessage::create("The number of attached dictionaries is more than {}.", shared->max_dictionary_num_to_warn.load());
        if (CurrentMetrics::get(CurrentMetrics::AttachedDatabase) > static_cast<Int64>(shared->max_database_num_to_warn))
            common_warnings[Context::WarningType::MAX_ATTACHED_DATABASES] = PreformattedMessage::create("The number of attached databases is more than {}.", shared->max_database_num_to_warn.load());
        if (CurrentMetrics::get(CurrentMetrics::PartsActive) > static_cast<Int64>(shared->max_part_num_to_warn))
            common_warnings[Context::WarningType::MAX_ACTIVE_PARTS] = PreformattedMessage::create("The number of active parts is more than {}.", shared->max_part_num_to_warn.load());
    }
    /// Make setting's name ordered
    auto obsolete_settings = settings->getChangedAndObsoleteNames();

    if (!obsolete_settings.empty())
    {
        bool single_element = obsolete_settings.size() == 1;
        constexpr auto message_format_string
            = "Obsolete setting{} [{}]{} changed. Please check 'SELECT * FROM system.settings WHERE changed AND is_obsolete' and read the "
              "changelog at https://github.com/ClickHouse/ClickHouse/blob/master/CHANGELOG.md";
        String settings_list = fmt::format("'{}'", fmt::join(obsolete_settings, "', '"));
        common_warnings[Context::WarningType::OBSOLETE_SETTINGS]
            = PreformattedMessage::create(message_format_string, single_element ? "" : "s", settings_list, single_element ? " is" : " are");
    }

    return common_warnings;
}

/// TODO: remove, use `getTempDataOnDisk`
VolumePtr Context::getGlobalTemporaryVolume() const
{
    SharedLockGuard lock(shared->mutex);
    /// Calling this method we just bypass the `temp_data_on_disk` and write to the file on the volume directly.
    /// Volume is the same for `root_temp_data_on_disk` (always set) and `temp_data_on_disk` (if it's set).
    if (shared->temporary_volume_legacy)
        return shared->temporary_volume_legacy;
    return nullptr;
}

TemporaryDataOnDiskScopePtr Context::getTempDataOnDisk() const
{
    if (temp_data_on_disk)
        return temp_data_on_disk;

    SharedLockGuard lock(shared->mutex);
    return shared->root_temp_data_on_disk;
}

TemporaryDataOnDiskScopePtr Context::getSharedTempDataOnDisk() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->root_temp_data_on_disk;
}

void Context::setTempDataOnDisk(TemporaryDataOnDiskScopePtr temp_data_on_disk_)
{
    /// It's set from `ProcessList::insert` in `executeQueryImpl` before query execution
    /// so no races with `getTempDataOnDisk` which is called from query execution.
    this->temp_data_on_disk = std::move(temp_data_on_disk_);
}

void Context::setPath(const String & path)
{
    std::lock_guard lock(shared->mutex);

    shared->path = path;

    if (shared->tmp_path.empty() && !shared->root_temp_data_on_disk)
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

void Context::setFilesystemCachesPath(const String & path)
{
    std::lock_guard lock(shared->mutex);

    if (getApplicationType() != ApplicationType::LOCAL && !fs::path(path).is_absolute())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Filesystem caches path must be absolute: {}", path);

    shared->filesystem_caches_path = path;
}

void Context::setFilesystemCacheUser(const String & user)
{
    std::lock_guard lock(shared->mutex);
    shared->filesystem_cache_user = user;
}

static void setupTmpPath(LoggerPtr log, const std::string & path)
try
{
    LOG_DEBUG(log, "Setting up {} to store temporary data in it", path);

    if (fs::exists(path))
    {
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
    else
    {
        fs::create_directories(path);
    }
}
catch (...)
{
    DB::tryLogCurrentException(log, fmt::format(
        "Caught exception while setting up temporary path: {}. "
        "It is ok to skip this exception as cleaning old temporary files is not necessary", path));
}

static VolumePtr createLocalSingleDiskVolume(const std::string & path, const Poco::Util::AbstractConfiguration & config_)
{
    auto disk = std::make_shared<DiskLocal>("_tmp_default", path, 0, config_, "storage_configuration.disks._tmp_default");
    VolumePtr volume = std::make_shared<SingleDiskVolume>("_tmp_default", disk, 0);
    return volume;
}

void Context::setTemporaryStoragePath(const String & path, size_t max_size)
{
    std::lock_guard lock(shared->mutex);

    if (shared->root_temp_data_on_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary storage is already set");

    shared->tmp_path = path;
    if (!shared->tmp_path.ends_with('/'))
        shared->tmp_path += '/';

    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path, shared->getConfigRefWithLock(lock));

    for (const auto & disk : volume->getDisks())
        setupTmpPath(shared->log, disk->getPath());

    TemporaryDataOnDiskSettings temporary_data_on_disk_settings;
    temporary_data_on_disk_settings.max_size_on_disk = max_size;
    shared->root_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, std::move(temporary_data_on_disk_settings));
    shared->temporary_volume_legacy = volume;
}

void Context::setTemporaryStoragePolicy(const String & policy_name, size_t max_size)
{
    StoragePolicyPtr tmp_policy;
    {
        /// lock in required only for accessing `shared->merge_tree_storage_policy_selector`
        /// StoragePolicy itself is immutable.
        std::lock_guard storage_policies_lock(shared->storage_policies_mutex);
        tmp_policy = getStoragePolicySelector(storage_policies_lock)->get(policy_name);
    }

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

    std::lock_guard lock(shared->mutex);

    if (shared->root_temp_data_on_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary storage is already set");

    TemporaryDataOnDiskSettings temporary_data_on_disk_settings;
    temporary_data_on_disk_settings.max_size_on_disk = max_size;
    shared->root_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(volume, std::move(temporary_data_on_disk_settings));
    shared->temporary_volume_legacy = volume;
}

void Context::setTemporaryStorageInCache(const String & cache_disk_name, size_t max_size)
{
    auto disk_ptr = getDisk(cache_disk_name);
    if (!disk_ptr)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Disk '{}' is not found", cache_disk_name);

    std::lock_guard lock(shared->mutex);
    if (shared->root_temp_data_on_disk)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Temporary storage is already set");

    auto file_cache = FileCacheFactory::instance().getByName(disk_ptr->getCacheName())->cache;
    if (!file_cache)
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "Cache '{}' is not found", disk_ptr->getCacheName());

    LOG_DEBUG(shared->log, "Using file cache ({}) for temporary files", file_cache->getBasePath());

    shared->tmp_path = file_cache->getBasePath();
    VolumePtr volume = createLocalSingleDiskVolume(shared->tmp_path, shared->getConfigRefWithLock(lock));

    TemporaryDataOnDiskSettings temporary_data_on_disk_settings;
    temporary_data_on_disk_settings.max_size_on_disk = max_size;
    shared->root_temp_data_on_disk = std::make_shared<TemporaryDataOnDiskScope>(file_cache.get(), std::move(temporary_data_on_disk_settings));
    shared->temporary_volume_legacy = volume;
}

void Context::setFlagsPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->flags_path = path;
}

void Context::setUserFilesPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->user_files_path = path;
}

void Context::setDictionariesLibPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->dictionaries_lib_path = path;
}

void Context::setUserScriptsPath(const String & path)
{
    std::lock_guard lock(shared->mutex);
    shared->user_scripts_path = path;
}

void Context::addOrUpdateWarningMessage(WarningType warning, const PreformattedMessage & message) const
{
    std::lock_guard lock(shared->mutex);
    auto suppress_re = shared->getConfigRefWithLock(lock).getString("warning_supress_regexp", "");

    bool is_supressed = !suppress_re.empty() && re2::RE2::PartialMatch(message.text, suppress_re);
    if (!is_supressed)
        shared->addOrUpdateWarningMessage(warning, message);
}

void Context::addWarningMessageAboutDatabaseOrdinary(const String & database_name) const
{
    std::lock_guard lock(shared->mutex);

    /// We would like to report only about the first database with engine Ordinary
    static std::atomic_bool is_called = false;
    if (is_called.exchange(true))
        return;

    /// We don't use getFlagsPath method, because it takes a shared lock.
    auto convert_databases_flag = fs::path(shared->flags_path) / "convert_ordinary_to_atomic";
    constexpr auto message_format_string
        = "Server has databases (for example `{}`) with Ordinary engine, which was deprecated. "
          "To convert this database to the new Atomic engine, create a flag {} and make sure that ClickHouse has write permission for it. "
          "Example: sudo touch '{}' && sudo chmod 666 '{}'";
    shared->addOrUpdateWarningMessage(
        Context::WarningType::DB_ORDINARY_DEPRECATED,
        PreformattedMessage::create(
            message_format_string,
            database_name,
            convert_databases_flag.string(),
            convert_databases_flag.string(),
            convert_databases_flag.string()));
}

void Context::removeWarningMessage(WarningType warning) const
{
    std::lock_guard lock(shared->mutex);
    shared->removeWarningMessage(warning);
}

void Context::removeAllWarnings() const
{
    std::lock_guard lock(shared->mutex);
    shared->removeAllWarnings();
}

void Context::setConfig(const ConfigurationPtr & config)
{
    shared->setConfig(config);
}

const Poco::Util::AbstractConfiguration & Context::getConfigRef() const
{
    return shared->getConfigRef();
}

AccessControl & Context::getAccessControl()
{
    SharedLockGuard lock(shared->mutex);
    return *shared->access_control;
}

const AccessControl & Context::getAccessControl() const
{
    SharedLockGuard lock(shared->mutex);
    return *shared->access_control;
}

void Context::setExternalAuthenticatorsConfig(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);
    shared->access_control->setExternalAuthenticatorsConfig(config);
}

std::unique_ptr<GSSAcceptorContext> Context::makeGSSAcceptorContext() const
{
    SharedLockGuard lock(shared->mutex);
    return std::make_unique<GSSAcceptorContext>(shared->access_control->getExternalAuthenticators().getKerberosParams());
}

void Context::setUsersConfig(const ConfigurationPtr & config)
{
    std::lock_guard lock(shared->mutex);
    shared->users_config = config;
    shared->access_control->setUsersConfig(*shared->users_config);
}

ConfigurationPtr Context::getUsersConfig()
{
    SharedLockGuard lock(shared->mutex);
    return shared->users_config;
}

void Context::setUser(const UUID & user_id_, const std::vector<UUID> & external_roles_)
{
    /// Prepare lists of user's profiles, constraints, settings, roles.
    /// NOTE: AccessControl::read<User>() and other AccessControl's functions may require some IO work,
    /// so Context::getLocalLock() and Context::getGlobalLock() must be unlocked while we're doing this.

    auto & access_control = getAccessControl();
    auto user = access_control.read<User>(user_id_);

    auto default_roles = user->granted_roles.findGranted(user->default_roles);
    auto enabled_roles = access_control.getEnabledRolesInfo(default_roles, {});
    auto enabled_profiles = access_control.getEnabledSettingsInfo(user_id_, user->settings, enabled_roles->enabled_roles, enabled_roles->settings_from_enabled_roles);
    const auto & database = user->default_database;

    /// Apply user's profiles, constraints, settings, roles.
    std::lock_guard lock(mutex);

    setUserIDWithLock(user_id_, lock);

    /// A profile can specify a value and a readonly constraint for same setting at the same time,
    /// so we shouldn't check constraints here.
    setCurrentProfilesWithLock(*enabled_profiles, /* check_constraints= */ false, lock);

    setCurrentRolesWithLock(default_roles, lock);
    setExternalRolesWithLock(external_roles_, lock);

    /// It's optional to specify the DEFAULT DATABASE in the user's definition.
    if (!database.empty())
        setCurrentDatabaseWithLock(database, lock);
}

std::shared_ptr<const User> Context::getUser() const
{
    return getAccess()->getUser();
}

String Context::getUserName() const
{
    return getAccess()->getUserName();
}

void Context::setUserIDWithLock(const UUID & user_id_, const std::lock_guard<ContextSharedMutex> &)
{
    user_id = user_id_;
    need_recalculate_access = true;
}

void Context::setUserID(const UUID & user_id_)
{
    std::lock_guard lock(mutex);
    setUserIDWithLock(user_id_, lock);
}

std::optional<UUID> Context::getUserID() const
{
    SharedLockGuard lock(mutex);
    return user_id;
}

void Context::setCurrentRolesWithLock(const std::vector<UUID> & new_current_roles, const std::lock_guard<ContextSharedMutex> &)
{
    if (new_current_roles.empty())
        current_roles = nullptr;
    else
        current_roles = std::make_shared<std::vector<UUID>>(new_current_roles);
    need_recalculate_access = true;
}

void Context::setExternalRolesWithLock(const std::vector<UUID> & new_external_roles, const std::lock_guard<ContextSharedMutex> &)
{
    // External roles are roles received from other node, current roles is a collection of roles that were assigned locally
    if (!new_external_roles.empty())
    {
        if (external_roles)
            external_roles->insert(external_roles->end(), new_external_roles.begin(), new_external_roles.end());
        else
            external_roles = std::make_shared<std::vector<UUID>>(new_external_roles);
        need_recalculate_access = true;
    }
}

void Context::setCurrentRolesImpl(const std::vector<UUID> & new_current_roles, bool throw_if_not_granted, bool skip_if_not_granted, const std::shared_ptr<const User> & user)
{
    if (skip_if_not_granted)
    {
        auto filtered_role_ids = user->granted_roles.findGranted(new_current_roles);
        std::lock_guard lock{mutex};
        setCurrentRolesWithLock(filtered_role_ids, lock);
        return;
    }
    if (throw_if_not_granted)
    {
        for (const auto & role_id : new_current_roles)
        {
            if (!user->granted_roles.isGranted(role_id))
            {
                auto role_name = getAccessControl().tryReadName(role_id);
                throw Exception(ErrorCodes::SET_NON_GRANTED_ROLE, "Role {} should be granted to set as a current", role_name.value_or(toString(role_id)));
            }
        }
    }
    std::lock_guard lock2{mutex};
    setCurrentRolesWithLock(new_current_roles, lock2);
}

void Context::setCurrentRoles(const std::vector<UUID> & new_current_roles, bool check_grants)
{
    setCurrentRolesImpl(new_current_roles, /* throw_if_not_granted= */ check_grants, /* skip_if_not_granted= */ !check_grants, getUser());
}

void Context::setCurrentRoles(const RolesOrUsersSet & new_current_roles, bool check_grants)
{
    if (new_current_roles.all)
    {
        auto user = getUser();
        setCurrentRolesImpl(user->granted_roles.findGranted(new_current_roles), /* throw_if_not_granted= */ false, /* skip_if_not_granted= */ false, user);
    }
    else
    {
        setCurrentRoles(new_current_roles.getMatchingIDs(), check_grants);
    }
}

void Context::setCurrentRoles(const Strings & new_current_roles, bool check_grants)
{
    setCurrentRoles(getAccessControl().getIDs<Role>(new_current_roles), check_grants);
}

void Context::setCurrentRolesDefault()
{
    auto user = getUser();
    setCurrentRolesImpl(user->granted_roles.findGranted(user->default_roles), /* throw_if_not_granted= */ false, /* skip_if_not_granted= */ false, user);
}

std::vector<UUID> Context::getCurrentRoles() const
{
    return getRolesInfo()->getCurrentRoles();
}

std::vector<UUID> Context::getEnabledRoles() const
{
    return getRolesInfo()->getEnabledRoles();
}

std::shared_ptr<const EnabledRolesInfo> Context::getRolesInfo() const
{
    return getAccess()->getRolesInfo();
}

namespace
{
ALWAYS_INLINE inline void
contextSanityClampSettingsWithLock(const Context & context, Settings & settings, const std::lock_guard<ContextSharedMutex> &)
{
    const auto type = context.getApplicationType();
    if (type == Context::ApplicationType::LOCAL || type == Context::ApplicationType::SERVER)
        doSettingsSanityCheckClamp(settings, getLogger("SettingsSanity"));
}

ALWAYS_INLINE inline void contextSanityClampSettings(const Context & context, Settings & settings)
{
    const auto type = context.getApplicationType();
    if (type == Context::ApplicationType::LOCAL || type == Context::ApplicationType::SERVER)
        doSettingsSanityCheckClamp(settings, getLogger("SettingsSanity"));
}
}

template <typename... Args>
void Context::checkAccessImpl(const Args &... args) const
{
    return getAccess()->checkAccess(args...);
}

void Context::checkAccess(const AccessFlags & flags) const { checkAccessImpl(flags); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database) const { checkAccessImpl(flags, database); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table) const { checkAccessImpl(flags, database, table); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, std::string_view column) const { checkAccessImpl(flags, database, table, column); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const std::vector<std::string_view> & columns) const { checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, std::string_view database, std::string_view table, const Strings & columns) const { checkAccessImpl(flags, database, table, columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName()); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, std::string_view column) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), column); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const std::vector<std::string_view> & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessFlags & flags, const StorageID & table_id, const Strings & columns) const { checkAccessImpl(flags, table_id.getDatabaseName(), table_id.getTableName(), columns); }
void Context::checkAccess(const AccessRightsElement & element) const { checkAccessImpl(element); }
void Context::checkAccess(const AccessRightsElements & elements) const { checkAccessImpl(elements); }

std::shared_ptr<const ContextAccessWrapper> Context::getAccess() const
{
    /// A helper function to collect parameters for calculating access rights, called with Context::getLocalSharedLock() acquired.
    auto get_params = [this]()
    {
        /// If setUserID() was never called then this must be the global context with the full access.
        bool full_access = !user_id;

        return ContextAccessParams{
            user_id, full_access, /* use_default_roles= */ false, current_roles, external_roles, *settings, current_database, client_info};
    };

    /// Check if the current access rights are still valid, otherwise get parameters for recalculating access rights.
    std::optional<ContextAccessParams> params;

    {
        SharedLockGuard lock(mutex);
        if (access && !need_recalculate_access)
            return std::make_shared<const ContextAccessWrapper>(access, shared_from_this()); /// No need to recalculate access rights.

        params.emplace(get_params());

        if (access && (access->getParams() == *params))
        {
            need_recalculate_access = false;
            return std::make_shared<const ContextAccessWrapper>(access, shared_from_this()); /// No need to recalculate access rights.
        }
    }

    /// Calculate new access rights according to the collected parameters.
    /// NOTE: AccessControl::getContextAccess() may require some IO work, so Context::getLocalLock()
    ///       and Context::getGlobalLock() must be unlocked while we're doing this.
    auto res = getAccessControl().getContextAccess(*params);

    {
        /// If the parameters of access rights were not changed while we were calculated them
        /// then we store the new access rights in the Context to allow reusing it later.
        std::lock_guard lock(mutex);
        if (get_params() == *params)
        {
            access = res;
            need_recalculate_access = false;
        }
    }

    return std::make_shared<const ContextAccessWrapper>(res, shared_from_this());
}

RowPolicyFilterPtr Context::getRowPolicyFilter(const String & database, const String & table_name, RowPolicyFilterType filter_type) const
{
    return getAccess()->getRowPolicyFilter(database, table_name, filter_type);
}


std::shared_ptr<const EnabledQuota> Context::getQuota() const
{
    return getAccess()->getQuota();
}


std::optional<QuotaUsage> Context::getQuotaUsage() const
{
    return getAccess()->getQuotaUsage();
}

void Context::setCurrentProfileWithLock(const String & profile_name, bool check_constraints, const std::lock_guard<ContextSharedMutex> & lock)
{
    try
    {
        UUID profile_id = getAccessControl().getID<SettingsProfile>(profile_name);
        setCurrentProfileWithLock(profile_id, check_constraints, lock);
    }
    catch (Exception & e)
    {
        e.addMessage(", while trying to set settings profile {}", profile_name);
        throw;
    }
}

void Context::setCurrentProfileWithLock(const UUID & profile_id, bool check_constraints, const std::lock_guard<ContextSharedMutex> & lock)
{
    auto profile_info = getAccessControl().getSettingsProfileInfo(profile_id);
    setCurrentProfilesWithLock(*profile_info, check_constraints, lock);
}

void Context::setCurrentProfilesWithLock(const SettingsProfilesInfo & profiles_info, bool check_constraints, const std::lock_guard<ContextSharedMutex> & lock)
{
    if (check_constraints)
        checkSettingsConstraintsWithLock(profiles_info.settings, SettingSource::PROFILE);
    applySettingsChangesWithLock(profiles_info.settings, lock);
    settings_constraints_and_current_profiles = profiles_info.getConstraintsAndProfileIDs(settings_constraints_and_current_profiles);
    contextSanityClampSettingsWithLock(*this, *settings, lock);
}

void Context::setCurrentProfile(const String & profile_name, bool check_constraints)
{
    std::lock_guard lock(mutex);
    setCurrentProfileWithLock(profile_name, check_constraints, lock);
}

void Context::setCurrentProfile(const UUID & profile_id, bool check_constraints)
{
    std::lock_guard lock(mutex);
    setCurrentProfileWithLock(profile_id, check_constraints, lock);
}

void Context::setCurrentProfiles(const SettingsProfilesInfo & profiles_info, bool check_constraints)
{
    std::lock_guard lock(mutex);
    setCurrentProfilesWithLock(profiles_info, check_constraints, lock);
}

std::vector<UUID> Context::getCurrentProfiles() const
{
    SharedLockGuard lock(mutex);
    if (!settings_constraints_and_current_profiles)
        return {};
    return settings_constraints_and_current_profiles->current_profiles;
}

std::vector<UUID> Context::getEnabledProfiles() const
{
    SharedLockGuard lock(mutex);
    if (!settings_constraints_and_current_profiles)
        return {};
    return settings_constraints_and_current_profiles->enabled_profiles;
}


ResourceManagerPtr Context::getResourceManager() const
{
    callOnce(shared->resource_manager_initialized, [&] {
        shared->resource_manager = createResourceManager(getGlobalContext());
    });

    return shared->resource_manager;
}

ClassifierPtr Context::getWorkloadClassifier() const
{
    ClassifierSettings settings{.throw_on_unknown_workload = getThrowOnUnknownWorkload()}; // to avoid locking shared mutex under `mutex`
    std::lock_guard lock(mutex);
    // NOTE: Workload cannot be changed after query start, and getWorkloadClassifier() should not be called before proper `workload` is set
    if (!classifier)
        classifier = getResourceManager()->acquire(getSettingsRef()[Setting::workload], settings);
    return classifier;
}

String Context::getMergeWorkload() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->merge_workload;
}

void Context::setMergeWorkload(const String & value)
{
    std::lock_guard lock(shared->mutex);
    shared->merge_workload = value;
}

String Context::getMutationWorkload() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->mutation_workload;
}

void Context::setMutationWorkload(const String & value)
{
    std::lock_guard lock(shared->mutex);
    shared->mutation_workload = value;
}

bool Context::getThrowOnUnknownWorkload() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->throw_on_unknown_workload;
}

void Context::setThrowOnUnknownWorkload(bool value)
{
    std::lock_guard lock(shared->mutex);
    shared->throw_on_unknown_workload = value;
}

UInt64 Context::getConcurrentThreadsSoftLimitNum() const
{
    std::lock_guard lock(shared->mutex);
    return shared->concurrent_threads_soft_limit_num;
}

UInt64 Context::getConcurrentThreadsSoftLimitRatioToCores() const
{
    std::lock_guard lock(shared->mutex);
    return shared->concurrent_threads_soft_limit_ratio_to_cores;
}

String Context::getConcurrentThreadsScheduler() const
{
    std::lock_guard lock(shared->mutex);
    return shared->concurrent_threads_scheduler;
}

std::pair<UInt64, String> Context::setConcurrentThreadsSoftLimit(UInt64 num, UInt64 ratio_to_cores, const String & scheduler)
{
    std::lock_guard lock(shared->mutex);

    // Set the scheduler
    bool ok = ConcurrencyControl::instance().setScheduler(scheduler);
    if (ok)
        shared->concurrent_threads_scheduler = scheduler;
    else
        LOG_ERROR(shared->log, "Invalid value '{}' is set for the server setting 'concurrent_threads_scheduler'. Scheduler was not changed.", scheduler);

    // Set the limit
    SlotCount concurrent_threads_soft_limit = UnlimitedSlots;
    if (num > 0 && num < concurrent_threads_soft_limit)
        concurrent_threads_soft_limit = num;
    if (ratio_to_cores > 0)
    {
        auto value = ratio_to_cores * getNumberOfCPUCoresToUse();
        if (value > 0 && value < concurrent_threads_soft_limit)
            concurrent_threads_soft_limit = value;
    }
    ConcurrencyControl::instance().setMaxConcurrency(concurrent_threads_soft_limit);
    shared->concurrent_threads_soft_limit_num = num;
    shared->concurrent_threads_soft_limit_ratio_to_cores = ratio_to_cores;
    return { concurrent_threads_soft_limit, ConcurrencyControl::instance().getScheduler() };
}


Scalars Context::getScalars() const
{
    std::lock_guard lock(mutex);
    return scalars;
}


Block Context::getScalar(const String & name) const
{
    std::lock_guard lock(mutex);

    auto it = scalars.find(name);
    if (scalars.end() == it)
    {
        // This should be a logical error, but it fails the sql_fuzz test too
        // often, so 'bad arguments' for now.
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Scalar {} doesn't exist (internal bug)", backQuoteIfNeed(name));
    }
    return it->second;
}

std::optional<Block> Context::tryGetSpecialScalar(const String & name) const
{
    std::lock_guard lock(mutex);
    auto it = special_scalars.find(name);
    if (special_scalars.end() == it)
        return std::nullopt;
    return it->second;
}

Tables Context::getExternalTables() const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    SharedLockGuard lock(mutex);

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
    addExternalTable(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}

void Context::updateExternalTable(const String & table_name, TemporaryTableHolder && temporary_table)
{
    updateExternalTable(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}

void Context::addOrUpdateExternalTable(const String & table_name, TemporaryTableHolder && temporary_table)
{
    addOrUpdateExternalTable(table_name, std::make_shared<TemporaryTableHolder>(std::move(temporary_table)));
}

void Context::addExternalTable(const String & table_name, std::shared_ptr<TemporaryTableHolder> temporary_table)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::lock_guard lock(mutex);
    if (external_tables_mapping.end() != external_tables_mapping.find(table_name))
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Temporary table {} already exists", backQuoteIfNeed(table_name));

    external_tables_mapping.emplace(table_name, std::move(temporary_table));
}

void Context::updateExternalTable(const String & table_name, std::shared_ptr<TemporaryTableHolder> temporary_table)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::lock_guard lock(mutex);
    auto it = external_tables_mapping.find(table_name);
    if (it == external_tables_mapping.end())
        throw Exception(ErrorCodes::TABLE_ALREADY_EXISTS, "Temporary table {} does not exist", backQuoteIfNeed(table_name));

    it->second = std::move(temporary_table);
}

void Context::addOrUpdateExternalTable(const String & table_name, std::shared_ptr<TemporaryTableHolder> temporary_table)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::lock_guard lock(mutex);
    auto [it, inserted] = external_tables_mapping.emplace(table_name, temporary_table);
    if (!inserted)
        it->second = std::move(temporary_table);
}

std::shared_ptr<TemporaryTableHolder> Context::findExternalTable(const String & table_name) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::shared_ptr<TemporaryTableHolder> holder;
    {
        SharedLockGuard lock(mutex);
        auto iter = external_tables_mapping.find(table_name);
        if (iter == external_tables_mapping.end())
            return {};
        holder = iter->second;
    }
    return holder;
}

std::shared_ptr<TemporaryTableHolder> Context::removeExternalTable(const String & table_name)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have external tables");

    std::shared_ptr<TemporaryTableHolder> holder;
    {
        std::lock_guard lock(mutex);
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

    std::lock_guard lock(mutex);
    scalars[name] = block;
}


void Context::addSpecialScalar(const String & name, const Block & block)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have local scalars");

    std::lock_guard lock(mutex);
    special_scalars[name] = block;
}


bool Context::hasScalar(const String & name) const
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have scalars");

    std::lock_guard lock(mutex);
    return scalars.contains(name);
}

void Context::addQueryAccessInfo(
    const StorageID & table_id,
    const Names & column_names)
{
    addQueryAccessInfo(backQuoteIfNeed(table_id.getDatabaseName()), table_id.getFullTableName(), column_names);
}

void Context::addQueryAccessInfo(
    const String & quoted_database_name,
    const String & full_quoted_table_name,
    const Names & column_names)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query access info");

    std::lock_guard lock(query_access_info->mutex);
    query_access_info->databases.emplace(quoted_database_name);
    query_access_info->tables.emplace(full_quoted_table_name);

    for (const auto & column_name : column_names)
        query_access_info->columns.emplace(full_quoted_table_name + "." + backQuoteIfNeed(column_name));
}

void Context::addQueryAccessInfo(const Names & partition_names)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query access info");

    std::lock_guard<std::mutex> lock(query_access_info->mutex);
    for (const auto & partition_name : partition_names)
        query_access_info->partitions.emplace(partition_name);
}

void Context::addViewAccessInfo(const String & view_name)
{
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query access info");

    std::lock_guard<std::mutex> lock(query_access_info->mutex);
    query_access_info->views.emplace(view_name);
}

void Context::addQueryAccessInfo(const QualifiedProjectionName & qualified_projection_name)
{
    if (!qualified_projection_name)
        return;

    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have query access info");

    std::lock_guard<std::mutex> lock(query_access_info->mutex);
    query_access_info->projections.emplace(fmt::format(
        "{}.{}", qualified_projection_name.storage_id.getFullTableName(), backQuoteIfNeed(qualified_projection_name.projection_name)));
}

Context::QueryFactoriesInfo Context::getQueryFactoriesInfo() const
{
    return query_factories_info;
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
            break;
        case QueryLogFactories::ExecutableUserDefinedFunction:
            query_factories_info.executable_user_defined_functions.emplace(created_object);
            break;
        case QueryLogFactories::SQLUserDefinedFunction:
            query_factories_info.sql_user_defined_functions.emplace(created_object);
    }
}

void Context::addQueryPrivilegesInfo(const String & privilege, bool granted) const
{
    std::lock_guard lock(query_privileges_info->mutex);
    if (granted)
        query_privileges_info->used_privileges.emplace(privilege);
    else
        query_privileges_info->missing_privileges.emplace(privilege);
}

static bool findIdentifier(const ASTFunction * function)
{
    if (!function || !function->arguments)
        return false;
    if (const auto * arguments = function->arguments->as<ASTExpressionList>())
    {
        for (const auto & argument : arguments->children)
        {
            if (argument->as<ASTIdentifier>())
                return true;
            if (const auto * f = argument->as<ASTFunction>(); f && findIdentifier(f))
                return true;
        }
    }
    return false;
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
        if (table.get()->isView() && table->as<StorageView>() && table->as<StorageView>()->isParameterizedView())
        {
            auto query = table->getInMemoryMetadataPtr()->getSelectQuery().inner_query->clone();
            NameToNameMap parameterized_view_values = analyzeFunctionParamValues(table_expression, getQueryContext());
            StorageView::replaceQueryParametersIfParameterizedView(query, parameterized_view_values);

            ASTCreateQuery create;
            create.select = query->as<ASTSelectWithUnionQuery>();
            auto sample_block = InterpreterSelectWithUnionQuery::getSampleBlock(query, getQueryContext());
            auto res = std::make_shared<StorageView>(StorageID(database_name, table_name),
                                                     create,
                                                     ColumnsDescription(sample_block.getNamesAndTypesList()),
                                                     /* comment */ "",
                                                     /* is_parameterized_view */ true);
            res->startup();
            function->prefer_subquery_to_function_formatting = true;
            return res;
        }
    }
    auto hash = table_expression->getTreeHash(/*ignore_aliases=*/ true);
    auto key = toString(hash);
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

        uint64_t use_structure_from_insertion_table_in_table_functions
            = getSettingsRef()[Setting::use_structure_from_insertion_table_in_table_functions];
        if (select_query_hint && use_structure_from_insertion_table_in_table_functions && table_function_ptr->needStructureHint()
            && hasInsertionTable())
        {
            const auto & insert_columns = DatabaseCatalog::instance()
                                              .getTable(getInsertionTable(), shared_from_this())
                                              ->getInMemoryMetadataPtr()
                                              ->getColumns();

            const auto & insert_column_names = hasInsertionTableColumnNames() ? *getInsertionTableColumnNames() : insert_columns.getOrdinary().getNames();
            DB::ColumnsDescription structure_hint;

            bool use_columns_from_insert_query = true;

            /// Insert table matches columns against SELECT expression by position, so we want to map
            /// insert table columns to table function columns through names from SELECT expression.

            auto insert_column_name_it = insert_column_names.begin();
            auto insert_column_names_end = insert_column_names.end();  /// end iterator of the range covered by possible asterisk
            auto virtual_column_names = table_function_ptr->getVirtualsToCheckBeforeUsingStructureHint();
            bool asterisk = false;
            const auto & expression_list = select_query_hint->select()->as<ASTExpressionList>()->children;
            const auto * expression = expression_list.begin();

            /// We want to go through SELECT expression list and correspond each expression to column in insert table
            /// which type will be used as a hint for the file structure inference.
            for (; expression != expression_list.end() && insert_column_name_it != insert_column_names_end; ++expression)
            {
                if (auto * identifier = (*expression)->as<ASTIdentifier>())
                {
                    if (!virtual_column_names.contains(identifier->name()))
                    {
                        if (asterisk)
                        {
                            if (use_structure_from_insertion_table_in_table_functions == 1)
                                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Asterisk cannot be mixed with column list in INSERT SELECT query.");

                            use_columns_from_insert_query = false;
                            break;
                        }

                        ColumnDescription column = insert_columns.get(*insert_column_name_it);
                        column.name = identifier->name();
                        /// Change ephemeral columns to default columns.
                        column.default_desc.kind = ColumnDefaultKind::Default;
                        structure_hint.add(std::move(column));
                    }

                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
                else if ((*expression)->as<ASTAsterisk>())
                {
                    if (asterisk)
                    {
                        if (use_structure_from_insertion_table_in_table_functions == 1)
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Only one asterisk can be used in INSERT SELECT query.");

                        use_columns_from_insert_query = false;
                        break;
                    }
                    if (!structure_hint.empty())
                    {
                        if (use_structure_from_insertion_table_in_table_functions == 1)
                            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Asterisk cannot be mixed with column list in INSERT SELECT query.");

                        use_columns_from_insert_query = false;
                        break;
                    }

                    asterisk = true;
                }
                else if (auto * func = (*expression)->as<ASTFunction>())
                {
                    if (use_structure_from_insertion_table_in_table_functions == 2 && findIdentifier(func))
                    {
                        use_columns_from_insert_query = false;
                        break;
                    }

                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
                else
                {
                    /// Once we hit asterisk we want to find end of the range covered by asterisk
                    /// contributing every further SELECT expression to the tail of insert structure
                    if (asterisk)
                        --insert_column_names_end;
                    else
                        ++insert_column_name_it;
                }
            }

            if (use_structure_from_insertion_table_in_table_functions == 2 && !asterisk)
            {
                /// For input function we should check if input format supports reading subset of columns.
                if (table_function_ptr->getName() == "input")
                    use_columns_from_insert_query = FormatFactory::instance().checkIfFormatSupportsSubsetOfColumns(getInsertFormat(), shared_from_this());
                else
                    use_columns_from_insert_query = table_function_ptr->supportsReadingSubsetOfColumns(shared_from_this());
            }

            if (use_columns_from_insert_query)
            {
                if (expression == expression_list.end())
                {
                    /// Append tail of insert structure to the hint
                    if (asterisk)
                    {
                        for (; insert_column_name_it != insert_column_names_end; ++insert_column_name_it)
                        {
                            ColumnDescription column = insert_columns.get(*insert_column_name_it);
                            /// Change ephemeral columns to default columns.
                            column.default_desc.kind = ColumnDefaultKind::Default;

                            structure_hint.add(std::move(column));
                        }
                    }

                    if (!structure_hint.empty())
                        table_function_ptr->setStructureHint(structure_hint);
                }
                else if (use_structure_from_insertion_table_in_table_functions == 1)
                    throw Exception(ErrorCodes::NUMBER_OF_COLUMNS_DOESNT_MATCH, "Number of columns in insert table less than required by SELECT expression.");
            }
        }

        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());

        /// Since ITableFunction::parseArguments() may change table_expression, i.e.:
        ///
        ///     remote('127.1', system.one) -> remote('127.1', 'system.one'),
        ///
        auto new_hash = table_expression->getTreeHash(/*ignore_aliases=*/ true);
        if (hash != new_hash)
        {
            key = toString(new_hash);
            table_function_results[key] = res;
        }
    }
    return res;
}

StoragePtr Context::executeTableFunction(const ASTPtr & table_expression, const TableFunctionPtr & table_function_ptr)
{
    const auto hash = table_expression->getTreeHash(/*ignore_aliases=*/ true);
    const auto key = toString(hash);
    StoragePtr & res = table_function_results[key];

    if (!res)
    {
        res = table_function_ptr->execute(table_expression, shared_from_this(), table_function_ptr->getName());
    }

    return res;
}


StoragePtr Context::buildParameterizedViewStorage(const String & database_name, const String & table_name, const NameToNameMap & param_values)
{
    if (table_name.empty())
        return nullptr;

    StoragePtr original_view = DatabaseCatalog::instance().tryGetTable({database_name, table_name}, getQueryContext());
    if (!original_view || !original_view->isView())
        return nullptr;
    auto * storage_view = original_view->as<StorageView>();
    if (!storage_view || !storage_view->isParameterizedView())
        return nullptr;

    auto original_view_metadata = original_view->getInMemoryMetadataPtr();
    auto query = original_view_metadata->getSelectQuery().inner_query->clone();
    StorageView::replaceQueryParametersIfParameterizedView(query, param_values);

    ASTCreateQuery create;
    create.select = query->as<ASTSelectWithUnionQuery>();

    auto sql_security = std::make_shared<ASTSQLSecurity>();
    sql_security->type = original_view_metadata->sql_security_type;
    if (original_view_metadata->definer)
        sql_security->definer = std::make_shared<ASTUserNameWithHost>(*original_view_metadata->definer);
    create.sql_security = sql_security;

    auto view_context = original_view_metadata->getSQLSecurityOverriddenContext(shared_from_this());
    auto sample_block = InterpreterSelectQueryAnalyzer::getSampleBlock(query, view_context);
    auto res = std::make_shared<StorageView>(StorageID(database_name, table_name),
                                                create,
                                                ColumnsDescription(sample_block.getNamesAndTypesList()),
            /* comment */ "",
            /* is_parameterized_view */ true);
    res->startup();
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

bool Context::displaySecretsInShowAndSelect() const
{
    return shared->server_settings[ServerSetting::display_secrets_in_show_and_select];
}

Settings Context::getSettingsCopy() const
{
    SharedLockGuard lock(mutex);
    return *settings;
}

void Context::setSettings(const Settings & settings_)
{
    std::lock_guard lock(mutex);
    *settings = settings_;
    need_recalculate_access = true;
    contextSanityClampSettings(*this, *settings);
}

void Context::setSettingWithLock(std::string_view name, const String & value, const std::lock_guard<ContextSharedMutex> & lock)
{
    if (name == "profile")
    {
        setCurrentProfileWithLock(value, true /*check_constraints*/, lock);
        return;
    }
    settings->set(name, value);
    if (ContextAccessParams::dependsOnSettingName(name))
        need_recalculate_access = true;
    contextSanityClampSettingsWithLock(*this, *settings, lock);
}

void Context::setSettingWithLock(std::string_view name, const Field & value, const std::lock_guard<ContextSharedMutex> & lock)
{
    if (name == "profile")
    {
        setCurrentProfileWithLock(value.safeGet<String>(), true /*check_constraints*/, lock);
        return;
    }
    settings->set(name, value);
    if (ContextAccessParams::dependsOnSettingName(name))
        need_recalculate_access = true;
}

void Context::applySettingChangeWithLock(const SettingChange & change, const std::lock_guard<ContextSharedMutex> & lock)
{
    try
    {
        setSettingWithLock(change.name, change.value, lock);
        contextSanityClampSettingsWithLock(*this, *settings, lock);
    }
    catch (Exception & e)
    {
        e.addMessage(fmt::format(
                         "in attempt to set the value of setting '{}' to {}",
                         change.name, applyVisitor(FieldVisitorToString(), change.value)));
        throw;
    }
}

void Context::applySettingsChangesWithLock(const SettingsChanges & changes, const std::lock_guard<ContextSharedMutex>& lock)
{
    for (const SettingChange & change : changes)
        applySettingChangeWithLock(change, lock);
    applySettingsQuirks(*settings);
}

void Context::setSetting(std::string_view name, const String & value)
{
    std::lock_guard lock(mutex);
    setSettingWithLock(name, value, lock);
}

void Context::setSetting(std::string_view name, const Field & value)
{
    std::lock_guard lock(mutex);
    setSettingWithLock(name, value, lock);
    contextSanityClampSettingsWithLock(*this, *settings, lock);
}

void Context::setServerSetting(std::string_view name, const Field & value)
{
    std::lock_guard lock(mutex);
    shared->server_settings.set(name, value);
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
    std::lock_guard lock(mutex);
    applySettingsChangesWithLock(changes, lock);
}

void Context::checkSettingsConstraintsWithLock(const AlterSettingsProfileElements & profile_elements, SettingSource source)
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(*settings, profile_elements, source);
    if (getApplicationType() == ApplicationType::LOCAL || getApplicationType() == ApplicationType::SERVER)
        doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));
}

void Context::checkSettingsConstraintsWithLock(const SettingChange & change, SettingSource source)
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(*settings, change, source);
    if (getApplicationType() == ApplicationType::LOCAL || getApplicationType() == ApplicationType::SERVER)
        doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));
}

void Context::checkSettingsConstraintsWithLock(const SettingsChanges & changes, SettingSource source)
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(*settings, changes, source);
    if (getApplicationType() == ApplicationType::LOCAL || getApplicationType() == ApplicationType::SERVER)
        doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));
}

void Context::checkSettingsConstraintsWithLock(SettingsChanges & changes, SettingSource source)
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(*settings, changes, source);
    if (getApplicationType() == ApplicationType::LOCAL || getApplicationType() == ApplicationType::SERVER)
        doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));
}

void Context::clampToSettingsConstraintsWithLock(SettingsChanges & changes, SettingSource source)
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.clamp(*settings, changes, source);
    if (getApplicationType() == ApplicationType::LOCAL || getApplicationType() == ApplicationType::SERVER)
        doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));
}

void Context::checkMergeTreeSettingsConstraintsWithLock(const MergeTreeSettings & merge_tree_settings, const SettingsChanges & changes) const
{
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(merge_tree_settings, changes);
}

void Context::checkSettingsConstraints(const AlterSettingsProfileElements & profile_elements, SettingSource source)
{
    SharedLockGuard lock(mutex);
    checkSettingsConstraintsWithLock(profile_elements, source);
}

void Context::checkSettingsConstraints(const SettingChange & change, SettingSource source)
{
    SharedLockGuard lock(mutex);
    checkSettingsConstraintsWithLock(change, source);
}

void Context::checkSettingsConstraints(const SettingsChanges & changes, SettingSource source)
{
    SharedLockGuard lock(mutex);
    getSettingsConstraintsAndCurrentProfilesWithLock()->constraints.check(*settings, changes, source);
    doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));
}

void Context::checkSettingsConstraints(SettingsChanges & changes, SettingSource source)
{
    SharedLockGuard lock(mutex);
    checkSettingsConstraintsWithLock(changes, source);
}

void Context::clampToSettingsConstraints(SettingsChanges & changes, SettingSource source)
{
    SharedLockGuard lock(mutex);
    clampToSettingsConstraintsWithLock(changes, source);
}

void Context::checkMergeTreeSettingsConstraints(const MergeTreeSettings & merge_tree_settings, const SettingsChanges & changes) const
{
    SharedLockGuard lock(mutex);
    checkMergeTreeSettingsConstraintsWithLock(merge_tree_settings, changes);
}

void Context::resetSettingsToDefaultValue(const std::vector<String> & names)
{
    std::lock_guard lock(mutex);
    for (const String & name: names)
        settings->setDefaultValue(name);
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfilesWithLock() const
{
    if (settings_constraints_and_current_profiles)
        return settings_constraints_and_current_profiles;
    static auto no_constraints_or_profiles = std::make_shared<SettingsConstraintsAndProfileIDs>(getAccessControl());
    return no_constraints_or_profiles;
}

std::shared_ptr<const SettingsConstraintsAndProfileIDs> Context::getSettingsConstraintsAndCurrentProfiles() const
{
    SharedLockGuard lock(mutex);
    return getSettingsConstraintsAndCurrentProfilesWithLock();
}

String Context::getCurrentDatabase() const
{
    SharedLockGuard lock(mutex);
    return current_database;
}


String Context::getInitialQueryId() const
{
    return client_info.initial_query_id;
}


void Context::setCurrentDatabaseNameInGlobalContext(const String & name)
{
    if (name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name cannot be empty");

    if (!isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
                        "Cannot set current database for non global context, this method should "
                        "be used during server initialization");
    std::lock_guard lock(mutex);

    if (!current_database.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Default database name cannot be changed in global context without server restart");

    current_database = name;
}

void Context::setCurrentDatabaseWithLock(const String & name, const std::lock_guard<ContextSharedMutex> &)
{
    if (name.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Database name cannot be empty");

    DatabaseCatalog::instance().assertDatabaseExists(name);
    current_database = name;
    need_recalculate_access = true;
}

void Context::setCurrentDatabase(const String & name)
{
    std::lock_guard lock(mutex);
    setCurrentDatabaseWithLock(name, lock);
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

void Context::setBackgroundOperationTypeForContext(ClientInfo::BackgroundOperationType background_operation)
{
    chassert(background_operation != ClientInfo::BackgroundOperationType::NOT_A_BACKGROUND_OPERATION);
    client_info.background_operation_type = background_operation;
}

bool Context::isBackgroundOperationContext() const
{
    return client_info.background_operation_type != ClientInfo::BackgroundOperationType::NOT_A_BACKGROUND_OPERATION;
}

void Context::killCurrentQuery() const
{
    if (auto elem = getProcessListElement())
        elem->cancelQuery(CancelReason::CANCELLED_BY_USER);
}

bool Context::isCurrentQueryKilled() const
{
    /// Here getProcessListElementSafe is used, not getProcessListElement call
    /// getProcessListElement requires that process list exists
    /// In the most cases it is true, because process list exists during the query execution time.
    /// That is valid for all operations with parts, like read and write operations.
    /// However that Context::isCurrentQueryKilled call could be used on the edges
    /// when query is starting or finishing, in such edges context still exist but process list already expired
    if (auto elem = getProcessListElementSafe())
        return elem->isKilled();

    return false;
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
    if (!ptr)
        throw Exception(ErrorCodes::THERE_IS_NO_QUERY, "There is no query or query context has expired");
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

void Context::makeQueryContext()
{
    query_context = shared_from_this();

    /// Throttling should not be inherited, otherwise if you will set
    /// throttling for default profile you will not able to overwrite it
    /// per-user/query.
    ///
    /// Note, that if you need to set it server-wide, you should use
    /// per-server settings, i.e.:
    /// - max_backup_bandwidth_for_server
    /// - max_remote_read_network_bandwidth_for_server
    /// - max_remote_write_network_bandwidth_for_server
    /// - max_local_read_bandwidth_for_server
    /// - max_local_write_bandwidth_for_server
    remote_read_query_throttler.reset();
    remote_write_query_throttler.reset();
    local_read_query_throttler.reset();
    local_write_query_throttler.reset();
    backups_query_throttler.reset();
    query_privileges_info = std::make_shared<QueryPrivilegesInfo>(*query_privileges_info);
}

void Context::makeQueryContextForMerge(const MergeTreeSettings & merge_tree_settings)
{
    makeQueryContext();
    classifier.reset(); // It is assumed that there are no active queries running using this classifier, otherwise this will lead to crashes
    (*settings)[Setting::workload] = merge_tree_settings[MergeTreeSetting::merge_workload].value.empty() ? getMergeWorkload() : merge_tree_settings[MergeTreeSetting::merge_workload];
}

void Context::makeQueryContextForMutate(const MergeTreeSettings & merge_tree_settings)
{
    makeQueryContext();
    classifier.reset(); // It is assumed that there are no active queries running using this classifier, otherwise this will lead to crashes
    (*settings)[Setting::workload]
        = merge_tree_settings[MergeTreeSetting::mutation_workload].value.empty() ? getMutationWorkload() : merge_tree_settings[MergeTreeSetting::mutation_workload];
}

void Context::makeSessionContext()
{
    session_context = shared_from_this();
}

void Context::makeGlobalContext()
{
    assert(!global_context_instance);
    global_context_instance = shared_from_this();
    DatabaseCatalog::init(shared_from_this());
    EventNotifier::init();

    global_context = shared_from_this();
}

const EmbeddedDictionaries & Context::getEmbeddedDictionaries() const
{
    return getEmbeddedDictionariesImpl(false);
}

EmbeddedDictionaries & Context::getEmbeddedDictionaries()
{
    return getEmbeddedDictionariesImpl(false);
}

AsyncLoader & Context::getAsyncLoader() const
{
    callOnce(shared->async_loader_initialized, [&] {
        shared->async_loader = std::make_unique<AsyncLoader>(std::vector<AsyncLoader::PoolInitializer>{
                // IMPORTANT: Pool declaration order should match the order in `PoolId.h` to get the indices right.
                { // TablesLoaderForegroundPoolId
                    "ForegroundLoad",
                    CurrentMetrics::TablesLoaderForegroundThreads,
                    CurrentMetrics::TablesLoaderForegroundThreadsActive,
                    CurrentMetrics::TablesLoaderForegroundThreadsScheduled,
                    shared->server_settings[ServerSetting::tables_loader_foreground_pool_size],
                    TablesLoaderForegroundPriority
                },
                { // TablesLoaderBackgroundLoadPoolId
                    "BackgroundLoad",
                    CurrentMetrics::TablesLoaderBackgroundThreads,
                    CurrentMetrics::TablesLoaderBackgroundThreadsActive,
                    CurrentMetrics::TablesLoaderBackgroundThreadsScheduled,
                    shared->server_settings[ServerSetting::tables_loader_background_pool_size],
                    TablesLoaderBackgroundLoadPriority
                },
                { // TablesLoaderBackgroundStartupPoolId
                    "BackgrndStartup",
                    CurrentMetrics::TablesLoaderBackgroundThreads,
                    CurrentMetrics::TablesLoaderBackgroundThreadsActive,
                    CurrentMetrics::TablesLoaderBackgroundThreadsScheduled,
                    shared->server_settings[ServerSetting::tables_loader_background_pool_size],
                    TablesLoaderBackgroundStartupPriority
                }
            },
            /* log_failures = */ true,
            /* log_progress = */ true,
            /* log_events = */ true);
    });

    return *shared->async_loader;
}


const ExternalDictionariesLoader & Context::getExternalDictionariesLoader() const
{
    return const_cast<Context *>(this)->getExternalDictionariesLoader();
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoader()
{
    std::lock_guard lock(shared->external_dictionaries_mutex);
    return getExternalDictionariesLoaderWithLock(lock);
}

ExternalDictionariesLoader & Context::getExternalDictionariesLoaderWithLock(const std::lock_guard<std::mutex> &) TSA_REQUIRES(shared->external_dictionaries_mutex)
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
    return getExternalUserDefinedExecutableFunctionsLoaderWithLock(lock);
}

ExternalUserDefinedExecutableFunctionsLoader &
Context::getExternalUserDefinedExecutableFunctionsLoaderWithLock(const std::lock_guard<std::mutex> &) TSA_REQUIRES(shared->external_user_defined_executable_functions_mutex)
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


void Context::tryCreateEmbeddedDictionaries() const
{
    if (!shared->server_settings[ServerSetting::dictionaries_lazy_load])
        static_cast<void>(getEmbeddedDictionariesImpl(true));
}

void Context::loadOrReloadDictionaries(const Poco::Util::AbstractConfiguration & config)
{
    bool dictionaries_lazy_load = shared->server_settings[ServerSetting::dictionaries_lazy_load];
    auto patterns_values = getMultipleValuesFromConfig(config, "", "dictionaries_config");
    std::unordered_set<std::string> patterns(patterns_values.begin(), patterns_values.end());

    std::lock_guard lock(shared->external_dictionaries_mutex);

    auto & external_dictionaries_loader = getExternalDictionariesLoaderWithLock(lock);
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

void Context::waitForDictionariesLoad() const
{
    LOG_INFO(shared->log, "Waiting for dictionaries to be loaded");
    auto results = getExternalDictionariesLoader().tryLoadAll<ExternalLoader::LoadResults>();
    bool all_dictionaries_loaded = true;
    for (const auto & result : results)
    {
        if ((result.status != ExternalLoaderStatus::LOADED) && (result.status != ExternalLoaderStatus::LOADED_AND_RELOADING))
        {
            LOG_WARNING(shared->log, "Dictionary {} was not loaded ({})", result.name, result.status);
            all_dictionaries_loaded = false;
        }
    }
    if (all_dictionaries_loaded)
        LOG_INFO(shared->log, "All dictionaries have been loaded");
    else
        LOG_INFO(shared->log, "Some dictionaries were not loaded");
}

void Context::loadOrReloadUserDefinedExecutableFunctions(const Poco::Util::AbstractConfiguration & config)
{
    auto patterns_values = getMultipleValuesFromConfig(config, "", "user_defined_executable_functions_config");
    std::unordered_set<std::string> patterns(patterns_values.begin(), patterns_values.end());

    std::lock_guard lock(shared->external_user_defined_executable_functions_mutex);

    auto & external_user_defined_executable_functions_loader = getExternalUserDefinedExecutableFunctionsLoaderWithLock(lock);

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

const IUserDefinedSQLObjectsStorage & Context::getUserDefinedSQLObjectsStorage() const
{
    callOnce(shared->user_defined_sql_objects_storage_initialized, [&] {
        shared->user_defined_sql_objects_storage = createUserDefinedSQLObjectsStorage(getGlobalContext());
    });

    SharedLockGuard lock(shared->mutex);
    return *shared->user_defined_sql_objects_storage;
}

IUserDefinedSQLObjectsStorage & Context::getUserDefinedSQLObjectsStorage()
{
    callOnce(shared->user_defined_sql_objects_storage_initialized, [&] {
        shared->user_defined_sql_objects_storage = createUserDefinedSQLObjectsStorage(getGlobalContext());
    });

    std::lock_guard lock(shared->mutex);
    return *shared->user_defined_sql_objects_storage;
}

void Context::setUserDefinedSQLObjectsStorage(std::unique_ptr<IUserDefinedSQLObjectsStorage> storage)
{
    std::lock_guard lock(shared->mutex);
    shared->user_defined_sql_objects_storage = std::move(storage);
}

IWorkloadEntityStorage & Context::getWorkloadEntityStorage() const
{
    callOnce(shared->workload_entity_storage_initialized, [&] {
        shared->workload_entity_storage = createWorkloadEntityStorage(getGlobalContext());
    });

    std::lock_guard lock(shared->mutex);
    return *shared->workload_entity_storage;
}

#if USE_NLP

SynonymsExtensions & Context::getSynonymsExtensions() const
{
    callOnce(shared->synonyms_extensions_initialized, [&] {
        shared->synonyms_extensions.emplace(getConfigRef());
    });

    return *shared->synonyms_extensions;
}

Lemmatizers & Context::getLemmatizers() const
{
    callOnce(shared->lemmatizers_initialized, [&] {
        shared->lemmatizers.emplace(getConfigRef());
    });

    return *shared->lemmatizers;
}
#endif

BackupsWorker & Context::getBackupsWorker() const
{
    callOnce(shared->backups_worker_initialized, [&] {
        const auto & config = getConfigRef();
        Poco::UInt64 max_threads_max_value = 256 * getNumberOfCPUCoresToUse(); /// Limit to something unreasonable
        size_t backup_threads = std::min(max_threads_max_value, std::max(Poco::UInt64{1}, config.getUInt64("backup_threads", 16)));
        size_t restore_threads = std::min(max_threads_max_value, std::max(Poco::UInt64{1}, config.getUInt64("restore_threads", 16)));

        shared->backups_worker.emplace(getGlobalContext(), backup_threads, restore_threads);
    });

    return *shared->backups_worker;
}

void Context::waitAllBackupsAndRestores() const
{
    if (shared->backups_worker)
        shared->backups_worker->waitAll();
}

BackupsInMemoryHolder & Context::getBackupsInMemory()
{
    return backups_in_memory;
}

const BackupsInMemoryHolder & Context::getBackupsInMemory() const
{
    return backups_in_memory;
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
    if (isGlobalContext())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context cannot have process list element");

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

QueryStatusPtr Context::getProcessListElementSafe() const
{
    if (!has_process_list_elem)
        return {};
    if (auto res = process_list_elem.lock())
        return res;
    return {};
}

void Context::setUncompressedCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Uncompressed cache has been already created.");

    shared->uncompressed_cache = std::make_shared<UncompressedCache>(cache_policy, max_size_in_bytes, size_ratio);
}

void Context::updateUncompressedCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Uncompressed cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("uncompressed_cache_size", DEFAULT_UNCOMPRESSED_CACHE_MAX_SIZE);
    shared->uncompressed_cache->setMaxSizeInBytes(max_size_in_bytes);
}

UncompressedCachePtr Context::getUncompressedCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->uncompressed_cache;
}

void Context::clearUncompressedCache() const
{
    UncompressedCachePtr cache = getUncompressedCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}

void Context::setPageCache(std::chrono::milliseconds history_window,
    const String & cache_policy, double size_ratio, size_t min_size_in_bytes, size_t max_size_in_bytes,
    double free_memory_ratio, size_t num_shards)
{
    std::lock_guard lock(shared->mutex);

    if (shared->page_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Page cache has been already created.");

    shared->page_cache = std::make_shared<PageCache>(
        history_window, cache_policy, size_ratio,
        min_size_in_bytes, max_size_in_bytes, free_memory_ratio, num_shards);
}

PageCachePtr Context::getPageCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->page_cache;
}

void Context::clearPageCache() const
{
    PageCachePtr cache;
    {
        SharedLockGuard lock(shared->mutex);
        cache = shared->page_cache;
    }
    if (cache)
        cache->clear();
}

void Context::setMarkCache(const String & cache_policy, size_t max_cache_size_in_bytes, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark cache has been already created.");

    shared->mark_cache = std::make_shared<MarkCache>(cache_policy, max_cache_size_in_bytes, size_ratio);
}

void Context::updateMarkCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("mark_cache_size", DEFAULT_MARK_CACHE_MAX_SIZE);
    shared->mark_cache->setMaxSizeInBytes(max_size_in_bytes);
}

MarkCachePtr Context::getMarkCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->mark_cache;
}

void Context::clearMarkCache() const
{
    MarkCachePtr cache = getMarkCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}

ThreadPool & Context::getLoadMarksThreadpool() const
{
    callOnce(shared->load_marks_threadpool_initialized, [&] {
        auto pool_size = shared->server_settings[ServerSetting::load_marks_threadpool_pool_size];
        auto queue_size = shared->server_settings[ServerSetting::load_marks_threadpool_queue_size];
        shared->load_marks_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::MarksLoaderThreads, CurrentMetrics::MarksLoaderThreadsActive, CurrentMetrics::MarksLoaderThreadsScheduled, pool_size, pool_size, queue_size);
    });

    return *shared->load_marks_threadpool;
}

ThreadPool & Context::getIcebergCatalogThreadpool() const
{
    callOnce(shared->iceberg_catalog_threadpool_initialized, [&]
    {
        auto pool_size = shared->server_settings[ServerSetting::iceberg_catalog_threadpool_pool_size];
        auto queue_size = shared->server_settings[ServerSetting::iceberg_catalog_threadpool_queue_size];

        shared->iceberg_catalog_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::IcebergCatalogThreads,
            CurrentMetrics::IcebergCatalogThreadsActive,
            CurrentMetrics::IcebergCatalogThreadsScheduled,
            pool_size, pool_size, queue_size);
    });

    return *shared->iceberg_catalog_threadpool;
}

void Context::setPrimaryIndexCache(const String & cache_policy, size_t max_cache_size_in_bytes, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->primary_index_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Primary index cache has been already created.");

    shared->primary_index_cache = std::make_shared<PrimaryIndexCache>(cache_policy, max_cache_size_in_bytes, size_ratio);
}

void Context::updatePrimaryIndexCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->primary_index_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Primary index cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("primary_index_cache_size", DEFAULT_PRIMARY_INDEX_CACHE_MAX_SIZE);
    shared->primary_index_cache->setMaxSizeInBytes(max_size_in_bytes);
}

PrimaryIndexCachePtr Context::getPrimaryIndexCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->primary_index_cache;
}

void Context::clearPrimaryIndexCache() const
{
    PrimaryIndexCachePtr cache = getPrimaryIndexCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}

void Context::setIndexUncompressedCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->index_uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index uncompressed cache has been already created.");

    shared->index_uncompressed_cache = std::make_shared<UncompressedCache>(cache_policy, max_size_in_bytes, size_ratio);
}

void Context::updateIndexUncompressedCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->index_uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index uncompressed cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("index_uncompressed_cache_size", DEFAULT_INDEX_UNCOMPRESSED_CACHE_MAX_SIZE);
    shared->index_uncompressed_cache->setMaxSizeInBytes(max_size_in_bytes);
}

UncompressedCachePtr Context::getIndexUncompressedCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->index_uncompressed_cache;
}

void Context::clearIndexUncompressedCache() const
{
    UncompressedCachePtr cache = getIndexUncompressedCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}

void Context::setIndexMarkCache(const String & cache_policy, size_t max_cache_size_in_bytes, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->index_mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index mark cache has been already created.");

    shared->index_mark_cache = std::make_shared<MarkCache>(cache_policy, max_cache_size_in_bytes, size_ratio);
}

void Context::updateIndexMarkCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->index_mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index mark cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("index_mark_cache_size", DEFAULT_INDEX_MARK_CACHE_MAX_SIZE);
    shared->index_mark_cache->setMaxSizeInBytes(max_size_in_bytes);
}

MarkCachePtr Context::getIndexMarkCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->index_mark_cache;
}

void Context::clearIndexMarkCache() const
{
    MarkCachePtr cache = getIndexMarkCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}

void Context::setVectorSimilarityIndexCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_entries, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->vector_similarity_index_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index cache has been already created.");

    shared->vector_similarity_index_cache = std::make_shared<VectorSimilarityIndexCache>(cache_policy, max_size_in_bytes, max_entries, size_ratio);
}

void Context::updateVectorSimilarityIndexCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->vector_similarity_index_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("vector_similarity_index_cache_size", DEFAULT_VECTOR_SIMILARITY_INDEX_CACHE_MAX_SIZE);
    size_t max_entries = config.getUInt64("vector_similarity_index_cache_max_entries", DEFAULT_VECTOR_SIMILARITY_INDEX_CACHE_MAX_ENTRIES);
    shared->vector_similarity_index_cache->setMaxSizeInBytes(max_size_in_bytes);
    shared->vector_similarity_index_cache->setMaxCount(max_entries);
}

VectorSimilarityIndexCachePtr Context::getVectorSimilarityIndexCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->vector_similarity_index_cache;
}

void Context::clearVectorSimilarityIndexCache() const
{
    std::lock_guard lock(shared->mutex);

    if (shared->vector_similarity_index_cache)
        shared->vector_similarity_index_cache->clear();
}

void Context::setMMappedFileCache(size_t max_cache_size_in_num_entries)
{
    std::lock_guard lock(shared->mutex);

    if (shared->mmap_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped file cache has been already created.");

    shared->mmap_cache = std::make_shared<MMappedFileCache>(max_cache_size_in_num_entries);
}

void Context::updateMMappedFileCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->mmap_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mapped file cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("mmap_cache_size", DEFAULT_MMAP_CACHE_MAX_SIZE);
    shared->mmap_cache->setMaxSizeInBytes(max_size_in_bytes);
}

MMappedFileCachePtr Context::getMMappedFileCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->mmap_cache;
}

void Context::clearMMappedFileCache() const
{
    MMappedFileCachePtr cache = getMMappedFileCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}

#if USE_AVRO
void Context::setIcebergMetadataFilesCache(const String & cache_policy, size_t max_size_in_bytes, size_t max_entries, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->iceberg_metadata_files_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg metadata cache has been already created.");

    shared->iceberg_metadata_files_cache = std::make_shared<IcebergMetadataFilesCache>(cache_policy, max_size_in_bytes, max_entries, size_ratio);
}

void Context::updateIcebergMetadataFilesCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->iceberg_metadata_files_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Iceberg metadata cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("iceberg_metadata_files_cache_size", DEFAULT_ICEBERG_METADATA_CACHE_MAX_SIZE);
    size_t max_entries = config.getUInt64("iceberg_metadata_files_cache_max_entries", DEFAULT_ICEBERG_METADATA_CACHE_MAX_ENTRIES);
    shared->iceberg_metadata_files_cache->setMaxSizeInBytes(max_size_in_bytes);
    shared->iceberg_metadata_files_cache->setMaxCount(max_entries);
}

std::shared_ptr<IcebergMetadataFilesCache> Context::getIcebergMetadataFilesCache() const
{
    std::lock_guard lock(shared->mutex);
    return shared->iceberg_metadata_files_cache;
}

void Context::clearIcebergMetadataFilesCache() const
{
    auto cache = getIcebergMetadataFilesCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear();
}
#endif

void Context::setQueryConditionCache(const String & cache_policy, size_t max_size_in_bytes, double size_ratio)
{
    std::lock_guard lock(shared->mutex);

    if (shared->query_condition_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark filter cache has been already create.");

    shared->query_condition_cache = std::make_shared<QueryConditionCache>(cache_policy, max_size_in_bytes, size_ratio);
}

QueryConditionCachePtr Context::getQueryConditionCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->query_condition_cache;
}

void Context::updateQueryConditionCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->query_condition_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query condition cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("query_condition_cache_size", DEFAULT_QUERY_CONDITION_CACHE_MAX_SIZE);
    shared->query_condition_cache->setMaxSizeInBytes(max_size_in_bytes);
}

void Context::clearQueryConditionCache() const
{
    std::lock_guard lock(shared->mutex);

    if (shared->query_condition_cache)
        shared->query_condition_cache->clear();
}


void Context::setQueryResultCache(size_t max_size_in_bytes, size_t max_entries, size_t max_entry_size_in_bytes, size_t max_entry_size_in_rows)
{
    std::lock_guard lock(shared->mutex);

    if (shared->query_result_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query cache has been already created.");

    shared->query_result_cache = std::make_shared<QueryResultCache>(max_size_in_bytes, max_entries, max_entry_size_in_bytes, max_entry_size_in_rows);
}

void Context::updateQueryResultCacheConfiguration(const Poco::Util::AbstractConfiguration & config)
{
    std::lock_guard lock(shared->mutex);

    if (!shared->query_result_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query cache was not created yet.");

    size_t max_size_in_bytes = config.getUInt64("query_cache.max_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_SIZE);
    size_t max_entries = config.getUInt64("query_cache.max_entries", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRIES);
    size_t max_entry_size_in_bytes = config.getUInt64("query_cache.max_entry_size_in_bytes", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_BYTES);
    size_t max_entry_size_in_rows = config.getUInt64("query_cache.max_entry_rows_in_rows", DEFAULT_QUERY_RESULT_CACHE_MAX_ENTRY_SIZE_IN_ROWS);
    shared->query_result_cache->updateConfiguration(max_size_in_bytes, max_entries, max_entry_size_in_bytes, max_entry_size_in_rows);
}

QueryResultCachePtr Context::getQueryResultCache() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->query_result_cache;
}

void Context::clearQueryResultCache(const std::optional<String> & tag) const
{
    QueryResultCachePtr cache = getQueryResultCache();

    /// Clear the cache without holding context mutex to avoid blocking context for a long time
    if (cache)
        cache->clear(tag);
}

void Context::clearCaches() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Uncompressed cache was not created yet.");
    shared->uncompressed_cache->clear();

    if (!shared->mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mark cache was not created yet.");
    shared->mark_cache->clear();

    if (!shared->primary_index_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Primary index cache was not created yet.");
    shared->primary_index_cache->clear();

    if (!shared->index_uncompressed_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index uncompressed cache was not created yet.");
    shared->index_uncompressed_cache->clear();

    if (!shared->index_mark_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index mark cache was not created yet.");
    shared->index_mark_cache->clear();

    if (!shared->vector_similarity_index_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Vector similarity index cache was not created yet.");
    shared->vector_similarity_index_cache->clear();

    if (!shared->mmap_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Mmapped file cache was not created yet.");
    shared->mmap_cache->clear();

    if (!shared->query_condition_cache)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query condition cache was not created yet.");
    shared->query_condition_cache->clear();

    /// Intentionally not clearing the query result cache which is transactionally inconsistent by design.
}

void Context::setAsynchronousMetrics(AsynchronousMetrics * asynchronous_metrics_)
{
    std::lock_guard lock(shared->mutex);
    shared->asynchronous_metrics = asynchronous_metrics_;
}

AsynchronousMetrics * Context::getAsynchronousMetrics() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->asynchronous_metrics;
}

ThreadPool & Context::getPrefetchThreadpool() const
{
    callOnce(shared->prefetch_threadpool_initialized, [&] {
        auto pool_size = shared->server_settings[ServerSetting::prefetch_threadpool_pool_size];
        auto queue_size = shared->server_settings[ServerSetting::prefetch_threadpool_queue_size];

        shared->prefetch_threadpool = std::make_unique<ThreadPool>(
            CurrentMetrics::IOPrefetchThreads, CurrentMetrics::IOPrefetchThreadsActive, CurrentMetrics::IOPrefetchThreadsScheduled, pool_size, pool_size, queue_size);
    });

    return *shared->prefetch_threadpool;
}

size_t Context::getPrefetchThreadpoolSize() const
{
    return shared->server_settings[ServerSetting::prefetch_threadpool_pool_size];
}

ThreadPool & Context::getBuildVectorSimilarityIndexThreadPool() const
{
    callOnce(
        shared->build_vector_similarity_index_threadpool_initialized,
        [&]
        {
            size_t pool_size = shared->server_settings[ServerSetting::max_build_vector_similarity_index_thread_pool_size] > 0
                ? shared->server_settings[ServerSetting::max_build_vector_similarity_index_thread_pool_size]
                : getNumberOfCPUCoresToUse();
            shared->build_vector_similarity_index_threadpool = std::make_unique<ThreadPool>(
                CurrentMetrics::BuildVectorSimilarityIndexThreads,
                CurrentMetrics::BuildVectorSimilarityIndexThreadsActive,
                CurrentMetrics::BuildVectorSimilarityIndexThreadsScheduled,
                pool_size);
        });
    return *shared->build_vector_similarity_index_threadpool;
}

BackgroundSchedulePool & Context::getBufferFlushSchedulePool() const
{
    callOnce(shared->buffer_flush_schedule_pool_initialized, [&] {
        shared->buffer_flush_schedule_pool = BackgroundSchedulePool::create(
            shared->server_settings[ServerSetting::background_buffer_flush_schedule_pool_size],
            CurrentMetrics::BackgroundBufferFlushSchedulePoolTask,
            CurrentMetrics::BackgroundBufferFlushSchedulePoolSize,
            "BgBufSchPool");
    });

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
    callOnce(shared->schedule_pool_initialized, [&] {
        shared->schedule_pool = BackgroundSchedulePool::create(
            shared->server_settings[ServerSetting::background_schedule_pool_size],
            CurrentMetrics::BackgroundSchedulePoolTask,
            CurrentMetrics::BackgroundSchedulePoolSize,
            "BgSchPool");
    });

    return *shared->schedule_pool;
}

BackgroundSchedulePool & Context::getDistributedSchedulePool() const
{
    callOnce(shared->distributed_schedule_pool_initialized, [&] {
        shared->distributed_schedule_pool = BackgroundSchedulePool::create(
            shared->server_settings[ServerSetting::background_distributed_schedule_pool_size],
            CurrentMetrics::BackgroundDistributedSchedulePoolTask,
            CurrentMetrics::BackgroundDistributedSchedulePoolSize,
            "BgDistSchPool");
    });

    return *shared->distributed_schedule_pool;
}

BackgroundSchedulePool & Context::getMessageBrokerSchedulePool() const
{
    callOnce(shared->message_broker_schedule_pool_initialized, [&] {
        shared->message_broker_schedule_pool = BackgroundSchedulePool::create(
            shared->server_settings[ServerSetting::background_message_broker_schedule_pool_size],
            CurrentMetrics::BackgroundMessageBrokerSchedulePoolTask,
            CurrentMetrics::BackgroundMessageBrokerSchedulePoolSize,
            "BgMBSchPool");
    });

    return *shared->message_broker_schedule_pool;
}

ThrottlerPtr Context::getReplicatedFetchesThrottler() const
{
    return shared->replicated_fetches_throttler;
}

ThrottlerPtr Context::getReplicatedSendsThrottler() const
{
    return shared->replicated_sends_throttler;
}

ThrottlerPtr Context::getRemoteReadThrottler() const
{
    ThrottlerPtr throttler;
    {
        std::lock_guard lock(shared->mutex);
        throttler = shared->remote_read_throttler;
    }

    if (auto bandwidth = getSettingsRef()[Setting::max_remote_read_network_bandwidth])
    {
        std::lock_guard lock(mutex);
        if (!remote_read_query_throttler)
            remote_read_query_throttler = std::make_shared<Throttler>(bandwidth, throttler);
        throttler = remote_read_query_throttler;
    }
    return throttler;
}

ThrottlerPtr Context::getRemoteWriteThrottler() const
{
    ThrottlerPtr throttler;
    {
        std::lock_guard lock(shared->mutex);
        throttler = shared->remote_write_throttler;
    }

    if (auto bandwidth = getSettingsRef()[Setting::max_remote_write_network_bandwidth])
    {
        std::lock_guard lock(mutex);
        if (!remote_write_query_throttler)
            remote_write_query_throttler = std::make_shared<Throttler>(bandwidth, throttler);
        throttler = remote_write_query_throttler;
    }
    return throttler;
}

ThrottlerPtr Context::getLocalReadThrottler() const
{
    ThrottlerPtr throttler;
    {
        std::lock_guard lock(shared->mutex);
        throttler = shared->local_read_throttler;
    }

    if (auto bandwidth = getSettingsRef()[Setting::max_local_read_bandwidth])
    {
        std::lock_guard lock(mutex);
        if (!local_read_query_throttler)
            local_read_query_throttler = std::make_shared<Throttler>(bandwidth, throttler);
        throttler = local_read_query_throttler;
    }
    return throttler;
}

ThrottlerPtr Context::getLocalWriteThrottler() const
{
    ThrottlerPtr throttler;
    {
        std::lock_guard lock(shared->mutex);
        throttler = shared->local_write_throttler;
    }

    if (auto bandwidth = getSettingsRef()[Setting::max_local_write_bandwidth])
    {
        std::lock_guard lock(mutex);
        if (!local_write_query_throttler)
            local_write_query_throttler = std::make_shared<Throttler>(bandwidth, throttler);
        throttler = local_write_query_throttler;
    }
    return throttler;
}

ThrottlerPtr Context::getBackupsThrottler() const
{
    ThrottlerPtr throttler = shared->backups_server_throttler;
    if (auto bandwidth = getSettingsRef()[Setting::max_backup_bandwidth])
    {
        std::lock_guard lock(mutex);
         if (!backups_query_throttler)
            backups_query_throttler = std::make_shared<Throttler>(bandwidth, throttler);
        throttler = backups_query_throttler;
    }
    return throttler;
}

ThrottlerPtr Context::getMutationsThrottler() const
{
    return shared->mutations_throttler;
}

ThrottlerPtr Context::getMergesThrottler() const
{
    return shared->merges_throttler;
}

void Context::reloadRemoteThrottlerConfig(size_t read_bandwidth, size_t write_bandwidth) const
{
    if (read_bandwidth)
    {
        std::lock_guard lock(shared->mutex);
        if (!shared->remote_read_throttler)
            shared->remote_read_throttler = std::make_shared<Throttler>(read_bandwidth);
    }

    if (shared->remote_read_throttler)
        shared->remote_read_throttler->setMaxSpeed(read_bandwidth);

    if (write_bandwidth)
    {
        std::lock_guard lock(shared->mutex);
        if (!shared->remote_write_throttler)
            shared->remote_write_throttler = std::make_shared<Throttler>(write_bandwidth);
    }

    if (shared->remote_write_throttler)
        shared->remote_write_throttler->setMaxSpeed(write_bandwidth);
}

void Context::reloadLocalThrottlerConfig(size_t read_bandwidth, size_t write_bandwidth) const
{
    if (read_bandwidth)
    {
        std::lock_guard lock(shared->mutex);
        if (!shared->local_read_throttler)
            shared->local_read_throttler = std::make_shared<Throttler>(read_bandwidth);
    }

    if (shared->local_read_throttler)
        shared->local_read_throttler->setMaxSpeed(read_bandwidth);

    if (write_bandwidth)
    {
        std::lock_guard lock(shared->mutex);
        if (!shared->local_write_throttler)
            shared->local_write_throttler = std::make_shared<Throttler>(write_bandwidth);
    }

    if (shared->local_write_throttler)
        shared->local_write_throttler->setMaxSpeed(write_bandwidth);
}

bool Context::hasDistributedDDL() const
{
    return getConfigRef().has("distributed_ddl");
}

void Context::setDDLWorker(std::unique_ptr<DDLWorker> ddl_worker, const LoadTaskPtrs & startup_after)
{
    std::lock_guard lock(shared->mutex);
    if (shared->ddl_worker)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "DDL background thread has already been initialized");

    shared->ddl_worker = std::move(ddl_worker);

    auto job = makeLoadJob(
        getGoals(startup_after),
        TablesLoaderBackgroundStartupPoolId,
        "startup ddl worker",
        [this] (AsyncLoader &, const LoadJobPtr &)
        {
            std::lock_guard lock2(shared->mutex);
            shared->ddl_worker->startup();
        });

    shared->ddl_worker_startup_task = makeLoadTask(getAsyncLoader(), {job});
    shared->ddl_worker_startup_task->schedule();
}

DDLWorker & Context::getDDLWorker() const
{
    // We have to ensure that DDL worker will not interfere with async loading of tables.
    // For example to prevent creation of a table that already exists, but has not been yet loaded.
    // So we have to wait for all tables to be loaded before starting up DDL worker.
    // NOTE: Possible improvement: above requirement can be loosen by waiting for specific tables to load.
    if (shared->ddl_worker_startup_task)
        waitLoad(shared->ddl_worker_startup_task); // Just wait and do not prioritize, because it depends on all load and startup tasks

    {
        /// Only acquire the lock for reading ddl_worker field.
        /// hasZooKeeper() and hasDistributedDDL() acquire the same lock as well and double acquisition of the lock in shared mode can lead
        /// to a deadlock if an exclusive lock attempt is made in the meantime by another thread.
        SharedLockGuard lock(shared->mutex);
        if (shared->ddl_worker)
            return *shared->ddl_worker;
    }

    if (!hasZooKeeper())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no Zookeeper configuration in server config");

    if (!hasDistributedDDL())
        throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "There is no DistributedDDL configuration in server config");

    throw Exception(ErrorCodes::NO_ELEMENTS_IN_CONFIG, "DDL background thread is not initialized");
}

zkutil::ZooKeeperPtr Context::getZooKeeper() const
{
    std::lock_guard lock(shared->zookeeper_mutex);

    const auto & config = shared->zookeeper_config ? *shared->zookeeper_config : getConfigRef();
    if (!shared->zookeeper)
        shared->zookeeper = zkutil::ZooKeeper::create(config, zkutil::getZooKeeperConfigName(config), getZooKeeperLog());

    if (shared->zookeeper->expired())
    {
        Stopwatch watch;
        LOG_DEBUG(shared->log, "Trying to establish a new connection with ZooKeeper");
        shared->zookeeper = shared->zookeeper->startNewSession();
        if (isServerCompletelyStarted())
            shared->zookeeper->setServerCompletelyStarted();
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
            if (isLocalAddress(DNSResolver::instance().resolveHostAllInOriginOrder(host).front()))
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
        const auto config_name = zkutil::getZooKeeperConfigName(getConfigRef());
        /// If our server is part of main Keeper cluster
        if (config_name == "keeper_server" || checkZooKeeperConfigIsLocal(getConfigRef(), config_name))
        {
            LOG_DEBUG(shared->log, "Keeper server is participant of the main zookeeper cluster, will try to connect to it");
            getZooKeeper();
            /// Connected, return true
            return true;
        }

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

    std::shared_ptr<ZooKeeperLog> zookeeper_log;
    {
        SharedLockGuard lock(shared->mutex);
        if (!shared->system_logs)
            return;

        zookeeper_log = shared->system_logs->zookeeper_log;
    }

    if (!zookeeper_log)
        return;

    {
        std::lock_guard lock(shared->zookeeper_mutex);
        if (shared->zookeeper)
            shared->zookeeper->setZooKeeperLog(zookeeper_log);
    }

    {
        std::lock_guard lock_auxiliary_zookeepers(shared->auxiliary_zookeepers_mutex);
        for (auto & zk : shared->auxiliary_zookeepers)
            zk.second->setZooKeeperLog(zookeeper_log);
    }
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
std::shared_ptr<KeeperDispatcher> Context::getKeeperDispatcher() const
{
    std::lock_guard lock(shared->keeper_dispatcher_mutex);
    if (!shared->keeper_dispatcher)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Keeper must be initialized before requests");

    return shared->keeper_dispatcher;
}

std::shared_ptr<KeeperDispatcher> Context::tryGetKeeperDispatcher() const
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
        if (name.contains(':') || name.contains('/'))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid auxiliary ZooKeeper name {}: ':' and '/' are not allowed", name);

        const auto & config = shared->auxiliary_zookeepers_config ? *shared->auxiliary_zookeepers_config : getConfigRef();
        if (!config.has("auxiliary_zookeepers." + name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown auxiliary ZooKeeper name '{}'. If it's required it can be added to the section <auxiliary_zookeepers> in "
                "config.xml",
                name);

        zookeeper = shared->auxiliary_zookeepers.emplace(name,
                        zkutil::ZooKeeper::create(config, "auxiliary_zookeepers." + name, getZooKeeperLog())).first;
    }
    else if (zookeeper->second->expired())
        zookeeper->second = zookeeper->second->startNewSession();

    return zookeeper->second;
}

std::shared_ptr<zkutil::ZooKeeper> Context::getDefaultOrAuxiliaryZooKeeper(const String & name) const
{
    return name == zkutil::DEFAULT_ZOOKEEPER_NAME ? getZooKeeper() : getAuxiliaryZooKeeper(name);
}


std::map<String, zkutil::ZooKeeperPtr> Context::getAuxiliaryZooKeepers() const
{
    std::lock_guard lock(shared->auxiliary_zookeepers_mutex);
    return shared->auxiliary_zookeepers;
}

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

        zk = zkutil::ZooKeeper::create(*config, config_name, std::move(zk_log));
        if (server_started)
            zk->setServerCompletelyStarted();
    }
}

void Context::reloadZooKeeperIfChanged(const ConfigurationPtr & config) const
{
    bool server_started = isServerCompletelyStarted();
    std::lock_guard lock(shared->zookeeper_mutex);
    shared->zookeeper_config = config;
    reloadZooKeeperIfChangedImpl(config, zkutil::getZooKeeperConfigName(*config), shared->zookeeper, getZooKeeperLog(), server_started);
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
    return zkutil::hasZooKeeperConfig(getConfigRef());
}

bool Context::hasAuxiliaryZooKeeper(const String & name) const
{
    return getConfigRef().has("auxiliary_zookeepers." + name);
}

void Context::reloadQueryMaskingRulesIfChanged(const ConfigurationPtr & config) const
{
    const auto old_config = shared->sensitive_data_masker_config;
    if (old_config && isSameConfiguration(*config, *old_config, "query_masking_rules"))
        return;

    SensitiveDataMasker::setInstance(std::make_unique<SensitiveDataMasker>(*config, "query_masking_rules"));
    shared->sensitive_data_masker_config = config;
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

void Context::setHTTPHeaderFilter(const Poco::Util::AbstractConfiguration & config)
{
    shared->http_header_filter.setValuesFromConfig(config);
}

const HTTPHeaderFilter & Context::getHTTPHeaderFilter() const
{
    return shared->http_header_filter;
}

UInt16 Context::getTCPPort() const
{
    const auto & config = getConfigRef();
    return config.getInt("tcp_port", DBMS_DEFAULT_PORT);
}

std::optional<UInt16> Context::getTCPPortSecure() const
{
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
        throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "There is no port named {}", port_name);
    return it->second;
}

size_t Context::getMaxPendingMutationsToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_pending_mutations_to_warn;
}

size_t Context::getMaxPendingMutationsExecutionTimeToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_pending_mutations_execution_time_to_warn;
}

size_t Context::getMaxPartNumToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_part_num_to_warn;
}

size_t Context::getMaxTableNumToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_table_num_to_warn;
}

size_t Context::getMaxViewNumToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_view_num_to_warn;
}

size_t Context::getMaxDictionaryNumToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_dictionary_num_to_warn;
}

size_t Context::getMaxDatabaseNumToWarn() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_database_num_to_warn;
}

void Context::setMaxPendingMutationsToWarn(size_t max_pending_mutations_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_pending_mutations_to_warn = max_pending_mutations_to_warn;
}

void Context::setMaxPendingMutationsExecutionTimeToWarn(size_t max_pending_mutations_execution_time_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_pending_mutations_execution_time_to_warn = max_pending_mutations_execution_time_to_warn;
}

void Context::setMaxPartNumToWarn(size_t max_part_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_part_num_to_warn = max_part_to_warn;
}

void Context::setMaxTableNumToWarn(size_t max_table_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_table_num_to_warn = max_table_to_warn;
}

void Context::setMaxViewNumToWarn(size_t max_view_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_view_num_to_warn = max_view_to_warn;
}

void Context::setMaxDictionaryNumToWarn(size_t max_dictionary_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_dictionary_num_to_warn = max_dictionary_to_warn;
}

void Context::setMaxDatabaseNumToWarn(size_t max_database_to_warn)
{
    SharedLockGuard lock(shared->mutex);
    shared->max_database_num_to_warn = max_database_to_warn;
}

double Context::getMinOSCPUWaitTimeRatioToDropConnection() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->min_os_cpu_wait_time_ratio_to_drop_connection;
}

double Context::getMaxOSCPUWaitTimeRatioToDropConnection() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->max_os_cpu_wait_time_ratio_to_drop_connection;
}

void Context::setOSCPUOverloadSettings(double min_os_cpu_wait_time_ratio_to_drop_connection, double max_os_cpu_wait_time_ratio_to_drop_connection)
{
    SharedLockGuard lock(shared->mutex);
    shared->min_os_cpu_wait_time_ratio_to_drop_connection = min_os_cpu_wait_time_ratio_to_drop_connection;
    shared->max_os_cpu_wait_time_ratio_to_drop_connection = max_os_cpu_wait_time_ratio_to_drop_connection;
}

std::shared_ptr<Cluster> Context::getCluster(const std::string & cluster_name) const
{
    if (auto res = tryGetCluster(cluster_name))
        return res;
    throw Exception(ErrorCodes::CLUSTER_DOESNT_EXIST, "Requested cluster '{}' not found", cluster_name);
}


std::shared_ptr<Cluster> Context::tryGetCluster(const std::string & cluster_name) const
{
    std::shared_ptr<Cluster> res = nullptr;

    {
        std::lock_guard lock(shared->clusters_mutex);
        res = getClustersImpl(lock)->getCluster(cluster_name);

        if (res == nullptr && shared->cluster_discovery)
            res = shared->cluster_discovery->getCluster(cluster_name);
    }

    if (res == nullptr && !cluster_name.empty())
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
        auto new_clusters = std::make_shared<Clusters>(config, *settings, getMacros());

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

std::map<String, ClusterPtr> Context::getClusters() const
{
    std::lock_guard lock(shared->clusters_mutex);

    auto clusters = getClustersImpl(lock)->getContainer();

    if (shared->cluster_discovery)
    {
        const auto & cluster_discovery_map = shared->cluster_discovery->getClusters();
        for (const auto & [name, cluster] : cluster_discovery_map)
            clusters.emplace(name, cluster);
    }
    return clusters;
}

std::shared_ptr<Clusters> Context::getClustersImpl(std::lock_guard<std::mutex> & /* lock */) const TSA_REQUIRES(shared->clusters_mutex)
{
    if (!shared->clusters)
    {
        const auto & config = shared->clusters_config ? *shared->clusters_config : getConfigRef();
        shared->clusters = std::make_shared<Clusters>(config, *settings, getMacros());
    }

    return shared->clusters;
}

void Context::startClusterDiscovery()
{
    std::lock_guard lock(shared->clusters_mutex);
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
        shared->cluster_discovery = std::make_unique<ClusterDiscovery>(*config, getGlobalContext(), getMacros());
    }

    /// Do not update clusters if this part of config wasn't changed.
    if (shared->clusters && isSameConfiguration(*config, *shared->clusters_config, config_name))
        return;

    auto old_clusters_config = shared->clusters_config;
    shared->clusters_config = config;

    if (!shared->clusters)
        shared->clusters = std::make_shared<Clusters>(*shared->clusters_config, *settings, getMacros(), config_name);
    else
        shared->clusters->updateClusters(*shared->clusters_config, *settings, config_name, old_clusters_config);

    ++shared->clusters_version;
}

size_t Context::getClustersVersion() const
{
    std::lock_guard lock(shared->clusters_mutex);
    return shared->clusters_version;
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
    /// It is required, because the initialization of system logs can be also
    /// triggered from another thread, that is launched while initializing the system logs,
    /// for example, system.filesystem_cache_log will be triggered by parts loading
    /// of any other table if it is stored on a disk with cache.
    callOnce(shared->system_logs_initialized, [&] {
        auto system_logs = std::make_unique<SystemLogs>(getGlobalContext(), getConfigRef());
        std::lock_guard lock(shared->mutex);
        shared->system_logs = std::move(system_logs);
    });
}

void Context::createTraceCollector()
{
    shared->createTraceCollector();
}

void Context::initializeTraceCollector()
{
    shared->initializeTraceCollector(getTraceLog());
}

/// Call after unexpected crash happen.
void Context::handleCrash() const
{
    std::lock_guard<std::mutex> lock(mutex_shared_context);
    if (!shared)
        return;

    SharedLockGuard lock2(shared->mutex);
    if (shared->system_logs)
        shared->system_logs->handleCrash();
}

bool Context::hasTraceCollector() const
{
    return shared->hasTraceCollector();
}


std::shared_ptr<QueryLog> Context::getQueryLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_log;
}

std::shared_ptr<QueryMetricLog> Context::getQueryMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_metric_log;
}

std::shared_ptr<QueryThreadLog> Context::getQueryThreadLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_thread_log;
}

std::shared_ptr<QueryViewsLog> Context::getQueryViewsLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->query_views_log;
}

std::shared_ptr<PartLog> Context::getPartLog(const String & part_database) const
{
    SharedLockGuard lock(shared->mutex);

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
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->trace_log;
}


std::shared_ptr<TextLog> Context::getTextLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->text_log;
}


std::shared_ptr<MetricLog> Context::getMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->metric_log;
}

std::shared_ptr<TransposedMetricLog> Context::getTransposedMetricLog() const
{
     SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->transposed_metric_log;
}


std::shared_ptr<LatencyLog> Context::getLatencyLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->latency_log;
}


std::shared_ptr<AsynchronousMetricLog> Context::getAsynchronousMetricLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_metric_log;
}


std::shared_ptr<OpenTelemetrySpanLog> Context::getOpenTelemetrySpanLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->opentelemetry_span_log;
}

std::shared_ptr<SessionLog> Context::getSessionLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->session_log;
}


std::shared_ptr<ZooKeeperLog> Context::getZooKeeperLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->zookeeper_log;
}


std::shared_ptr<TransactionsInfoLog> Context::getTransactionsInfoLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->transactions_info_log;
}


std::shared_ptr<ProcessorsProfileLog> Context::getProcessorsProfileLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->processors_profile_log;
}

std::shared_ptr<FilesystemCacheLog> Context::getFilesystemCacheLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->filesystem_cache_log;
}

std::shared_ptr<ObjectStorageQueueLog> Context::getS3QueueLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->s3queue_log;
}

std::shared_ptr<ObjectStorageQueueLog> Context::getAzureQueueLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->azure_queue_log;
}

std::shared_ptr<FilesystemReadPrefetchesLog> Context::getFilesystemReadPrefetchesLog() const
{
    SharedLockGuard lock(shared->mutex);
    if (!shared->system_logs)
        return {};

    return shared->system_logs->filesystem_read_prefetches_log;
}

std::shared_ptr<AsynchronousInsertLog> Context::getAsynchronousInsertLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->asynchronous_insert_log;
}

std::shared_ptr<BackupLog> Context::getBackupLog() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};

    return shared->system_logs->backup_log;
}

std::shared_ptr<BlobStorageLog> Context::getBlobStorageLog() const
{
    bool enable_blob_storage_log = getSettingsRef()[Setting::enable_blob_storage_log];
    if (hasQueryContext())
        enable_blob_storage_log = getQueryContext()->getSettingsRef()[Setting::enable_blob_storage_log];

    if (!enable_blob_storage_log)
        return {};

    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};
    return shared->system_logs->blob_storage_log;
}

SystemLogs Context::getSystemLogs() const
{
    SharedLockGuard lock(shared->mutex);

    if (!shared->system_logs)
        return {};
    return *shared->system_logs;
}

std::optional<Context::Dashboards> Context::getDashboards() const
{
    std::lock_guard lock(shared->dashboard_mutex);

    if (!shared->dashboards)
        return {};
    return shared->dashboards;
}

namespace
{

String trim(const String & text)
{
    std::string_view view(text);
    ::trim(view, '\n');
    return String(view);
}

}

void Context::setDashboardsConfig(const ConfigurationPtr & config)
{
    Poco::Util::AbstractConfiguration::Keys keys;
    config->keys("dashboards", keys);

    Dashboards dashboards;
    for (const auto & key : keys)
    {
        const auto & prefix = "dashboards." + key + ".";
        dashboards.push_back({
            { "dashboard", config->getString(prefix + "dashboard") },
            { "title",     config->getString(prefix + "title") },
            { "query",     trim(config->getString(prefix + "query")) },
        });
    }

    {
        std::lock_guard lock(shared->dashboard_mutex);
        if (!dashboards.empty())
            shared->dashboards.emplace(std::move(dashboards));
        else
            shared->dashboards.reset();
    }
}

CompressionCodecPtr Context::chooseCompressionCodec(size_t part_size, double part_size_ratio) const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->compression_codec_selector)
    {
        constexpr auto config_name = "compression";
        const auto & config = shared->getConfigRefWithLock(lock);

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

StoragePolicyPtr Context::getOrCreateStoragePolicy(const String & name, StoragePolicyCreator creator) const
{
    std::lock_guard lock(shared->storage_policies_mutex);

    auto storage_policy_selector = getStoragePolicySelector(lock);

    auto storage_policy = storage_policy_selector->tryGet(name);
    if (!storage_policy)
    {
        storage_policy = creator(storage_policy_selector->getPoliciesMap());
        const_cast<StoragePolicySelector *>(storage_policy_selector.get())->add(storage_policy);
    }

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

DiskSelectorPtr Context::getDiskSelector(std::lock_guard<std::mutex> & /* lock */) const TSA_REQUIRES(shared->storage_policies_mutex)
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

StoragePolicySelectorPtr Context::getStoragePolicySelector(std::lock_guard<std::mutex> & lock) const TSA_REQUIRES(shared->storage_policies_mutex)
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
    {
        std::lock_guard lock(shared->storage_policies_mutex);
        Strings disks_to_reinit;
        if (shared->merge_tree_disk_selector)
            shared->merge_tree_disk_selector
                = shared->merge_tree_disk_selector->updateFromConfig(config, "storage_configuration.disks", shared_from_this());

        if (shared->merge_tree_storage_policy_selector)
        {
            try
            {
                shared->merge_tree_storage_policy_selector = shared->merge_tree_storage_policy_selector->updateFromConfig(
                    config, "storage_configuration.policies", shared->merge_tree_disk_selector, disks_to_reinit);
            }
            catch (Exception & e)
            {
                LOG_ERROR(
                    shared->log, "An error has occurred while reloading storage policies, storage policies were not applied: {}", e.message());
            }
        }

        if (!disks_to_reinit.empty())
        {
            LOG_INFO(shared->log, "Initializing disks: ({}) for all tables", fmt::join(disks_to_reinit, ", "));
            DatabaseCatalog::instance().triggerReloadDisksTask(disks_to_reinit);
        }
    }

    {
        std::lock_guard lock(shared->mutex);
        if (shared->storage_s3_settings)
            shared->storage_s3_settings->loadFromConfig(config, /* config_prefix */"s3", getSettingsRef());
    }

    {
        std::lock_guard lock(shared->mutex);
        if (shared->storage_azure_settings)
            shared->storage_azure_settings->loadFromConfig(config, /* config_prefix */"configuration.disks.", getSettingsRef());
    }

}


const MergeTreeSettings & Context::getMergeTreeSettings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->merge_tree_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        MergeTreeSettings mt_settings;

        /// Respect compatibility setting from the default profile.
        /// First, we apply compatibility values, and only after apply changes from the config.
        mt_settings.applyCompatibilitySetting((*settings)[Setting::compatibility]);

        mt_settings.loadFromConfig("merge_tree", config);
        shared->merge_tree_settings.emplace(mt_settings);
    }

    return *shared->merge_tree_settings;
}

const MergeTreeSettings & Context::getReplicatedMergeTreeSettings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->replicated_merge_tree_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        MergeTreeSettings mt_settings;

        /// Respect compatibility setting from the default profile.
        /// First, we apply compatibility values, and only after apply changes from the config.
        mt_settings.applyCompatibilitySetting((*settings)[Setting::compatibility]);

        mt_settings.loadFromConfig("merge_tree", config);
        mt_settings.loadFromConfig("replicated_merge_tree", config);
        shared->replicated_merge_tree_settings.emplace(mt_settings);
    }

    return *shared->replicated_merge_tree_settings;
}

const DistributedSettings & Context::getDistributedSettings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->distributed_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        DistributedSettings distributed_settings;
        distributed_settings.loadFromConfig("distributed", config);
        shared->distributed_settings.emplace(distributed_settings);
    }

    return *shared->distributed_settings;
}

const S3SettingsByEndpoint & Context::getStorageS3Settings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->storage_s3_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        shared->storage_s3_settings.emplace().loadFromConfig(config, "s3", getSettingsRef());
    }

    return *shared->storage_s3_settings;
}

const AzureSettingsByEndpoint & Context::getStorageAzureSettings() const
{
    std::lock_guard lock(shared->mutex);

    if (!shared->storage_azure_settings)
    {
        const auto & config = shared->getConfigRefWithLock(lock);
        shared->storage_azure_settings.emplace().loadFromConfig(config, "storage_configuration.disks", getSettingsRef());
    }

    return *shared->storage_azure_settings;
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
                    "2. Either pass a bigger (or set to zero) max_[table/partition]_size_to_drop through query settings\n"
                    "3. Either create forcing file {} and make sure that ClickHouse has write permission for it.\n"
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

size_t Context::getMaxTableSizeToDrop() const
{
    return shared->max_table_size_to_drop.load();
}

void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size) const
{
    size_t max_table_size_to_drop = shared->max_table_size_to_drop.load();

    checkCanBeDropped(database, table, table_size, max_table_size_to_drop);
}

void Context::checkTableCanBeDropped(const String & database, const String & table, const size_t & table_size, const size_t & max_table_size_to_drop) const
{
    checkCanBeDropped(database, table, table_size, max_table_size_to_drop);
}

void Context::setMaxPartitionSizeToDrop(size_t max_size)
{
    // Is initialized at server startup and updated at config reload
    shared->max_partition_size_to_drop.store(max_size, std::memory_order_relaxed);
}

size_t Context::getMaxPartitionSizeToDrop() const
{
    return shared->max_partition_size_to_drop.load();
}

void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size) const
{
    size_t max_partition_size_to_drop = shared->max_partition_size_to_drop.load();

    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}

void Context::checkPartitionCanBeDropped(const String & database, const String & table, const size_t & partition_size, const size_t & max_partition_size_to_drop) const
{
    checkCanBeDropped(database, table, partition_size, max_partition_size_to_drop);
}

void Context::setConfigReloaderInterval(size_t value_ms)
{
    shared->config_reload_interval_ms.store(value_ms, std::memory_order_relaxed);
}

size_t Context::getConfigReloaderInterval() const
{
    return shared->config_reload_interval_ms.load(std::memory_order_relaxed);
}

InputFormatPtr Context::getInputFormat(const String & name, ReadBuffer & buf, const Block & sample, UInt64 max_block_size, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getInput(name, buf, sample, shared_from_this(), max_block_size, format_settings);
}

OutputFormatPtr Context::getOutputFormat(const String & name, WriteBuffer & buf, const Block & sample, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getOutputFormat(name, buf, sample, shared_from_this(), format_settings);
}

OutputFormatPtr Context::getOutputFormatParallelIfPossible(const String & name, WriteBuffer & buf, const Block & sample, const std::optional<FormatSettings> & format_settings) const
{
    return FormatFactory::instance().getOutputFormatParallelIfPossible(name, buf, sample, shared_from_this(), format_settings);
}


double Context::getUptimeSeconds() const
{
    SharedLockGuard lock(shared->mutex);
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

void Context::setStartServersCallback(StartStopServersCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->start_servers_callback = std::move(callback);
}

void Context::setStopServersCallback(StartStopServersCallback && callback)
{
    /// Is initialized at server startup, so lock isn't required. Otherwise use mutex.
    shared->stop_servers_callback = std::move(callback);
}

void Context::startServers(const ServerType & server_type) const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->start_servers_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't start servers because start_servers_callback is not set.");

    shared->start_servers_callback(server_type);
}

void Context::stopServers(const ServerType & server_type) const
{
    /// Use mutex if callback may be changed after startup.
    if (!shared->stop_servers_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't stop servers because stop_servers_callback is not set.");

    shared->stop_servers_callback(server_type);
}


void Context::shutdown() TSA_NO_THREAD_SAFETY_ANALYSIS
{
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

    if (type == ApplicationType::LOCAL || type == ApplicationType::SERVER || type == ApplicationType::DISKS)
        shared->server_settings.loadSettingsFromConfig(Poco::Util::Application::instance().config());

    if (type == ApplicationType::SERVER)
        shared->configureServerWideThrottling();
}

void Context::setDefaultProfiles(const Poco::Util::AbstractConfiguration & config)
{
    shared->default_profile_name = config.getString("default_profile", "default");
    getAccessControl().setDefaultProfileName(shared->default_profile_name);

    shared->system_profile_name = config.getString("system_profile", shared->default_profile_name);

    /// Don't check for constraints on first load. This makes the default profile consistent with other users, where
    /// the default value set in the config might be outside of the constraints range
    /// It makes it possible to change the value of experimental settings with `allow_feature_tier` != 2
    bool check_constraints = false;
    setCurrentProfile(shared->system_profile_name, check_constraints);

    applySettingsQuirks(*settings, getLogger("SettingsQuirks"));
    doSettingsSanityCheckClamp(*settings, getLogger("SettingsSanity"));

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

String Context::getGoogleProtosPath() const
{
    return shared->google_protos_path;
}

void Context::setGoogleProtosPath(const String & path)
{
    shared->google_protos_path = path;
}

std::pair<Context::SampleBlockCache *, std::unique_lock<std::mutex>> Context::getSampleBlockCache() const
{
    chassert(hasQueryContext());
    return std::make_pair(&getQueryContext()->sample_block_cache, std::unique_lock(getQueryContext()->sample_block_cache_mutex));
}

std::pair<Context::StorageMetadataCache *, std::unique_lock<std::mutex>> Context::getStorageMetadataCache() const
{
    chassert(hasQueryContext());
    return std::make_pair(&getQueryContext()->storage_metadata_cache, std::unique_lock(getQueryContext()->storage_metadata_cache_mutex));
}

std::pair<Context::StorageSnapshotCache *, std::unique_lock<std::mutex>> Context::getStorageSnapshotCache() const
{
    chassert(hasQueryContext());
    return std::make_pair(&getQueryContext()->storage_snapshot_cache, std::unique_lock(getQueryContext()->storage_snapshot_cache_mutex));
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
    callOnce(shared->action_locks_manager_initialized, [&] {
        shared->action_locks_manager = std::make_shared<ActionLocksManager>(shared_from_this());
    });

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

void Context::setQueryPlanDeserializationCallback(QueryPlanDeserializationCallback && callback)
{
    query_plan_deserialization_callback = std::move(callback);
}

std::shared_ptr<QueryPlanAndSets> Context::getDeserializedQueryPlan()
{
    if (!query_plan_deserialization_callback)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Query plan deserialization callback is not set");

    return query_plan_deserialization_callback();
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


void Context::setClientInfo(const ClientInfo & client_info_)
{
    client_info = client_info_;
    need_recalculate_access = true;
}

void Context::setClientName(const String & client_name)
{
    client_info.client_name = client_name;
}

void Context::setClientInterface(ClientInfo::Interface interface)
{
    client_info.interface = interface;
    need_recalculate_access = true;
}

void Context::setClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version)
{
    client_info.client_version_major = client_version_major;
    client_info.client_version_minor = client_version_minor;
    client_info.client_version_patch = client_version_patch;
    client_info.client_tcp_protocol_version = client_tcp_protocol_version;
}

void Context::setScriptQueryAndLineNumber(uint32_t query_number, uint32_t line_number)
{
    client_info.script_query_number = query_number;
    client_info.script_line_number = line_number;
}

void Context::setClientConnectionId(uint32_t connection_id_)
{
    client_info.connection_id = connection_id_;
}

void Context::setHTTPClientInfo(const Poco::Net::HTTPRequest & request)
{
    client_info.setFromHTTPRequest(request);
    need_recalculate_access = true;
}

void Context::setForwardedFor(const String & forwarded_for)
{
    client_info.forwarded_for = forwarded_for;
    need_recalculate_access = true;
}

void Context::setQueryKind(ClientInfo::QueryKind query_kind)
{
    client_info.query_kind = query_kind;
}

void Context::setQueryKindInitial()
{
    /// TODO: Try to combine this function with setQueryKind().
    client_info.setInitialQuery();
}

void Context::setQueryKindReplicatedDatabaseInternal()
{
    /// TODO: Try to combine this function with setQueryKind().
    client_info.is_replicated_database_internal = true;
}

void Context::setCurrentUserName(const String & current_user_name)
{
    /// TODO: Try to combine this function with setUser().
    client_info.current_user = current_user_name;
    need_recalculate_access = true;
}

void Context::setCurrentAddress(const Poco::Net::SocketAddress & current_address)
{
    client_info.current_address = std::make_shared<Poco::Net::SocketAddress>(current_address);
    need_recalculate_access = true;
}

void Context::setInitialUserName(const String & initial_user_name)
{
    client_info.initial_user = initial_user_name;
    need_recalculate_access = true;
}

void Context::setInitialAddress(const Poco::Net::SocketAddress & initial_address)
{
    client_info.initial_address = std::make_shared<Poco::Net::SocketAddress>(initial_address);
}

void Context::setInitialQueryId(const String & initial_query_id)
{
    client_info.initial_query_id = initial_query_id;
}

void Context::setInitialQueryStartTime(std::chrono::time_point<std::chrono::system_clock> initial_query_start_time)
{
    client_info.initial_query_start_time = timeInSeconds(initial_query_start_time);
    client_info.initial_query_start_time_microseconds = timeInMicroseconds(initial_query_start_time);
}

void Context::setQuotaClientKey(const String & quota_key_)
{
    client_info.quota_key = quota_key_;
    need_recalculate_access = true;
}

void Context::setConnectionClientVersion(UInt64 client_version_major, UInt64 client_version_minor, UInt64 client_version_patch, unsigned client_tcp_protocol_version)
{
    client_info.connection_client_version_major = client_version_major;
    client_info.connection_client_version_minor = client_version_minor;
    client_info.connection_client_version_patch = client_version_patch;
    client_info.connection_tcp_protocol_version = client_tcp_protocol_version;
}

void Context::increaseDistributedDepth()
{
    ++client_info.distributed_depth;
}


StorageID Context::resolveStorageID(StorageID storage_id, StorageNamespace where) const
{
    if (storage_id.uuid != UUIDHelpers::Nil)
        return storage_id;

    StorageID resolved = StorageID::createEmpty();
    std::optional<Exception> exc;
    {
        SharedLockGuard lock(mutex);
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
        SharedLockGuard lock(mutex);
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
            exception->emplace(Exception(ErrorCodes::UNKNOWN_TABLE, "Both table name and UUID are empty"));
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
                exception->emplace(Exception(ErrorCodes::UNKNOWN_DATABASE, "Default database is not selected"));
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

void Context::setParentTable(UUID uuid)
{
    chassert(!parent_table_uuid.has_value());
    parent_table_uuid = uuid;
}

std::optional<UUID> Context::getParentTable() const
{
    return parent_table_uuid;
}

void Context::setDDLQueryCancellation(StopToken cancel)
{
    chassert(!ddl_query_cancellation.stop_possible());
    ddl_query_cancellation = cancel;
}

StopToken Context::getDDLQueryCancellation() const
{
    return ddl_query_cancellation;
}

void Context::setDDLAdditionalChecksOnEnqueue(Coordination::Requests requests)
{
    ddl_additional_checks_on_enqueue = requests;
}

Coordination::Requests Context::getDDLAdditionalChecksOnEnqueue() const
{
    return ddl_additional_checks_on_enqueue;
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
    SharedLockGuard lock(shared->mutex);
    assert(getApplicationType() == ApplicationType::SERVER);
    return shared->is_server_completely_started;
}

void Context::setServerCompletelyStarted()
{
    {
        {
            std::lock_guard lock(shared->zookeeper_mutex);
            if (shared->zookeeper)
                shared->zookeeper->setServerCompletelyStarted();
        }

        {
            std::lock_guard lock(shared->auxiliary_zookeepers_mutex);
            for (auto & zk : shared->auxiliary_zookeepers)
                zk.second->setServerCompletelyStarted();
        }
    }

    std::lock_guard lock(shared->mutex);
    assert(global_context.lock().get() == this);
    assert(!shared->is_server_completely_started);
    assert(getApplicationType() == ApplicationType::SERVER);
    shared->is_server_completely_started = true;
}

PartUUIDsPtr Context::getPartUUIDs() const
{
    std::lock_guard lock(mutex);

    if (!part_uuids)
        /// For context itself, only this initialization is not const.
        /// We could have done in constructor.
        /// TODO: probably, remove this from Context.
        const_cast<PartUUIDsPtr &>(part_uuids) = std::make_shared<PartUUIDs>();

    return part_uuids;
}


ClusterFunctionReadTaskCallback Context::getClusterFunctionReadTaskCallback() const
{
    if (!next_task_callback.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Next task callback is not set for query {}", getInitialQueryId());
    return next_task_callback.value();
}


void Context::setClusterFunctionReadTaskCallback(ClusterFunctionReadTaskCallback && callback)
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

BlockMarshallingCallback Context::getBlockMarshallingCallback() const
{
    return block_marshalling_callback;
}

void Context::setBlockMarshallingCallback(BlockMarshallingCallback && callback)
{
    block_marshalling_callback = std::move(callback);
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
    std::lock_guard lock(mutex);
    if (!ignored_part_uuids)
        const_cast<PartUUIDsPtr &>(ignored_part_uuids) = std::make_shared<PartUUIDs>();

    return ignored_part_uuids;
}

AsynchronousInsertQueue * Context::tryGetAsynchronousInsertQueue() const
{
    SharedLockGuard lock(shared->mutex);
    return shared->async_insert_queue.get();
}

void Context::setAsynchronousInsertQueue(const std::shared_ptr<AsynchronousInsertQueue> & ptr)
{
    AsynchronousInsertQueue::validateSettings(*settings, getLogger("Context"));

    SharedLockGuard lock(shared->mutex);

    if (std::chrono::milliseconds(getSettingsRef()[Setting::async_insert_poll_timeout_ms]) == std::chrono::milliseconds::zero())
        throw Exception(ErrorCodes::INVALID_SETTING_VALUE, "Setting async_insert_poll_timeout_ms can't be zero");

    shared->async_insert_queue = ptr;
}

void Context::initializeBackgroundExecutorsIfNeeded()
{
    std::lock_guard lock(shared->background_executors_mutex);

    if (shared->are_background_executors_initialized)
        return;

    const ServerSettings & server_settings = shared->server_settings;
    size_t background_pool_size = server_settings[ServerSetting::background_pool_size];
    auto background_merges_mutations_concurrency_ratio = server_settings[ServerSetting::background_merges_mutations_concurrency_ratio];
    size_t background_pool_max_tasks_count = static_cast<size_t>(background_pool_size * background_merges_mutations_concurrency_ratio);
    String background_merges_mutations_scheduling_policy = server_settings[ServerSetting::background_merges_mutations_scheduling_policy];
    size_t background_move_pool_size = server_settings[ServerSetting::background_move_pool_size];
    size_t background_fetches_pool_size = server_settings[ServerSetting::background_fetches_pool_size];
    size_t background_common_pool_size = server_settings[ServerSetting::background_common_pool_size];

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

bool Context::areBackgroundExecutorsInitialized() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->are_background_executors_initialized;
}

MergeMutateBackgroundExecutorPtr Context::getMergeMutateExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->merge_mutate_executor;
}

OrdinaryBackgroundExecutorPtr Context::getMovesExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->moves_executor;
}

OrdinaryBackgroundExecutorPtr Context::getFetchesExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->fetch_executor;
}

OrdinaryBackgroundExecutorPtr Context::getCommonExecutor() const
{
    SharedLockGuard lock(shared->background_executors_mutex);
    return shared->common_executor;
}

IAsynchronousReader & Context::getThreadPoolReader(FilesystemReaderType type) const
{
    callOnce(shared->readers_initialized, [&] {
        const auto & config = getConfigRef();
        shared->asynchronous_remote_fs_reader = createThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER, config);
        shared->asynchronous_local_fs_reader = createThreadPoolReader(FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER, config);
        shared->synchronous_local_fs_reader = createThreadPoolReader(FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER, config);
    });

    switch (type)
    {
        case FilesystemReaderType::ASYNCHRONOUS_REMOTE_FS_READER:
            return *shared->asynchronous_remote_fs_reader;
        case FilesystemReaderType::ASYNCHRONOUS_LOCAL_FS_READER:
            return *shared->asynchronous_local_fs_reader;
        case FilesystemReaderType::SYNCHRONOUS_LOCAL_FS_READER:
            return *shared->synchronous_local_fs_reader;
    }
}

#if USE_LIBURING
IOUringReader & Context::getIOUringReader() const
{
    callOnce(shared->io_uring_reader_initialized, [&] {
        shared->io_uring_reader = createIOUringReader();
    });

    return *shared->io_uring_reader;
}
#endif

ThreadPool & Context::getThreadPoolWriter() const
{
    callOnce(shared->threadpool_writer_initialized, [&] {
        auto pool_size = shared->server_settings[ServerSetting::threadpool_writer_pool_size];
        auto queue_size = shared->server_settings[ServerSetting::threadpool_writer_queue_size];

        shared->threadpool_writer = std::make_unique<ThreadPool>(
            CurrentMetrics::IOWriterThreads, CurrentMetrics::IOWriterThreadsActive, CurrentMetrics::IOWriterThreadsScheduled, pool_size, pool_size, queue_size);
    });

    return *shared->threadpool_writer;
}

ReadSettings Context::getReadSettings() const
{
    ReadSettings res;
    const auto & settings_ref = getSettingsRef();

    std::string_view read_method_str = getSettingsRef()[Setting::local_filesystem_read_method].value;

    if (auto opt_method = magic_enum::enum_cast<LocalFSReadMethod>(read_method_str))
        res.local_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for local filesystem", read_method_str);

    read_method_str = getSettingsRef()[Setting::remote_filesystem_read_method].value;

    if (auto opt_method = magic_enum::enum_cast<RemoteFSReadMethod>(read_method_str))
        res.remote_fs_method = *opt_method;
    else
        throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}' for remote filesystem", read_method_str);

    res.local_fs_prefetch = settings_ref[Setting::local_filesystem_read_prefetch];
    res.remote_fs_prefetch = settings_ref[Setting::remote_filesystem_read_prefetch];

    res.load_marks_asynchronously = settings_ref[Setting::load_marks_asynchronously];

    res.enable_filesystem_read_prefetches_log = settings_ref[Setting::enable_filesystem_read_prefetches_log];

    res.remote_fs_read_max_backoff_ms = settings_ref[Setting::remote_fs_read_max_backoff_ms];
    res.remote_fs_read_backoff_max_tries = settings_ref[Setting::remote_fs_read_backoff_max_tries];
    res.enable_filesystem_cache = settings_ref[Setting::enable_filesystem_cache];
    res.read_from_filesystem_cache_if_exists_otherwise_bypass_cache
        = settings_ref[Setting::read_from_filesystem_cache_if_exists_otherwise_bypass_cache];
    res.enable_filesystem_cache_log = settings_ref[Setting::enable_filesystem_cache_log];
    res.filesystem_cache_segments_batch_size = settings_ref[Setting::filesystem_cache_segments_batch_size];
    res.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds
        = settings_ref[Setting::filesystem_cache_reserve_space_wait_lock_timeout_milliseconds];
    res.filesystem_cache_allow_background_download_for_metadata_files_in_packed_storage
        = settings_ref[Setting::filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage];
    res.filesystem_cache_allow_background_download_during_fetch = settings_ref[Setting::filesystem_cache_enable_background_download_during_fetch];
    res.filesystem_cache_prefer_bigger_buffer_size = settings_ref[Setting::filesystem_cache_prefer_bigger_buffer_size];

    res.filesystem_cache_max_download_size = settings_ref[Setting::filesystem_cache_max_download_size];
    res.filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit = settings_ref[Setting::filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit];

    res.page_cache = getPageCache();
    res.use_page_cache_for_disks_without_file_cache = settings_ref[Setting::use_page_cache_for_disks_without_file_cache];
    res.use_page_cache_with_distributed_cache = settings_ref[Setting::use_page_cache_with_distributed_cache];
    res.read_from_page_cache_if_exists_otherwise_bypass_cache = settings_ref[Setting::read_from_page_cache_if_exists_otherwise_bypass_cache];
    res.page_cache_inject_eviction = settings_ref[Setting::page_cache_inject_eviction];
    res.page_cache_block_size = settings_ref[Setting::page_cache_block_size];
    res.page_cache_lookahead_blocks = settings_ref[Setting::page_cache_lookahead_blocks];

    res.remote_read_min_bytes_for_seek = getSettingsRef()[Setting::remote_read_min_bytes_for_seek];

    /// Zero read buffer will not make progress.
    if (!getSettingsRef()[Setting::max_read_buffer_size])
    {
        throw Exception(
            ErrorCodes::INVALID_SETTING_VALUE, "Invalid value '{}' for max_read_buffer_size", getSettingsRef()[Setting::max_read_buffer_size].value);
    }

    res.local_fs_buffer_size
        = settings_ref[Setting::max_read_buffer_size_local_fs] ? settings_ref[Setting::max_read_buffer_size_local_fs] : settings_ref[Setting::max_read_buffer_size];
    res.remote_fs_buffer_size
        = settings_ref[Setting::max_read_buffer_size_remote_fs] ? settings_ref[Setting::max_read_buffer_size_remote_fs] : settings_ref[Setting::max_read_buffer_size];
    res.prefetch_buffer_size = settings_ref[Setting::prefetch_buffer_size];
    res.direct_io_threshold = settings_ref[Setting::min_bytes_to_use_direct_io];
    res.mmap_threshold = settings_ref[Setting::min_bytes_to_use_mmap_io];
    res.priority = Priority{settings_ref[Setting::read_priority]};

    res.remote_throttler = getRemoteReadThrottler();
    res.local_throttler = getLocalReadThrottler();

    res.http_max_tries = settings_ref[Setting::http_max_tries];
    res.http_retry_initial_backoff_ms = settings_ref[Setting::http_retry_initial_backoff_ms];
    res.http_retry_max_backoff_ms = settings_ref[Setting::http_retry_max_backoff_ms];
    res.http_skip_not_found_url_for_globs = settings_ref[Setting::http_skip_not_found_url_for_globs];
    res.http_make_head_request = settings_ref[Setting::http_make_head_request];

    res.mmap_cache = getMMappedFileCache().get();
    res.enable_hdfs_pread = settings_ref[Setting::enable_hdfs_pread];

    return res;
}

WriteSettings Context::getWriteSettings() const
{
    WriteSettings res;
    const auto & settings_ref = getSettingsRef();

    res.enable_filesystem_cache_on_write_operations = settings_ref[Setting::enable_filesystem_cache_on_write_operations];
    res.enable_filesystem_cache_log = settings_ref[Setting::enable_filesystem_cache_log];
    res.throw_on_error_from_cache = settings_ref[Setting::throw_on_error_from_cache_on_write_operations];
    res.filesystem_cache_reserve_space_wait_lock_timeout_milliseconds
        = settings_ref[Setting::filesystem_cache_reserve_space_wait_lock_timeout_milliseconds];

    res.s3_allow_parallel_part_upload = settings_ref[Setting::s3_allow_parallel_part_upload];
    res.azure_allow_parallel_part_upload = settings_ref[Setting::azure_allow_parallel_part_upload];

    res.remote_throttler = getRemoteWriteThrottler();
    res.local_throttler = getLocalWriteThrottler();

    return res;
}

std::shared_ptr<AsyncReadCounters> Context::getAsyncReadCounters() const
{
    std::lock_guard lock(mutex);
    if (!async_read_counters)
        async_read_counters = std::make_shared<AsyncReadCounters>();
    return async_read_counters;
}

bool Context::canUseTaskBasedParallelReplicas() const
{
    const auto & settings_ref = getSettingsRef();

    if (!settings_ref[Setting::allow_experimental_analyzer] && settings_ref[Setting::parallel_replicas_only_with_analyzer])
        return false;

    return settings_ref[Setting::allow_experimental_parallel_reading_from_replicas] > 0
        && settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::READ_TASKS
        && settings_ref[Setting::max_parallel_replicas] > 1;
}

bool Context::canUseParallelReplicasOnInitiator() const
{
    return canUseTaskBasedParallelReplicas() && !getClientInfo().collaborate_with_initiator;
}

bool Context::canUseParallelReplicasOnFollower() const
{
    return canUseTaskBasedParallelReplicas() && getClientInfo().collaborate_with_initiator;
}

bool Context::canUseParallelReplicasCustomKey() const
{
    const auto & settings_ref = getSettingsRef();

    const bool has_enough_servers = settings_ref[Setting::max_parallel_replicas] > 1;
    const bool parallel_replicas_enabled = settings_ref[Setting::allow_experimental_parallel_reading_from_replicas] > 0;
    const bool is_parallel_replicas_with_custom_key =
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_SAMPLING ||
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_RANGE;

    return has_enough_servers && parallel_replicas_enabled && is_parallel_replicas_with_custom_key;
}

bool Context::canUseParallelReplicasCustomKeyForCluster(const Cluster & cluster) const
{
    return canUseParallelReplicasCustomKey() && cluster.getShardCount() == 1 && cluster.getShardsInfo()[0].getAllNodeCount() > 1;
}

bool Context::canUseOffsetParallelReplicas() const
{
    const auto & settings_ref = getSettingsRef();

    /**
     * Offset parallel replicas algorithm is not only the one which relies on native SAMPLING KEY,
     * but also those which rely on customer-provided "custom" key.
     * We combine them together into one group for convenience.
     */
    const bool has_enough_servers = settings_ref[Setting::max_parallel_replicas] > 1;
    const bool parallel_replicas_enabled = settings_ref[Setting::allow_experimental_parallel_reading_from_replicas] > 0;
    const bool is_parallel_replicas_with_custom_key_or_native_sampling_key =
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::SAMPLING_KEY ||
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_SAMPLING ||
        settings_ref[Setting::parallel_replicas_mode] == ParallelReplicasMode::CUSTOM_KEY_RANGE;
    return offset_parallel_replicas_enabled &&
           has_enough_servers &&
           parallel_replicas_enabled &&
           is_parallel_replicas_with_custom_key_or_native_sampling_key;
}

void Context::disableOffsetParallelReplicas()
{
    offset_parallel_replicas_enabled = false;
}

ClusterPtr Context::getClusterForParallelReplicas() const
{
    const auto & settings_ref = getSettingsRef();
    /// check cluster for parallel replicas
    if (settings_ref[Setting::cluster_for_parallel_replicas].value.empty())
        throw Exception(
            ErrorCodes::CLUSTER_DOESNT_EXIST,
            "Reading in parallel from replicas is enabled but cluster to execute query is not provided. Please set "
            "'cluster_for_parallel_replicas' setting");

    return getCluster(settings_ref[Setting::cluster_for_parallel_replicas]);
}

void Context::setPreparedSetsCache(const PreparedSetsCachePtr & cache)
{
    prepared_sets_cache = cache;
}

PreparedSetsCachePtr Context::getPreparedSetsCache() const
{
    return prepared_sets_cache;
}

UInt64 Context::getClientProtocolVersion() const
{
    return client_protocol_version;
}

void Context::setClientProtocolVersion(UInt64 version)
{
    client_protocol_version = version;
}

void Context::setPartitionIdToMaxBlock(PartitionIdToMaxBlockPtr partitions)
{
    partition_id_to_max_block = std::move(partitions);
}

PartitionIdToMaxBlockPtr Context::getPartitionIdToMaxBlock() const
{
    return partition_id_to_max_block;
}

const ServerSettings & Context::getServerSettings() const
{
    return shared->server_settings;
}

uint64_t HTTPContext::getMaxHstsAge() const
{
    return context->getSettingsRef()[Setting::hsts_max_age];
}

    uint64_t HTTPContext::getMaxUriSize() const
{
    return context->getSettingsRef()[Setting::http_max_uri_size];
}

uint64_t HTTPContext::getMaxFields() const
{
    return context->getSettingsRef()[Setting::http_max_fields];
}

uint64_t HTTPContext::getMaxFieldNameSize() const
{
    return context->getSettingsRef()[Setting::http_max_field_name_size];
}

uint64_t HTTPContext::getMaxFieldValueSize() const
{
    return context->getSettingsRef()[Setting::http_max_field_value_size];
}

Poco::Timespan HTTPContext::getReceiveTimeout() const
{
    return context->getSettingsRef()[Setting::http_receive_timeout];
}

Poco::Timespan HTTPContext::getSendTimeout() const
{
    return context->getSettingsRef()[Setting::http_send_timeout];
}

}
