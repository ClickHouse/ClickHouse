#include "Storages/System/StorageSystemKeywords.h"
#include "config.h"

#include <Databases/IDatabase.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachSystemTablesImpl.h>

#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemAsyncLoader.h>
#include <Storages/System/StorageSystemBackups.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemCollations.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemDataSkippingIndices.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <Storages/System/StorageSystemDetachedParts.h>
#include <Storages/System/StorageSystemDetachedTables.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/System/StorageSystemEvents.h>
#include <Storages/System/StorageSystemFormats.h>
#include <Storages/System/StorageSystemFunctions.h>
#include <Storages/System/StorageSystemWorkloads.h>
#include <Storages/System/StorageSystemResources.h>
#include <Storages/System/StorageSystemGraphite.h>
#include <Storages/System/StorageSystemMacros.h>
#include <Storages/System/StorageSystemMerges.h>
#include <Storages/System/StorageSystemMoves.h>
#include <Storages/System/StorageSystemReplicatedFetches.h>
#include <Storages/System/StorageSystemMetrics.h>
#include <Storages/System/StorageSystemModels.h>
#include <Storages/System/StorageSystemMutations.h>
#include <Storages/System/StorageSystemNumbers.h>
#include <Storages/System/StorageSystemOne.h>
#include <Storages/System/StorageSystemPartMovesBetweenShards.h>
#include <Storages/System/StorageSystemParts.h>
#include <Storages/System/StorageSystemProjectionParts.h>
#include <Storages/System/StorageSystemPartsColumns.h>
#include <Storages/System/StorageSystemProjectionPartsColumns.h>
#include <Storages/System/StorageSystemProcesses.h>
#include <Storages/System/StorageSystemUserProcesses.h>
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/System/StorageSystemServerSettings.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemSettingsChanges.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Storages/System/StorageSystemDatabaseEngines.h>
#include <Storages/System/StorageSystemTableEngines.h>
#include <Storages/System/StorageSystemTableFunctions.h>
#include <Storages/System/StorageSystemTables.h>
#include <Storages/System/StorageSystemProjections.h>
#include <Storages/System/StorageSystemZooKeeper.h>
#include <Storages/System/StorageSystemContributors.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Storages/System/StorageSystemWarnings.h>
#include <Storages/System/StorageSystemDDLWorkerQueue.h>
#include <Storages/System/StorageSystemLicenses.h>
#include <Storages/System/StorageSystemTimeZones.h>
#include <Storages/System/StorageSystemDisks.h>
#include <Storages/System/StorageSystemStoragePolicies.h>
#include <Storages/System/StorageSystemZeros.h>
#include <Storages/System/StorageSystemUsers.h>
#include <Storages/System/StorageSystemRoles.h>
#include <Storages/System/StorageSystemGrants.h>
#include <Storages/System/StorageSystemRoleGrants.h>
#include <Storages/System/StorageSystemCurrentRoles.h>
#include <Storages/System/StorageSystemEnabledRoles.h>
#include <Storages/System/StorageSystemSettingsProfiles.h>
#include <Storages/System/StorageSystemSettingsProfileElements.h>
#include <Storages/System/StorageSystemRowPolicies.h>
#include <Storages/System/StorageSystemQuotas.h>
#include <Storages/System/StorageSystemQuotaLimits.h>
#include <Storages/System/StorageSystemQuotaUsage.h>
#include <Storages/System/StorageSystemQuotasUsage.h>
#include <Storages/System/StorageSystemUserDirectories.h>
#include <Storages/System/StorageSystemPrivileges.h>
#include <Storages/System/StorageSystemAsynchronousInserts.h>
#include <Storages/System/StorageSystemTransactions.h>
#include <Storages/System/StorageSystemFilesystemCache.h>
#include <Storages/System/StorageSystemFilesystemCacheSettings.h>
#include <Storages/System/StorageSystemQueryCache.h>
#include <Storages/System/StorageSystemNamedCollections.h>
#include <Storages/System/StorageSystemRemoteDataPaths.h>
#include <Storages/System/StorageSystemCertificates.h>
#include <Storages/System/StorageSystemSchemaInferenceCache.h>
#include <Storages/System/StorageSystemDroppedTables.h>
#include <Storages/System/StorageSystemDroppedTablesParts.h>
#include <Storages/System/StorageSystemZooKeeperConnection.h>
#include <Storages/System/StorageSystemJemalloc.h>
#include <Storages/System/StorageSystemScheduler.h>
#include <Storages/System/StorageSystemS3Queue.h>
#include <Storages/System/StorageSystemObjectStorageQueueSettings.h>
#include <Storages/System/StorageSystemDashboards.h>
#include <Storages/System/StorageSystemViewRefreshes.h>
#include <Storages/System/StorageSystemDNSCache.h>

#if defined(__ELF__) && !defined(OS_FREEBSD)
#include <Storages/System/StorageSystemSymbols.h>
#endif

#if USE_RDKAFKA
#include <Storages/System/StorageSystemKafkaConsumers.h>
#endif

#ifdef OS_LINUX
#include <Storages/System/StorageSystemStackTrace.h>
#endif

#if USE_ROCKSDB
#include <Storages/RocksDB/StorageSystemRocksDB.h>
#endif

#if USE_MYSQL
#include <Storages/System/StorageSystemMySQLBinlogs.h>
#endif


namespace DB
{

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper)
{
    attachNoDescription<StorageSystemOne>(context, system_database, "one", "This table contains a single row with a single dummy UInt8 column containing the value 0. Used when the table is not specified explicitly, for example in queries like `SELECT 1`.");
    attachNoDescription<StorageSystemNumbers>(context, system_database, "numbers", "Generates all natural numbers, starting from 0 (to 2^64 - 1, and then again) in sorted order.", false, "number");
    attachNoDescription<StorageSystemNumbers>(context, system_database, "numbers_mt", "Multithreaded version of `system.numbers`. Numbers order is not guaranteed.", true, "number");
    attachNoDescription<StorageSystemZeros>(context, system_database, "zeros", "Produces unlimited number of non-materialized zeros.", false);
    attachNoDescription<StorageSystemZeros>(context, system_database, "zeros_mt", "Multithreaded version of system.zeros.", true);
    attach<StorageSystemDatabases>(context, system_database, "databases", "Lists all databases of the current server.");
    attachNoDescription<StorageSystemTables>(context, system_database, "tables", "Lists all tables of the current server.");
    attachNoDescription<StorageSystemDetachedTables>(context, system_database, "detached_tables", "Lists all detached tables of the current server.");
    attachNoDescription<StorageSystemColumns>(context, system_database, "columns", "Lists all columns from all tables of the current server.");
    attach<StorageSystemFunctions>(context, system_database, "functions", "Contains a list of all available ordinary and aggregate functions with their descriptions.");
    attach<StorageSystemEvents>(context, system_database, "events", "Contains profiling events and their current value.");
    attach<StorageSystemSettings>(context, system_database, "settings", "Contains a list of all user-level settings (which can be modified in a scope of query or session), their current and default values along with descriptions.");
    attach<StorageSystemServerSettings>(context, system_database, "server_settings", "Contains a list of all server-wide settings (which are effective only on server startup and usually cannot be modified at runtime), their current and default values along with descriptions.");
    attach<StorageSystemSettingsChanges>(context, system_database, "settings_changes", "Contains the information about the settings changes through different ClickHouse versions. You may make ClickHouse behave like a particular previous version by changing the `compatibility` user-level settings.");
    attach<SystemMergeTreeSettings<false>>(context, system_database, "merge_tree_settings", "Contains a list of all MergeTree engine specific settings, their current and default values along with descriptions. You may change any of them in SETTINGS section in CREATE query.");
    attach<SystemMergeTreeSettings<true>>(context, system_database, "replicated_merge_tree_settings", "Contains a list of all ReplicatedMergeTree engine specific settings, their current and default values along with descriptions. You may change any of them in SETTINGS section in CREATE query. ");
    attach<StorageSystemBuildOptions>(context, system_database, "build_options", "Contains a list of all build flags, compiler options and commit hash for used build.");
    attach<StorageSystemFormats>(context, system_database, "formats", "Contains a list of all the formats along with flags whether a format is suitable for input/output or whether it supports parallelization.");
    attach<StorageSystemTableFunctions>(context, system_database, "table_functions", "Contains a list of all available table functions with their descriptions.");
    attach<StorageSystemAggregateFunctionCombinators>(context, system_database, "aggregate_function_combinators", "Contains a list of all available aggregate function combinators, which could be applied to aggregate functions and change the way they work.");
    attach<StorageSystemDataTypeFamilies>(context, system_database, "data_type_families", "Contains a list of all available native data types along with all the aliases used for compatibility with other DBMS.");
    attach<StorageSystemCollations>(context, system_database, "collations", "Contains a list of all available collations for alphabetical comparison of strings.");
    attach<StorageSystemDatabaseEngines>(context, system_database, "database_engines", "Contains a list of all available database engines");
    attach<StorageSystemTableEngines>(context, system_database, "table_engines", "Contains a list of all available table engines along with information whether a particular table engine supports some specific features (e.g. settings, skipping indices, projections, replication, TTL, deduplication, parallel insert, etc.)");
    attach<StorageSystemContributors>(context, system_database, "contributors", "Contains a list of all ClickHouse contributors <3");
    attach<StorageSystemUsers>(context, system_database, "users", "Contains a list of all users profiles either configured at the server through a configuration file or created via SQL.");
    attach<StorageSystemRoles>(context, system_database, "roles", "Contains a list of all roles created at the server.");
    attach<StorageSystemGrants>(context, system_database, "grants", "Contains the information about privileges granted to ClickHouse user accounts.");
    attach<StorageSystemRoleGrants>(context, system_database, "role_grants", "Contains the role grants for users and roles. To add entries to this table, use `GRANT role TO user`. Using this table you may find out which roles are assigned to which users or which roles a user has.");
    attach<StorageSystemCurrentRoles>(context, system_database, "current_roles", "Contains active roles of a current user. SET ROLE changes the contents of this table.");
    attach<StorageSystemEnabledRoles>(context, system_database, "enabled_roles", "Contains all active roles at the moment, including current role of the current user and granted roles for current role.");
    attach<StorageSystemSettingsProfiles>(context, system_database, "settings_profiles", "Contains properties of configured setting profiles.");
    attach<StorageSystemSettingsProfileElements>(context, system_database, "settings_profile_elements", "Describes the content of each settings profile configured on the server. Including settings constraints, roles and users for which the settings are applied, and parent settings profiles.");
    attach<StorageSystemRowPolicies>(context, system_database, "row_policies", "Contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.");
    attach<StorageSystemQuotas>(context, system_database, "quotas", "Contains information about quotas.");
    attach<StorageSystemQuotaLimits>(context, system_database, "quota_limits", "Contains information about maximums for all intervals of all quotas. Any number of rows or zero can correspond to specific quota.");
    attach<StorageSystemQuotaUsage>(context, system_database, "quota_usage", "Contains quota usage by the current user: how much is used and how much is left.");
    attach<StorageSystemQuotasUsage>(context, system_database, "quotas_usage", "Contains quota usage by all users.");
    attach<StorageSystemUserDirectories>(context, system_database, "user_directories", "Contains the information about configured user directories - directories on the file system from which ClickHouse server is allowed to read user provided data.");
    attach<StorageSystemPrivileges>(context, system_database, "privileges", "Contains a list of all available privileges that could be granted to a user or role.");
    attach<StorageSystemErrors>(context, system_database, "errors", "Contains a list of all errors which have ever happened including the error code, last time and message with unsymbolized stacktrace.");
    attach<StorageSystemWarnings>(context, system_database, "warnings", "Contains warnings about server configuration to be displayed by clickhouse-client right after it connects to the server.");
    attachNoDescription<StorageSystemDataSkippingIndices>(context, system_database, "data_skipping_indices", "Contains all the information about all the data skipping indices in tables, similar to system.columns.");
    attachNoDescription<StorageSystemProjections>(context, system_database, "projections", "Contains all the information about all the projections in tables, similar to system.data_skipping_indices.");
    attach<StorageSystemLicenses>(context, system_database, "licenses", "Contains licenses of third-party libraries that are located in the contrib directory of ClickHouse sources.");
    attach<StorageSystemTimeZones>(context, system_database, "time_zones", "Contains a list of time zones that are supported by the ClickHouse server. This list of timezones might vary depending on the version of ClickHouse.");
    attach<StorageSystemBackups>(context, system_database, "backups", "Contains a list of all BACKUP or RESTORE operations with their current states and other propertis. Note, that table is not persistent and it shows only operations executed after the last server restart.");
    attach<StorageSystemSchemaInferenceCache>(context, system_database, "schema_inference_cache", "Contains information about all cached file schemas.");
    attach<StorageSystemDroppedTables>(context, system_database, "dropped_tables", "Contains a list of tables which were dropped from Atomic databases but not completely removed yet.");
    attachNoDescription<StorageSystemDroppedTablesParts>(context, system_database, "dropped_tables_parts", "Contains parts of system.dropped_tables tables ");
    attach<StorageSystemScheduler>(context, system_database, "scheduler", "Contains information and status for scheduling nodes residing on the local server.");
    attach<StorageSystemDNSCache>(context, system_database, "dns_cache", "Contains information about cached DNS records.");
#if defined(__ELF__) && !defined(OS_FREEBSD)
    attachNoDescription<StorageSystemSymbols>(context, system_database, "symbols", "Contains information for introspection of ClickHouse binary. This table is only useful for C++ experts and ClickHouse engineers.");
#endif
#if USE_RDKAFKA
    attach<StorageSystemKafkaConsumers>(context, system_database, "kafka_consumers", "Contains information about Kafka consumers. Applicable for Kafka table engine (native ClickHouse integration).");
#endif
#ifdef OS_LINUX
    attachNoDescription<StorageSystemStackTrace>(context, system_database, "stack_trace", "Allows to obtain an unsymbolized stacktrace from all the threads of the server process.");
#endif
#if USE_ROCKSDB
    attach<StorageSystemRocksDB>(context, system_database, "rocksdb", "Contains a list of metrics exposed from embedded RocksDB.");
#endif
#if USE_MYSQL
    attachNoDescription<StorageSystemMySQLBinlogs>(context, system_database, "mysql_binlogs", "Shows a list of active binlogs for MaterializedMySQL.");
#endif

    attach<StorageSystemKeywords>(context, system_database, "keywords", "Contains a list of all keywords used in ClickHouse parser.");
    attachNoDescription<StorageSystemParts>(context, system_database, "parts", "Contains a list of currently existing (both active and inactive) parts of all *-MergeTree tables. Each part is represented by a single row.");
    attachNoDescription<StorageSystemProjectionParts>(context, system_database, "projection_parts", "Contains a list of currently existing projection parts (a copy of some part containing aggregated data or just sorted in different order) created for all the projections for all tables within a cluster.");
    attachNoDescription<StorageSystemDetachedParts>(context, system_database, "detached_parts", "Contains a list of all parts which are being found in /detached directory along with a reason why it was detached. ClickHouse server doesn't use such parts anyhow.");
    attachNoDescription<StorageSystemPartsColumns>(context, system_database, "parts_columns", "Contains a list of columns of all currently existing parts of all MergeTree tables. Each column is represented by a single row.");
    attachNoDescription<StorageSystemProjectionPartsColumns>(context, system_database, "projection_parts_columns", "Contains a list of columns of all currently existing projection parts of all MergeTree tables. Each column is represented by a single row.");
    attachNoDescription<StorageSystemDisks>(context, system_database, "disks", "Contains information about disks defined in the server configuration.");
    attachNoDescription<StorageSystemStoragePolicies>(context, system_database, "storage_policies", "Contains information about storage policies and volumes defined in the server configuration.");
    attach<StorageSystemProcesses>(context, system_database, "processes", "Contains a list of currently executing processes (queries) with their progress.");
    attach<StorageSystemMetrics>(context, system_database, "metrics", "Contains metrics which can be calculated instantly, or have a current value. For example, the number of simultaneously processed queries or the current replica delay. This table is always up to date.");
    attach<StorageSystemMerges>(context, system_database, "merges", "Contains a list of merges currently executing merges of MergeTree tables and their progress. Each merge operation is represented by a single row.");
    attach<StorageSystemMoves>(context, system_database, "moves", "Contains information about in-progress data part moves of MergeTree tables. Each data part movement is represented by a single row.");
    attach<StorageSystemMutations>(context, system_database, "mutations", "Contains a list of mutations and their progress. Each mutation command is represented by a single row.");
    attachNoDescription<StorageSystemReplicas>(context, system_database, "replicas", "Contains information and status of all table replicas on current server. Each replica is represented by a single row.");
    attach<StorageSystemReplicationQueue>(context, system_database, "replication_queue", "Contains information about tasks from replication queues stored in ClickHouse Keeper, or ZooKeeper, for each table replica.");
    attach<StorageSystemDDLWorkerQueue>(context, system_database, "distributed_ddl_queue", "Contains information about distributed DDL queries (ON CLUSTER clause) that were executed on a cluster.");
    attach<StorageSystemDistributionQueue>(context, system_database, "distribution_queue", "Contains information about local files that are in the queue to be sent to the shards. These local files contain new parts that are created by inserting new data into the Distributed table in asynchronous mode.");
    attach<StorageSystemDictionaries>(context, system_database, "dictionaries", "Contains information about dictionaries.");
    attach<StorageSystemModels>(context, system_database, "models", "Contains a list of CatBoost models loaded into a LibraryBridge's memory along with time when it was loaded.");
    attach<StorageSystemClusters>(context, system_database, "clusters", "Contains information about clusters defined in the configuration file or generated by a Replicated database.");
    attach<StorageSystemGraphite>(context, system_database, "graphite_retentions", "Contains information about parameters graphite_rollup which are used in tables with *GraphiteMergeTree engines.");
    attach<StorageSystemMacros>(context, system_database, "macros", "Contains a list of all macros defined in server configuration.");
    attach<StorageSystemReplicatedFetches>(context, system_database, "replicated_fetches", "Contains information about currently running background fetches.");
    attach<StorageSystemPartMovesBetweenShards>(context, system_database, "part_moves_between_shards", "Contains information about parts which are currently in a process of moving between shards and their progress.");
    attach<StorageSystemAsynchronousInserts>(context, system_database, "asynchronous_inserts", "Contains information about pending asynchronous inserts in queue in server's memory.");
    attachNoDescription<StorageSystemFilesystemCache>(context, system_database, "filesystem_cache", "Contains information about all entries inside filesystem cache for remote objects.");
    attachNoDescription<StorageSystemFilesystemCacheSettings>(context, system_database, "filesystem_cache_settings", "Contains information about all filesystem cache settings");
    attachNoDescription<StorageSystemQueryCache>(context, system_database, "query_cache", "Contains information about all entries inside query cache in server's memory.");
    attachNoDescription<StorageSystemRemoteDataPaths>(context, system_database, "remote_data_paths", "Contains a mapping from a filename on local filesystem to a blob name inside object storage.");
    attach<StorageSystemCertificates>(context, system_database, "certificates", "Contains information about available certificates and their sources.");
    attachNoDescription<StorageSystemNamedCollections>(context, system_database, "named_collections", "Contains a list of all named collections which were created via SQL query or parsed from configuration file.");
    attach<StorageSystemAsyncLoader>(context, system_database, "asynchronous_loader", "Contains information and status for recent asynchronous jobs (e.g. for tables loading). The table contains a row for every job.");
    attach<StorageSystemUserProcesses>(context, system_database, "user_processes", "This system table can be used to get overview of memory usage and ProfileEvents of users.");
    attachNoDescription<StorageSystemJemallocBins>(context, system_database, "jemalloc_bins", "Contains information about memory allocations done via jemalloc allocator in different size classes (bins) aggregated from all arenas. These statistics might not be absolutely accurate because of thread local caching in jemalloc.");
    attachNoDescription<StorageSystemS3Queue>(context, system_database, "s3queue", "Contains in-memory state of S3Queue metadata and currently processed rows per file.");
    attach<StorageSystemObjectStorageQueueSettings<ObjectStorageType::S3>>(context, system_database, "s3_queue_settings", "Contains a list of settings of S3Queue tables.");
    attach<StorageSystemObjectStorageQueueSettings<ObjectStorageType::Azure>>(context, system_database, "azure_queue_settings", "Contains a list of settings of AzureQueue tables.");
    attach<StorageSystemDashboards>(context, system_database, "dashboards", "Contains queries used by /dashboard page accessible though HTTP interface. This table can be useful for monitoring and troubleshooting. The table contains a row for every chart in a dashboard.");
    attach<StorageSystemViewRefreshes>(context, system_database, "view_refreshes", "Lists all Refreshable Materialized Views of current server.");
    attach<StorageSystemWorkloads>(context, system_database, "workloads", "Contains a list of all currently existing workloads.");
    attach<StorageSystemResources>(context, system_database, "resources", "Contains a list of all currently existing resources.");

    if (has_zookeeper)
    {
        attachNoDescription<StorageSystemZooKeeper>(context, system_database, "zookeeper", "Exposes data from the [Zoo]Keeper cluster defined in the config. Allow to get the list of children for a particular node or read the value written inside it.");
        attach<StorageSystemZooKeeperConnection>(context, system_database, "zookeeper_connection", "Shows the information about current connections to [Zoo]Keeper (including auxiliary [ZooKeepers)");
    }

    if (context->getConfigRef().getInt("allow_experimental_transactions", 0))
        attach<StorageSystemTransactions>(context, system_database, "transactions", "Contains a list of transactions and their state.");
}

void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attachNoDescription<StorageSystemAsynchronousMetrics>(context, system_database, "asynchronous_metrics", "Contains metrics that are calculated periodically in the background. For example, the amount of RAM in use.", async_metrics);
}

}
