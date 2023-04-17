#include "config.h"

#include <Databases/IDatabase.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachSystemTablesImpl.h>

#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemBackups.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemCollations.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDatabases.h>
#include <Storages/System/StorageSystemDataSkippingIndices.h>
#include <Storages/System/StorageSystemDataTypeFamilies.h>
#include <Storages/System/StorageSystemDetachedParts.h>
#include <Storages/System/StorageSystemDictionaries.h>
#include <Storages/System/StorageSystemEvents.h>
#include <Storages/System/StorageSystemFormats.h>
#include <Storages/System/StorageSystemFunctions.h>
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
#include <Storages/System/StorageSystemReplicas.h>
#include <Storages/System/StorageSystemReplicationQueue.h>
#include <Storages/System/StorageSystemDistributionQueue.h>
#include <Storages/System/StorageSystemServerSettings.h>
#include <Storages/System/StorageSystemSettings.h>
#include <Storages/System/StorageSystemSettingsChanges.h>
#include <Storages/System/StorageSystemMergeTreeSettings.h>
#include <Storages/System/StorageSystemTableEngines.h>
#include <Storages/System/StorageSystemTableFunctions.h>
#include <Storages/System/StorageSystemTables.h>
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
#include <Storages/System/StorageSystemQueryCache.h>
#include <Storages/System/StorageSystemNamedCollections.h>
#include <Storages/System/StorageSystemRemoteDataPaths.h>
#include <Storages/System/StorageSystemCertificates.h>
#include <Storages/System/StorageSystemSchemaInferenceCache.h>
#include <Storages/System/StorageSystemDroppedTables.h>

#ifdef OS_LINUX
#include <Storages/System/StorageSystemStackTrace.h>
#endif

#if USE_ROCKSDB
#include <Storages/RocksDB/StorageSystemRocksDB.h>
#include <Storages/System/StorageSystemMergeTreeMetadataCache.h>
#endif


namespace DB
{

void attachSystemTablesLocal(ContextPtr context, IDatabase & system_database)
{
    attach<StorageSystemOne>(context, system_database, "one", "Used when the table is not specified explicitly, for example in queries like `SELECT 1`. Analog of the DUAL table in Oracle and MySQL.");
    attach<StorageSystemNumbers>(context, system_database, "numbers", "Generates all natural numbers, starting from 0 (to 2^64 - 1, and then again) in sorted order.", false);
    attach<StorageSystemNumbers>(context, system_database, "numbers_mt", "Multithreaded version of `system.numbers`. Numbers order is not guaranteed.", true);
    attach<StorageSystemZeros>(context, system_database, "zeros", "Produces unlimited number of non-materialized zeros.", false);
    attach<StorageSystemZeros>(context, system_database, "zeros_mt", "Multithreaded version of system.zeros.", true);
    attach<StorageSystemDatabases>(context, system_database, "databases", "Contains all the information about all the databases within the current server.");
    attach<StorageSystemTables>(context, system_database, "tables", "Contains all the information about all the tables within the current server.");
    attach<StorageSystemColumns>(context, system_database, "columns", "Contains all the information about all the columns from all the tables within the current server.");
    attach<StorageSystemFunctions>(context, system_database, "functions", "Contains a list of all available ordinary and aggregate functions with their descriptions.");
    attach<StorageSystemEvents>(context, system_database, "events", "Contains a list of useful for profiling events with their descriptions and values at a moment of execution.");
    attach<StorageSystemSettings>(context, system_database, "settings", "Contains a list of all user-level settings (which can be modified in a scope of query or session), their current and default values along with descriptions.");
    attach<StorageSystemServerSettings>(context, system_database, "server_settings", "Contains a list of all server-wide settings (which are effective only on server startup and usually cannot be modified at runtime), their current and default values along with descriptions.");
    attach<StorageSystemSettingsChanges>(context, system_database, "settings_changes", "Contains the information about the settings changes through different ClickHouse versions. You may make ClickHouse behave like a particular previous version by changing the `compatibility` user-level settings.");
    attach<SystemMergeTreeSettings<false>>(context, system_database, "merge_tree_settings", "Contains a list of all MergeTree engine specific settings, their current and default values along with descriptions. You may change any of them in SETTINGS section in CREATE query.");
    attach<SystemMergeTreeSettings<true>>(context, system_database, "replicated_merge_tree_settings", "Contains a list of all ReplicatedMergeTree engine specific settings, their current and default values along with descriptions. You may change any of them in SETTINGS section in CREATE query. ");
    attach<StorageSystemBuildOptions>(context, system_database, "build_options", "Contains a list of all build flags, compiler options and commit hash from which this particular server was built.");
    attach<StorageSystemFormats>(context, system_database, "formats", "Contains a list of all the formats along with flags whether a format is suitable for input/output or whether it supports parallelization.");
    attach<StorageSystemTableFunctions>(context, system_database, "table_functions", "Contains a list of all available table functions with their descriptions.");
    attach<StorageSystemAggregateFunctionCombinators>(context, system_database, "aggregate_function_combinators", "Contains a list of all available aggregate function combinator, which could be added to the end of aggregate function and change the way how this aggregate function works.");
    attach<StorageSystemDataTypeFamilies>(context, system_database, "data_type_families", "Contains a list of all available native data types along with all the aliases which a useful for compatibility with other databases.");
    attach<StorageSystemCollations>(context, system_database, "collations", "Contains a list of all available collations for alphabetical comparison of strings.");
    attach<StorageSystemTableEngines>(context, system_database, "table_engines", "Contains a list of all available table engines along with several flags whether a particular table engine supports some feature (e.g. settings, skipping indices, projections, replication, ttl, deduplication, parallel insert etc.)");
    attach<StorageSystemContributors>(context, system_database, "contributors", "Contains a list of all ClickHouse contributors <3");
    attach<StorageSystemUsers>(context, system_database, "users", "Contains a list of all users accounts either configured at the server through configuration file or created via SQL.");
    attach<StorageSystemRoles>(context, system_database, "roles", "Contains a list of all roles created at the server.");
    attach<StorageSystemGrants>(context, system_database, "grants", "Contains the information about privileges granted to ClickHouse user accounts.");
    attach<StorageSystemRoleGrants>(context, system_database, "role_grants", "Contains the role grants for users and roles. To add entries to this table, use `GRANT role TO user`. Using this table you may find out which roles are assigned to which users or which roles does user has.");
    attach<StorageSystemCurrentRoles>(context, system_database, "current_roles", "Contains active roles of a current user. SET ROLE changes the contents of this table.");
    attach<StorageSystemEnabledRoles>(context, system_database, "enabled_roles", "Contains all active roles at the moment, including current role of the current user and granted roles for current role.");
    attach<StorageSystemSettingsProfiles>(context, system_database, "settings_profiles", "Contains properties of configured setting profiles.");
    attach<StorageSystemSettingsProfileElements>(context, system_database, "settings_profile_elements", "Describes the content of each settings profile configured on the server. Including settings contraints, roles and users that the setting applies to and parent settings profiles.");
    attach<StorageSystemRowPolicies>(context, system_database, "row_policies", "Contains filters for one particular table, as well as a list of roles and/or users which should use this row policy.");
    attach<StorageSystemQuotas>(context, system_database, "quotas", "Contains information about quotas - a way how to limit resource usage over a period of time or track the use of resources.");
    attach<StorageSystemQuotaLimits>(context, system_database, "quota_limits", "Contains information about maximums for all intervals of all quotas. Any number of rows or zero can correspond to specific quota.");
    attach<StorageSystemQuotaUsage>(context, system_database, "quota_usage", "Contains quota usage by the current user: how much is used and how much is left.");
    attach<StorageSystemQuotasUsage>(context, system_database, "quotas_usage", "Contains quota usage by all users.");
    attach<StorageSystemUserDirectories>(context, system_database, "user_directories", "Contains the information about configured user directories - directories on the file system from which ClickHouse server is allowed to read user provided data.");
    attach<StorageSystemPrivileges>(context, system_database, "privileges", "Contains a list of all available privileges that could be granted to a user or role.");
    attach<StorageSystemErrors>(context, system_database, "errors", "Contains a list of all error which have ever happened including the error code, last time and message with unsymbolized stacktrace.");
    attach<StorageSystemWarnings>(context, system_database, "warnings", "Contains warnings about server configuration to be displayed by clickhouse-client right after it connects to the server.");
    attach<StorageSystemDataSkippingIndices>(context, system_database, "data_skipping_indices", "Contains all the information about all the data skipping indices in tables, similar to system.columns.");
    attach<StorageSystemLicenses>(context, system_database, "licenses", "Сontains licenses of third-party libraries that are located in the contrib directory of ClickHouse sources.");
    attach<StorageSystemTimeZones>(context, system_database, "time_zones", "Contains a list of time zones that are supported by the ClickHouse server. This list of timezones might vary depending on the version of ClickHouse.");
    attach<StorageSystemBackups>(context, system_database, "backups", "Contains a list of all BACKUP or RESTORE operations with their current states and other propertis. Note, that table is not persistent and it shows only operations executed after the last server restart.");
    attach<StorageSystemSchemaInferenceCache>(context, system_database, "schema_inference_cache", "Contains information about all cached file schemas.");
    attach<StorageSystemDroppedTables>(context, system_database, "dropped_tables", "Contains a list of tables which were dropped from Atomic databases but not completely removed yet.");
#ifdef OS_LINUX
    attach<StorageSystemStackTrace>(context, system_database, "stack_trace", "Allows to obtain an unsymbolized stacktrace from all the threads of the server process.");
#endif
#if USE_ROCKSDB
    attach<StorageSystemRocksDB>(context, system_database, "rocksdb", "Contains a list of metrics exposed from embedded RocksDB.");
    attach<StorageSystemMergeTreeMetadataCache>(context, system_database, "merge_tree_metadata_cache", "Allows to look inside metadata cache stored in RocksDB to check its consistency.");
#endif
}

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper)
{
    attachSystemTablesLocal(context, system_database);

    attach<StorageSystemParts>(context, system_database, "parts", "Contains a list of currently existing (both active and inactive) parts of all *-MergeTree tables.");
    attach<StorageSystemProjectionParts>(context, system_database, "projection_parts", "Contains a list of currently existing projection parts (a copy of some part containing aggregated data or just sorted in different order) created for all the projections for all tables within a cluster.");
    attach<StorageSystemDetachedParts>(context, system_database, "detached_parts", "Contains a list of all parts which are being found in /detached directory along with a reason why it was detached. ClickHouse server doesn't use such parts anyhow.");
    attach<StorageSystemPartsColumns>(context, system_database, "parts_columns", "");
    attach<StorageSystemProjectionPartsColumns>(context, system_database, "projection_parts_columns", "");
    attach<StorageSystemDisks>(context, system_database, "disks", "");
    attach<StorageSystemStoragePolicies>(context, system_database, "storage_policies", "");
    attach<StorageSystemProcesses>(context, system_database, "processes", "");
    attach<StorageSystemMetrics>(context, system_database, "metrics", "");
    attach<StorageSystemMerges>(context, system_database, "merges", "");
    attach<StorageSystemMoves>(context, system_database, "moves", "");
    attach<StorageSystemMutations>(context, system_database, "mutations", "");
    attach<StorageSystemReplicas>(context, system_database, "replicas", "");
    attach<StorageSystemReplicationQueue>(context, system_database, "replication_queue", "");
    attach<StorageSystemDDLWorkerQueue>(context, system_database, "distributed_ddl_queue", "");
    attach<StorageSystemDistributionQueue>(context, system_database, "distribution_queue", "");
    attach<StorageSystemDictionaries>(context, system_database, "dictionaries", "");
    attach<StorageSystemModels>(context, system_database, "models", "");
    attach<StorageSystemClusters>(context, system_database, "clusters", "");
    attach<StorageSystemGraphite>(context, system_database, "graphite_retentions", "");
    attach<StorageSystemMacros>(context, system_database, "macros", "");
    attach<StorageSystemReplicatedFetches>(context, system_database, "replicated_fetches", "");
    attach<StorageSystemPartMovesBetweenShards>(context, system_database, "part_moves_between_shards", "");
    attach<StorageSystemAsynchronousInserts>(context, system_database, "asynchronous_inserts", "");
    attach<StorageSystemFilesystemCache>(context, system_database, "filesystem_cache", "");
    attach<StorageSystemQueryCache>(context, system_database, "query_cache", "");
    attach<StorageSystemRemoteDataPaths>(context, system_database, "remote_data_paths", "");
    attach<StorageSystemCertificates>(context, system_database, "certificates", "");
    attach<StorageSystemNamedCollections>(context, system_database, "named_collections", "");

    if (has_zookeeper)
        attach<StorageSystemZooKeeper>(context, system_database, "zookeeper", "Exposes data from the [Zoo]Keeper cluster defined in the config. Allow to get the list of children for a particular node or read the value written inside it.");

    if (context->getConfigRef().getInt("allow_experimental_transactions", 0))
        attach<StorageSystemTransactions>(context, system_database, "transactions", "");
}

void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attach<StorageSystemAsynchronousMetrics>(context, system_database, "asynchronous_metrics", "", async_metrics);
}

}
