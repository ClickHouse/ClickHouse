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
#include <Storages/System/StorageSystemUserProcesses.h>
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
#include <Storages/System/StorageSystemZooKeeperConnection.h>
#include <Storages/System/StorageSystemJemalloc.h>
#include <Storages/System/StorageSystemScheduler.h>
#include <Storages/System/StorageSystemS3Queue.h>
#include <Storages/System/StorageSystemDashboards.h>

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


namespace DB
{

void attachSystemTablesLocal(ContextPtr context, IDatabase & system_database)
{
    attachLazy<StorageSystemOne>(context, system_database, "one");
    attachLazy<StorageSystemNumbers>(context, system_database, "numbers", false);
    attachLazy<StorageSystemNumbers>(context, system_database, "numbers_mt", true);
    attachLazy<StorageSystemZeros>(context, system_database, "zeros", false);
    attachLazy<StorageSystemZeros>(context, system_database, "zeros_mt", true);
    attachLazy<StorageSystemDatabases>(context, system_database, "databases");
    attachLazy<StorageSystemTables>(context, system_database, "tables");
    attachLazy<StorageSystemColumns>(context, system_database, "columns");
    attachLazy<StorageSystemFunctions>(context, system_database, "functions");
    attachLazy<StorageSystemEvents>(context, system_database, "events");
    attachLazy<StorageSystemSettings>(context, system_database, "settings");
    attachLazy<StorageSystemServerSettings>(context, system_database, "server_settings");
    attachLazy<StorageSystemSettingsChanges>(context, system_database, "settings_changes");
    attachLazy<SystemMergeTreeSettings<false>>(context, system_database, "merge_tree_settings");
    attachLazy<SystemMergeTreeSettings<true>>(context, system_database, "replicated_merge_tree_settings");
    attachLazy<StorageSystemBuildOptions>(context, system_database, "build_options");
    attachLazy<StorageSystemFormats>(context, system_database, "formats");
    attachLazy<StorageSystemTableFunctions>(context, system_database, "table_functions");
    attachLazy<StorageSystemAggregateFunctionCombinators>(context, system_database, "aggregate_function_combinators");
    attachLazy<StorageSystemDataTypeFamilies>(context, system_database, "data_type_families");
    attachLazy<StorageSystemCollations>(context, system_database, "collations");
    attachLazy<StorageSystemTableEngines>(context, system_database, "table_engines");
    attachLazy<StorageSystemContributors>(context, system_database, "contributors");
    attachLazy<StorageSystemUsers>(context, system_database, "users");
    attachLazy<StorageSystemRoles>(context, system_database, "roles");
    attachLazy<StorageSystemGrants>(context, system_database, "grants");
    attachLazy<StorageSystemRoleGrants>(context, system_database, "role_grants");
    attachLazy<StorageSystemCurrentRoles>(context, system_database, "current_roles");
    attachLazy<StorageSystemEnabledRoles>(context, system_database, "enabled_roles");
    attachLazy<StorageSystemSettingsProfiles>(context, system_database, "settings_profiles");
    attachLazy<StorageSystemSettingsProfileElements>(context, system_database, "settings_profile_elements");
    attachLazy<StorageSystemRowPolicies>(context, system_database, "row_policies");
    attachLazy<StorageSystemQuotas>(context, system_database, "quotas");
    attachLazy<StorageSystemQuotaLimits>(context, system_database, "quota_limits");
    attachLazy<StorageSystemQuotaUsage>(context, system_database, "quota_usage");
    attachLazy<StorageSystemQuotasUsage>(context, system_database, "quotas_usage");
    attachLazy<StorageSystemUserDirectories>(context, system_database, "user_directories");
    attachLazy<StorageSystemPrivileges>(context, system_database, "privileges");
    attachLazy<StorageSystemErrors>(context, system_database, "errors");
    attachLazy<StorageSystemWarnings>(context, system_database, "warnings");
    attachLazy<StorageSystemDataSkippingIndices>(context, system_database, "data_skipping_indices");
    attachLazy<StorageSystemLicenses>(context, system_database, "licenses");
    attachLazy<StorageSystemTimeZones>(context, system_database, "time_zones");
    attachLazy<StorageSystemBackups>(context, system_database, "backups");
    attachLazy<StorageSystemSchemaInferenceCache>(context, system_database, "schema_inference_cache");
    attachLazy<StorageSystemDroppedTables>(context, system_database, "dropped_tables");
    attachLazy<StorageSystemScheduler>(context, system_database, "scheduler");
#if defined(__ELF__) && !defined(OS_FREEBSD)
    attachLazy<StorageSystemSymbols>(context, system_database, "symbols");
#endif
#if USE_RDKAFKA
    attachLazy<StorageSystemKafkaConsumers>(context, system_database, "kafka_consumers");
#endif
#ifdef OS_LINUX
    attachLazy<StorageSystemStackTrace>(context, system_database, "stack_trace");
#endif
#if USE_ROCKSDB
    attachLazy<StorageSystemRocksDB>(context, system_database, "rocksdb");
#endif
}

void attachSystemTablesServer(ContextPtr context, IDatabase & system_database, bool has_zookeeper)
{
    attachSystemTablesLocal(context, system_database);

    attach<StorageSystemParts>(context, system_database, "parts");
    attach<StorageSystemProjectionParts>(context, system_database, "projection_parts");
    attach<StorageSystemDetachedParts>(context, system_database, "detached_parts");
    attach<StorageSystemPartsColumns>(context, system_database, "parts_columns");
    attach<StorageSystemProjectionPartsColumns>(context, system_database, "projection_parts_columns");
    attach<StorageSystemDisks>(context, system_database, "disks");
    attach<StorageSystemStoragePolicies>(context, system_database, "storage_policies");
    attach<StorageSystemProcesses>(context, system_database, "processes");
    attach<StorageSystemMetrics>(context, system_database, "metrics");
    attach<StorageSystemMerges>(context, system_database, "merges");
    attach<StorageSystemMoves>(context, system_database, "moves");
    attach<StorageSystemMutations>(context, system_database, "mutations");
    attach<StorageSystemReplicas>(context, system_database, "replicas");
    attach<StorageSystemReplicationQueue>(context, system_database, "replication_queue");
    attach<StorageSystemDDLWorkerQueue>(context, system_database, "distributed_ddl_queue");
    attach<StorageSystemDistributionQueue>(context, system_database, "distribution_queue");
    attach<StorageSystemDictionaries>(context, system_database, "dictionaries");
    attach<StorageSystemModels>(context, system_database, "models");
    attach<StorageSystemClusters>(context, system_database, "clusters");
    attach<StorageSystemGraphite>(context, system_database, "graphite_retentions");
    attach<StorageSystemMacros>(context, system_database, "macros");
    attach<StorageSystemReplicatedFetches>(context, system_database, "replicated_fetches");
    attach<StorageSystemPartMovesBetweenShards>(context, system_database, "part_moves_between_shards");
    attach<StorageSystemAsynchronousInserts>(context, system_database, "asynchronous_inserts");
    attach<StorageSystemFilesystemCache>(context, system_database, "filesystem_cache");
    attach<StorageSystemQueryCache>(context, system_database, "query_cache");
    attach<StorageSystemRemoteDataPaths>(context, system_database, "remote_data_paths");
    attach<StorageSystemCertificates>(context, system_database, "certificates");
    attach<StorageSystemNamedCollections>(context, system_database, "named_collections");
    attach<StorageSystemAsyncLoader>(context, system_database, "async_loader");
    attach<StorageSystemUserProcesses>(context, system_database, "user_processes");
    attach<StorageSystemJemallocBins>(context, system_database, "jemalloc_bins");
    attach<StorageSystemS3Queue>(context, system_database, "s3queue");
    attach<StorageSystemDashboards>(context, system_database, "dashboards");

    if (has_zookeeper)
    {
        attach<StorageSystemZooKeeper>(context, system_database, "zookeeper");
        attach<StorageSystemZooKeeperConnection>(context, system_database, "zookeeper_connection");
    }

    if (context->getConfigRef().getInt("allow_experimental_transactions", 0))
        attach<StorageSystemTransactions>(context, system_database, "transactions");
}

void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attach<StorageSystemAsynchronousMetrics>(context, system_database, "asynchronous_metrics", async_metrics);
}

}
