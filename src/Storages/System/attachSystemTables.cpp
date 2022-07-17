#include "config_core.h"

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
#include <Storages/System/StorageSystemSettings.h>
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
#include <Storages/System/StorageSystemRemoteDataPaths.h>
#include <Storages/System/StorageSystemCertificates.h>

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
    attach<StorageSystemOne>(context, system_database, "one");
    attach<StorageSystemNumbers>(context, system_database, "numbers", false);
    attach<StorageSystemNumbers>(context, system_database, "numbers_mt", true);
    attach<StorageSystemZeros>(context, system_database, "zeros", false);
    attach<StorageSystemZeros>(context, system_database, "zeros_mt", true);
    attach<StorageSystemDatabases>(context, system_database, "databases");
    attach<StorageSystemTables>(context, system_database, "tables");
    attach<StorageSystemColumns>(context, system_database, "columns");
    attach<StorageSystemFunctions>(context, system_database, "functions");
    attach<StorageSystemEvents>(context, system_database, "events");
    attach<StorageSystemSettings>(context, system_database, "settings");
    attach<SystemMergeTreeSettings<false>>(context, system_database, "merge_tree_settings");
    attach<SystemMergeTreeSettings<true>>(context, system_database, "replicated_merge_tree_settings");
    attach<StorageSystemBuildOptions>(context, system_database, "build_options");
    attach<StorageSystemFormats>(context, system_database, "formats");
    attach<StorageSystemTableFunctions>(context, system_database, "table_functions");
    attach<StorageSystemAggregateFunctionCombinators>(context, system_database, "aggregate_function_combinators");
    attach<StorageSystemDataTypeFamilies>(context, system_database, "data_type_families");
    attach<StorageSystemCollations>(context, system_database, "collations");
    attach<StorageSystemTableEngines>(context, system_database, "table_engines");
    attach<StorageSystemContributors>(context, system_database, "contributors");
    attach<StorageSystemUsers>(context, system_database, "users");
    attach<StorageSystemRoles>(context, system_database, "roles");
    attach<StorageSystemGrants>(context, system_database, "grants");
    attach<StorageSystemRoleGrants>(context, system_database, "role_grants");
    attach<StorageSystemCurrentRoles>(context, system_database, "current_roles");
    attach<StorageSystemEnabledRoles>(context, system_database, "enabled_roles");
    attach<StorageSystemSettingsProfiles>(context, system_database, "settings_profiles");
    attach<StorageSystemSettingsProfileElements>(context, system_database, "settings_profile_elements");
    attach<StorageSystemRowPolicies>(context, system_database, "row_policies");
    attach<StorageSystemQuotas>(context, system_database, "quotas");
    attach<StorageSystemQuotaLimits>(context, system_database, "quota_limits");
    attach<StorageSystemQuotaUsage>(context, system_database, "quota_usage");
    attach<StorageSystemQuotasUsage>(context, system_database, "quotas_usage");
    attach<StorageSystemUserDirectories>(context, system_database, "user_directories");
    attach<StorageSystemPrivileges>(context, system_database, "privileges");
    attach<StorageSystemErrors>(context, system_database, "errors");
    attach<StorageSystemWarnings>(context, system_database, "warnings");
    attach<StorageSystemDataSkippingIndices>(context, system_database, "data_skipping_indices");
    attach<StorageSystemLicenses>(context, system_database, "licenses");
    attach<StorageSystemTimeZones>(context, system_database, "time_zones");
    attach<StorageSystemBackups>(context, system_database, "backups");
#ifdef OS_LINUX
    attach<StorageSystemStackTrace>(context, system_database, "stack_trace");
#endif
#if USE_ROCKSDB
    attach<StorageSystemRocksDB>(context, system_database, "rocksdb");
    attach<StorageSystemMergeTreeMetadataCache>(context, system_database, "merge_tree_metadata_cache");
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
    attach<StorageSystemRemoteDataPaths>(context, system_database, "remote_data_paths");
    attach<StorageSystemCertificates>(context, system_database, "certificates");

    if (has_zookeeper)
        attach<StorageSystemZooKeeper>(context, system_database, "zookeeper");

    if (context->getConfigRef().getInt("allow_experimental_transactions", 0))
        attach<StorageSystemTransactions>(context, system_database, "transactions");
}

void attachSystemTablesAsync(ContextPtr context, IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attach<StorageSystemAsynchronousMetrics>(context, system_database, "asynchronous_metrics", async_metrics);
}

}
