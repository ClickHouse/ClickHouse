#include <Databases/IDatabase.h>
#include <Storages/System/attachSystemTables.h>
#include <Storages/System/attachSystemTablesImpl.h>

#include <Storages/System/StorageSystemAggregateFunctionCombinators.h>
#include <Storages/System/StorageSystemAsynchronousMetrics.h>
#include <Storages/System/StorageSystemBuildOptions.h>
#include <Storages/System/StorageSystemCollations.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/System/StorageSystemColumns.h>
#include <Storages/System/StorageSystemDatabases.h>
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
#include <Storages/System/StorageSystemParts.h>
#include <Storages/System/StorageSystemPartsColumns.h>
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
#include <Storages/System/StorageSystemDDLWorkerQueue.h>

#if !defined(ARCADIA_BUILD)
    #include <Storages/System/StorageSystemLicenses.h>
    #include <Storages/System/StorageSystemTimeZones.h>
#endif
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

#ifdef OS_LINUX
#include <Storages/System/StorageSystemStackTrace.h>
#endif


namespace DB
{

void attachSystemTablesLocal(IDatabase & system_database)
{
    attachSystemTable<StorageSystemOne>(system_database, "one");
    attachSystemTable<StorageSystemNumbers>(system_database, "numbers", false);
    attachSystemTable<StorageSystemNumbers>(system_database, "numbers_mt", true);
    attachSystemTable<StorageSystemZeros>(system_database, "zeros", false);
    attachSystemTable<StorageSystemZeros>(system_database, "zeros_mt", true);
    attachSystemTable<StorageSystemDatabases>(system_database, "databases");
    attachSystemTable<StorageSystemTables>(system_database, "tables");
    attachSystemTable<StorageSystemColumns>(system_database, "columns");
    attachSystemTable<StorageSystemFunctions>(system_database, "functions");
    attachSystemTable<StorageSystemEvents>(system_database, "events");
    attachSystemTable<StorageSystemSettings>(system_database, "settings");
    attachSystemTable<SystemMergeTreeSettings<false>>(system_database, "merge_tree_settings");
    attachSystemTable<SystemMergeTreeSettings<true>>(system_database, "replicated_merge_tree_settings");
    attachSystemTable<StorageSystemBuildOptions>(system_database, "build_options");
    attachSystemTable<StorageSystemFormats>(system_database, "formats");
    attachSystemTable<StorageSystemTableFunctions>(system_database, "table_functions");
    attachSystemTable<StorageSystemAggregateFunctionCombinators>(system_database, "aggregate_function_combinators");
    attachSystemTable<StorageSystemDataTypeFamilies>(system_database, "data_type_families");
    attachSystemTable<StorageSystemCollations>(system_database, "collations");
    attachSystemTable<StorageSystemTableEngines>(system_database, "table_engines");
    attachSystemTable<StorageSystemContributors>(system_database, "contributors");
    attachSystemTable<StorageSystemUsers>(system_database, "users");
    attachSystemTable<StorageSystemRoles>(system_database, "roles");
    attachSystemTable<StorageSystemGrants>(system_database, "grants");
    attachSystemTable<StorageSystemRoleGrants>(system_database, "role_grants");
    attachSystemTable<StorageSystemCurrentRoles>(system_database, "current_roles");
    attachSystemTable<StorageSystemEnabledRoles>(system_database, "enabled_roles");
    attachSystemTable<StorageSystemSettingsProfiles>(system_database, "settings_profiles");
    attachSystemTable<StorageSystemSettingsProfileElements>(system_database, "settings_profile_elements");
    attachSystemTable<StorageSystemRowPolicies>(system_database, "row_policies");
    attachSystemTable<StorageSystemQuotas>(system_database, "quotas");
    attachSystemTable<StorageSystemQuotaLimits>(system_database, "quota_limits");
    attachSystemTable<StorageSystemQuotaUsage>(system_database, "quota_usage");
    attachSystemTable<StorageSystemQuotasUsage>(system_database, "quotas_usage");
    attachSystemTable<StorageSystemUserDirectories>(system_database, "user_directories");
    attachSystemTable<StorageSystemPrivileges>(system_database, "privileges");
    attachSystemTable<StorageSystemErrors>(system_database, "errors");
#if !defined(ARCADIA_BUILD)
    attachSystemTable<StorageSystemLicenses>(system_database, "licenses");
    attachSystemTable<StorageSystemTimeZones>(system_database, "time_zones");
#endif
#ifdef OS_LINUX
    attachSystemTable<StorageSystemStackTrace>(system_database, "stack_trace");
#endif
}

void attachSystemTablesServer(IDatabase & system_database, bool has_zookeeper)
{
    attachSystemTablesLocal(system_database);

    attachSystemTable<StorageSystemParts>(system_database, "parts");
    attachSystemTable<StorageSystemDetachedParts>(system_database, "detached_parts");
    attachSystemTable<StorageSystemPartsColumns>(system_database, "parts_columns");
    attachSystemTable<StorageSystemDisks>(system_database, "disks");
    attachSystemTable<StorageSystemStoragePolicies>(system_database, "storage_policies");
    attachSystemTable<StorageSystemProcesses>(system_database, "processes");
    attachSystemTable<StorageSystemMetrics>(system_database, "metrics");
    attachSystemTable<StorageSystemMerges>(system_database, "merges");
    attachSystemTable<StorageSystemMutations>(system_database, "mutations");
    attachSystemTable<StorageSystemReplicas>(system_database, "replicas");
    attachSystemTable<StorageSystemReplicationQueue>(system_database, "replication_queue");
    attachSystemTable<StorageSystemDDLWorkerQueue>(system_database, "distributed_ddl_queue");
    attachSystemTable<StorageSystemDistributionQueue>(system_database, "distribution_queue");
    attachSystemTable<StorageSystemDictionaries>(system_database, "dictionaries");
    attachSystemTable<StorageSystemModels>(system_database, "models");
    attachSystemTable<StorageSystemClusters>(system_database, "clusters");
    attachSystemTable<StorageSystemGraphite>(system_database, "graphite_retentions");
    attachSystemTable<StorageSystemMacros>(system_database, "macros");
    attachSystemTable<StorageSystemReplicatedFetches>(system_database, "replicated_fetches");

    if (has_zookeeper)
        attachSystemTable<StorageSystemZooKeeper>(system_database, "zookeeper");
}

void attachSystemTablesAsync(IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    attachSystemTable<StorageSystemAsynchronousMetrics>(system_database, "asynchronous_metrics", async_metrics);
}

}
