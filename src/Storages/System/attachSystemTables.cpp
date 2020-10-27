#include <Databases/IDatabase.h>
#include <Storages/System/attachSystemTables.h>

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
#if !defined(ARCADIA_BUILD)
    #include <Storages/System/StorageSystemLicenses.h>
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
#include <Storages/System/StorageSystemPrivileges.h>

#ifdef OS_LINUX
#include <Storages/System/StorageSystemStackTrace.h>
#endif


namespace DB
{

///TODO allow store system tables in DatabaseAtomic
void attachSystemTablesLocal(IDatabase & system_database)
{
    system_database.attachTable("one", StorageSystemOne::create("one"));
    system_database.attachTable("numbers", StorageSystemNumbers::create(StorageID("system", "numbers"), false));
    system_database.attachTable("numbers_mt", StorageSystemNumbers::create(StorageID("system", "numbers_mt"), true));
    system_database.attachTable("zeros", StorageSystemZeros::create(StorageID("system", "zeros"), false));
    system_database.attachTable("zeros_mt", StorageSystemZeros::create(StorageID("system", "zeros_mt"), true));
    system_database.attachTable("databases", StorageSystemDatabases::create("databases"));
    system_database.attachTable("tables", StorageSystemTables::create("tables"));
    system_database.attachTable("columns", StorageSystemColumns::create("columns"));
    system_database.attachTable("functions", StorageSystemFunctions::create("functions"));
    system_database.attachTable("events", StorageSystemEvents::create("events"));
    system_database.attachTable("settings", StorageSystemSettings::create("settings"));
    system_database.attachTable("merge_tree_settings", SystemMergeTreeSettings::create("merge_tree_settings"));
    system_database.attachTable("build_options", StorageSystemBuildOptions::create("build_options"));
    system_database.attachTable("formats", StorageSystemFormats::create("formats"));
    system_database.attachTable("table_functions", StorageSystemTableFunctions::create("table_functions"));
    system_database.attachTable("aggregate_function_combinators", StorageSystemAggregateFunctionCombinators::create("aggregate_function_combinators"));
    system_database.attachTable("data_type_families", StorageSystemDataTypeFamilies::create("data_type_families"));
    system_database.attachTable("collations", StorageSystemCollations::create("collations"));
    system_database.attachTable("table_engines", StorageSystemTableEngines::create("table_engines"));
    system_database.attachTable("contributors", StorageSystemContributors::create("contributors"));
    system_database.attachTable("users", StorageSystemUsers::create("users"));
    system_database.attachTable("roles", StorageSystemRoles::create("roles"));
    system_database.attachTable("grants", StorageSystemGrants::create("grants"));
    system_database.attachTable("role_grants", StorageSystemRoleGrants::create("role_grants"));
    system_database.attachTable("current_roles", StorageSystemCurrentRoles::create("current_roles"));
    system_database.attachTable("enabled_roles", StorageSystemEnabledRoles::create("enabled_roles"));
    system_database.attachTable("settings_profiles", StorageSystemSettingsProfiles::create("settings_profiles"));
    system_database.attachTable("settings_profile_elements", StorageSystemSettingsProfileElements::create("settings_profile_elements"));
    system_database.attachTable("row_policies", StorageSystemRowPolicies::create("row_policies"));
    system_database.attachTable("quotas", StorageSystemQuotas::create("quotas"));
    system_database.attachTable("quota_limits", StorageSystemQuotaLimits::create("quota_limits"));
    system_database.attachTable("quota_usage", StorageSystemQuotaUsage::create("quota_usage"));
    system_database.attachTable("quotas_usage", StorageSystemQuotasUsage::create("all_quotas_usage"));
    system_database.attachTable("privileges", StorageSystemPrivileges::create("privileges"));
#if !defined(ARCADIA_BUILD)
    system_database.attachTable("licenses", StorageSystemLicenses::create("licenses"));
#endif
#ifdef OS_LINUX
    system_database.attachTable("stack_trace", StorageSystemStackTrace::create("stack_trace"));
#endif
}

void attachSystemTablesServer(IDatabase & system_database, bool has_zookeeper)
{
    attachSystemTablesLocal(system_database);
    system_database.attachTable("parts", StorageSystemParts::create("parts"));
    system_database.attachTable("detached_parts", createDetachedPartsTable());
    system_database.attachTable("parts_columns", StorageSystemPartsColumns::create("parts_columns"));
    system_database.attachTable("disks", StorageSystemDisks::create("disks"));
    system_database.attachTable("storage_policies", StorageSystemStoragePolicies::create("storage_policies"));
    system_database.attachTable("processes", StorageSystemProcesses::create("processes"));
    system_database.attachTable("metrics", StorageSystemMetrics::create("metrics"));
    system_database.attachTable("merges", StorageSystemMerges::create("merges"));
    system_database.attachTable("mutations", StorageSystemMutations::create("mutations"));
    system_database.attachTable("replicas", StorageSystemReplicas::create("replicas"));
    system_database.attachTable("replication_queue", StorageSystemReplicationQueue::create("replication_queue"));
    system_database.attachTable("distribution_queue", StorageSystemDistributionQueue::create("distribution_queue"));
    system_database.attachTable("dictionaries", StorageSystemDictionaries::create("dictionaries"));
    system_database.attachTable("models", StorageSystemModels::create("models"));
    system_database.attachTable("clusters", StorageSystemClusters::create("clusters"));
    system_database.attachTable("graphite_retentions", StorageSystemGraphite::create("graphite_retentions"));
    system_database.attachTable("macros", StorageSystemMacros::create("macros"));

    if (has_zookeeper)
        system_database.attachTable("zookeeper", StorageSystemZooKeeper::create("zookeeper"));
}

void attachSystemTablesAsync(IDatabase & system_database, AsynchronousMetrics & async_metrics)
{
    system_database.attachTable("asynchronous_metrics", StorageSystemAsynchronousMetrics::create("asynchronous_metrics", async_metrics));
}

}
