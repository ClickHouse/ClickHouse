#include <DB/Storages/System/attachSystemTables.h>

#include <DB/Storages/System/StorageSystemAsynchronousMetrics.h>
#include <DB/Storages/System/StorageSystemBuildOptions.h>
#include <DB/Storages/System/StorageSystemClusters.h>
#include <DB/Storages/System/StorageSystemColumns.h>
#include <DB/Storages/System/StorageSystemDatabases.h>
#include <DB/Storages/System/StorageSystemDictionaries.h>
#include <DB/Storages/System/StorageSystemEvents.h>
#include <DB/Storages/System/StorageSystemFunctions.h>
#include <DB/Storages/System/StorageSystemGraphite.h>
#include <DB/Storages/System/StorageSystemMerges.h>
#include <DB/Storages/System/StorageSystemMetrics.h>
#include <DB/Storages/System/StorageSystemNumbers.h>
#include <DB/Storages/System/StorageSystemOne.h>
#include <DB/Storages/System/StorageSystemParts.h>
#include <DB/Storages/System/StorageSystemProcesses.h>
#include <DB/Storages/System/StorageSystemReplicas.h>
#include <DB/Storages/System/StorageSystemReplicationQueue.h>
#include <DB/Storages/System/StorageSystemSettings.h>
#include <DB/Storages/System/StorageSystemTables.h>
#include <DB/Storages/System/StorageSystemZooKeeper.h>

namespace DB
{
void attachSystemTablesLocal(DatabasePtr system_database)
{
    system_database->attachTable("one", StorageSystemOne::create("one"));
    system_database->attachTable("numbers", StorageSystemNumbers::create("numbers"));
    system_database->attachTable("numbers_mt", StorageSystemNumbers::create("numbers_mt", true));
    system_database->attachTable("databases", StorageSystemDatabases::create("databases"));
    system_database->attachTable("tables", StorageSystemTables::create("tables"));
    system_database->attachTable("columns", StorageSystemColumns::create("columns"));
    system_database->attachTable("functions", StorageSystemFunctions::create("functions"));
    system_database->attachTable("events", StorageSystemEvents::create("events"));
    system_database->attachTable("settings", StorageSystemSettings::create("settings"));
    system_database->attachTable("build_options", StorageSystemBuildOptions::create("build_options"));
}

void attachSystemTablesServer(DatabasePtr system_database, Context * global_context, bool has_zookeeper)
{
    attachSystemTablesLocal(system_database);
    system_database->attachTable("parts", StorageSystemParts::create("parts"));
    system_database->attachTable("processes", StorageSystemProcesses::create("processes"));
    system_database->attachTable("metrics", StorageSystemMetrics::create("metrics"));
    system_database->attachTable("merges", StorageSystemMerges::create("merges"));
    system_database->attachTable("replicas", StorageSystemReplicas::create("replicas"));
    system_database->attachTable("replication_queue", StorageSystemReplicationQueue::create("replication_queue"));
    system_database->attachTable("dictionaries", StorageSystemDictionaries::create("dictionaries"));
    system_database->attachTable("clusters", StorageSystemClusters::create("clusters", *global_context));
    system_database->attachTable("graphite_retentions", StorageSystemGraphite::create("graphite_retentions"));

    if (has_zookeeper)
        system_database->attachTable("zookeeper", StorageSystemZooKeeper::create("zookeeper"));
}

void attachSystemTablesAsync(DatabasePtr system_database, AsynchronousMetrics & async_metrics)
{
    system_database->attachTable("asynchronous_metrics", StorageSystemAsynchronousMetrics::create("asynchronous_metrics", async_metrics));
}
}
