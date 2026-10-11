#include <Columns/ColumnString.h>
#include <Columns/IColumn.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Storages/VirtualColumnUtils.h>
#include <Databases/DatabaseReplicated.h>
#include <Databases/PendingReplicasInfo.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

#include <algorithm>
#include <optional>

namespace DB
{

ColumnsDescription StorageSystemClusters::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"cluster", std::make_shared<DataTypeString>(), "The cluster name."},
        {"shard_num", std::make_shared<DataTypeUInt32>(), "The shard number in the cluster, starting from 1."},
        {"shard_name", std::make_shared<DataTypeString>(), "The name of the shard in the cluster."},
        {"shard_weight", std::make_shared<DataTypeUInt32>(), "The relative weight of the shard when writing data."},
        {"internal_replication", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether this host is a part on ensemble which can replicate the data on its own."},
        {"replica_num", std::make_shared<DataTypeUInt32>(), "The replica number in the shard, starting from 1."},
        {"host_name", std::make_shared<DataTypeString>(), "The host name, as specified in the config."},
        {"host_address", std::make_shared<DataTypeString>(), "The host IP address obtained from DNS."},
        {"port", std::make_shared<DataTypeUInt16>(), "The port to use for connecting to the server."},
        {"is_local", std::make_shared<DataTypeUInt8>(), "Flag that indicates whether the host is local."},
        {"user", std::make_shared<DataTypeString>(), "The name of the user for connecting to the server."},
        {"default_database", std::make_shared<DataTypeString>(), "The default database name."},
        {"errors_count", std::make_shared<DataTypeUInt32>(), "The number of times this host failed to reach replica."},
        {"slowdowns_count", std::make_shared<DataTypeUInt32>(), "The number of slowdowns that led to changing replica when establishing a connection with hedged requests."},
        {"estimated_recovery_time", std::make_shared<DataTypeUInt32>(), "Seconds remaining until the replica error count is zeroed and it is considered to be back to normal."},
        {"database_shard_name", std::make_shared<DataTypeString>(), "The name of the `Replicated` database shard (for clusters that belong to a `Replicated` database)."},
        {"database_replica_name", std::make_shared<DataTypeString>(), "The name of the `Replicated` database replica (for clusters that belong to a `Replicated` database)."},
        {"is_shared_catalog_cluster", std::make_shared<DataTypeUInt8>(), "Bool indicating if the cluster belongs to shared catalog."},
        {"is_active", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "The status of the Replicated database replica (for clusters that belong to a Replicated database): 1 means 'replica is online', 0 means 'replica is offline', NULL means 'unknown'."},
           {"unsynced_after_recovery", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "Indicates if a Replicated database replica has replication lag more than max_replication_lag_to_enqueue after creating or recovering the replica."},
        {"replication_lag", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt32>()), "The replication lag of the `Replicated` database replica (for clusters that belong to a Replicated database)."},
        {"recovery_time", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt64>()), "The recovery time of the `Replicated` database replica (for clusters that belong to a Replicated database), in milliseconds."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "cluster"},
    });

    return description;
}

namespace
{

/// A cluster to show, in output order: from the configuration or of a `Replicated` database.
struct ClusterEntry
{
    String name;
    ClusterPtr cluster;
    /// Set for clusters of `Replicated` databases, whose replica state is read from Keeper.
    const DatabaseReplicated * replicated = nullptr;
};

/// The names that pass the part of the `WHERE` clause referring to `cluster` (see `getFilterSampleBlock`), so that
/// `SELECT ... FROM system.clusters WHERE cluster = 'x'` does not go to Keeper for the other clusters.
/// Names are not unique (a cluster from the configuration may have the name of a `Replicated` database), hence a set.
NameSet selectClusterNames(const Strings & names, const ActionsDAG::Node * predicate, const ContextPtr & context)
{
    auto name_column = ColumnString::create();
    for (const auto & name : names)
        name_column->insert(name);

    Block block{ColumnWithTypeAndName(std::move(name_column), std::make_shared<DataTypeString>(), "cluster")};
    VirtualColumnUtils::filterBlockWithPredicate(predicate, block, context);

    NameSet selected;
    const auto & selected_column = block.getByPosition(0).column;
    for (size_t i = 0; i < selected_column->size(); ++i)
        selected.insert(String(selected_column->getDataAt(i)));
    return selected;
}

void writeCluster(
    MutableColumns & res_columns,
    const std::vector<UInt8> & columns_mask,
    const String & cluster_name,
    const Cluster & cluster,
    const ReplicasInfo & replicas_info)
{
    const auto & shards_info = cluster.getShardsInfo();
    const auto & addresses_with_failover = cluster.getShardsAddresses();
    const auto & replicas = replicas_info.replicas;

    size_t replica_idx = 0;
    for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
    {
        const auto & shard_info = shards_info[shard_index];
        const auto & shard_addresses = addresses_with_failover[shard_index];
        const auto pool_status = shard_info.pool->getStatus();

        for (size_t replica_index = 0; replica_index < shard_addresses.size(); ++replica_index)
        {
            size_t src_index = 0;
            size_t res_index = 0;
            const auto & address = shard_addresses[replica_index];

            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(cluster_name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(shard_info.shard_num);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(shard_info.name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(shard_info.weight);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(shard_info.has_internal_replication);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(replica_index + 1);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.host_name);
            if (columns_mask[src_index++])
            {
                auto resolved = address.getResolvedAddress();
                res_columns[res_index++]->insert(resolved ? resolved->host().toString() : String());
            }
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.port);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.is_local);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.user);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.default_database);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(pool_status[replica_index].error_count);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(pool_status[replica_index].slowdown_count);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(pool_status[replica_index].estimated_recovery_time.count());
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.database_shard_name);
            if (columns_mask[src_index++])
                res_columns[res_index++]->insert(address.database_replica_name);

            if (columns_mask[src_index++])
            {
                res_columns[res_index++]->insert(replicas_info.replicas_belong_to_shared_catalog);
            }
            if (columns_mask[src_index++])
            {
                if (replicas.empty())
                    res_columns[res_index++]->insertDefault();
                else
                {
                    const auto & replica_info = replicas[replica_idx];
                    res_columns[res_index++]->insert(replica_info.is_active);
                }
            }
            if (columns_mask[src_index++])
            {
                if (replicas.empty())
                    res_columns[res_index++]->insertDefault();
                else
                {
                    const auto & replica_info = replicas[replica_idx];
                    res_columns[res_index++]->insert(replica_info.unsynced_after_recovery);
                }
            }
            if (columns_mask[src_index++])
            {
                if (replicas.empty())
                    res_columns[res_index++]->insertDefault();
                else
                {
                    const auto & replica_info = replicas[replica_idx];
                    if (replica_info.replication_lag != std::nullopt)
                        res_columns[res_index++]->insert(*replica_info.replication_lag);
                    else
                        res_columns[res_index++]->insertDefault();
                }
            }
            if (columns_mask[src_index++])
            {
                if (replicas.empty())
                    res_columns[res_index++]->insertDefault();
                else
                {
                    const auto & replica_info = replicas[replica_idx];
                    if (replica_info.recovery_time != 0)
                        res_columns[res_index++]->insert(replica_info.recovery_time);
                    else
                        res_columns[res_index++]->insertDefault();
                }
            }

            ++replica_idx;
        }
    }
}

}

Block StorageSystemClusters::getFilterSampleBlock() const
{
    /// Must list every column of the block passed to `filterBlockWithPredicate` in `selectClusterNames`.
    return {
        { {}, std::make_shared<DataTypeString>(), "cluster" },
    };
}

bool StorageSystemClusters::needsReplicasInfo(const std::vector<UInt8> & columns_mask, const ContextPtr & context) const
{
    /// The columns filled from the replica state in Keeper, see `DatabaseReplicated::tryGetReplicasInfo`.
    static const Names replica_state_columns{
        "is_shared_catalog_cluster", "is_active", "unsynced_after_recovery", "replication_lag", "recovery_time"};

    const auto metadata_snapshot = getInMemoryMetadataPtr(context, /* bypass_metadata_cache= */ false);
    const Block sample_block = metadata_snapshot->getSampleBlock();
    return std::ranges::any_of(
        replica_state_columns, [&](const auto & name) { return columns_mask[sample_block.getPositionByName(name)]; });
}

void StorageSystemClusters::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node * predicate, std::vector<UInt8> columns_mask) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemClusters::fillData");
    const bool with_replicas_info = needsReplicasInfo(columns_mask, context);

    /// The filter is applied to the names before the clusters are built: for a `Replicated` database whose
    /// cluster is not cached yet, `tryGetCluster` has to read the topology from Keeper.
    const auto configured_clusters = context->getClusters();
    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});

    /// `databases` keeps the databases alive for the rest of the function.
    std::vector<std::pair<String, const DatabaseReplicated *>> replicated_databases;
    for (const auto & [database_name, database] : databases)
        if (const auto * replicated = typeid_cast<const DatabaseReplicated *>(database.get()))
            replicated_databases.emplace_back(database_name, replicated);

    Strings names;
    for (const auto & name_and_cluster : configured_clusters)
        names.push_back(name_and_cluster.first);
    for (const auto & [database_name, replicated] : replicated_databases)
    {
        names.push_back(database_name);
        names.push_back(DatabaseReplicated::ALL_GROUPS_CLUSTER_PREFIX + database_name);
    }
    const NameSet selected_names = selectClusterNames(names, predicate, context);

    std::vector<ClusterEntry> entries;
    for (const auto & [name, cluster] : configured_clusters)
    {
        if (selected_names.contains(name))
            entries.push_back({.name = name, .cluster = cluster});
    }
    for (const auto & [database_name, replicated] : replicated_databases)
    {
        if (selected_names.contains(database_name))
            if (auto cluster = replicated->tryGetCluster())
                entries.push_back({.name = database_name, .cluster = std::move(cluster), .replicated = replicated});

        const String all_groups_name = DatabaseReplicated::ALL_GROUPS_CLUSTER_PREFIX + database_name;
        if (selected_names.contains(all_groups_name))
            if (auto cluster = replicated->tryGetAllGroupsCluster())
                entries.push_back({.name = all_groups_name, .cluster = std::move(cluster), .replicated = replicated});
    }

    /// Send the Keeper requests of all `Replicated` databases first and await them one by one while writing
    /// the rows: the requests are in flight together, one round trip for all databases instead of one each.
    std::vector<PendingReplicasInfo> pending_replicas_info(entries.size());
    if (with_replicas_info)
    {
        for (size_t i = 0; i < entries.size(); ++i)
            if (entries[i].replicated)
                pending_replicas_info[i] = entries[i].replicated->requestReplicasInfo(entries[i].cluster);
    }

    for (size_t i = 0; i < entries.size(); ++i)
    {
        const ReplicasInfo replicas_info
            = entries[i].replicated ? entries[i].replicated->awaitReplicasInfo(std::move(pending_replicas_info[i])) : ReplicasInfo{};
        writeCluster(res_columns, columns_mask, entries[i].name, *entries[i].cluster, replicas_info);
    }

#if CLICKHOUSE_CLOUD
    if (SharedDatabaseCatalog::initialized())
    {
        const auto cluster_name = SharedDatabaseCatalog::instance().getClusterName();
        const Strings catalog_cluster_names{cluster_name, SharedDatabaseCatalog::ALL_GROUPS_CLUSTER_PREFIX + cluster_name};
        const NameSet selected_catalog_cluster_names = selectClusterNames(catalog_cluster_names, predicate, context);
        for (const auto & name : catalog_cluster_names)
        {
            if (!selected_catalog_cluster_names.contains(name))
                continue;

            if (auto catalog_cluster = SharedDatabaseCatalog::instance().getCluster(name))
                writeCluster(
                    res_columns,
                    columns_mask,
                    name,
                    *catalog_cluster,
                    with_replicas_info ? SharedDatabaseCatalog::instance().tryGetReplicasInfo(catalog_cluster) : ReplicasInfo{});
        }
    }
#endif
}

}

/// Register the source file of this system table for `system.documentation`.
namespace DB { REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemClusters) }
