#include <Columns/IColumn.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Client/ConnectionPoolWithFailover.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Databases/DatabaseReplicated.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>
#if CLICKHOUSE_CLOUD
#include <Interpreters/SharedDatabaseCatalog.h>
#endif

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

void StorageSystemClusters::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8> columns_mask) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemClusters::fillData");
    for (const auto & name_and_cluster : context->getClusters())
        writeCluster(res_columns, columns_mask, name_and_cluster, /* replicas_info_getter= */ {});

    const auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{.with_datalake_catalogs = false});
    for (const auto & name_and_database : databases)
    {
        if (const auto * replicated = typeid_cast<const DatabaseReplicated *>(name_and_database.second.get()))
        {

            if (auto database_cluster = replicated->tryGetCluster())
                writeCluster(res_columns, columns_mask, {name_and_database.first, database_cluster},
                    [replicated, database_cluster]() -> ReplicasInfo { return replicated->tryGetReplicasInfo(database_cluster); });

            if (auto database_cluster = replicated->tryGetAllGroupsCluster())
                writeCluster(res_columns, columns_mask, {DatabaseReplicated::ALL_GROUPS_CLUSTER_PREFIX + name_and_database.first, database_cluster},
                    [replicated, database_cluster]() -> ReplicasInfo { return replicated->tryGetReplicasInfo(database_cluster); });
        }
    }

#if CLICKHOUSE_CLOUD
    if (SharedDatabaseCatalog::initialized())
    {
        auto cluster_name = SharedDatabaseCatalog::instance().getClusterName();
        if (auto catalog_cluster = SharedDatabaseCatalog::instance().getCluster(cluster_name))
            writeCluster(res_columns, columns_mask, {cluster_name, catalog_cluster},
                         [catalog_cluster]() -> ReplicasInfo { return SharedDatabaseCatalog::instance().tryGetReplicasInfo(catalog_cluster); });

        auto all_groups_cluster_name = SharedDatabaseCatalog::ALL_GROUPS_CLUSTER_PREFIX + cluster_name;
        if (auto catalog_cluster = SharedDatabaseCatalog::instance().getCluster(all_groups_cluster_name))
            writeCluster(res_columns, columns_mask, {all_groups_cluster_name, catalog_cluster},
                         [catalog_cluster]() -> ReplicasInfo { return SharedDatabaseCatalog::instance().tryGetReplicasInfo(catalog_cluster); });
    }
#endif
}

void StorageSystemClusters::writeCluster(MutableColumns & res_columns, const std::vector<UInt8> & columns_mask, const NameAndCluster & name_and_cluster, std::function<ReplicasInfo()> && replicas_info_getter)
{
    const String & cluster_name = name_and_cluster.first;
    const ClusterPtr & cluster = name_and_cluster.second;
    const auto & shards_info = cluster->getShardsInfo();
    const auto & addresses_with_failover = cluster->getShardsAddresses();

    const size_t recovery_time_column_idx = columns_mask.size() - 1;
    const size_t replication_lag_column_idx = columns_mask.size() - 2;
    const size_t is_unsynced_column_idx = columns_mask.size() - 3;
    const size_t is_active_column_idx = columns_mask.size() - 4;
    const size_t is_shared_catalog_cluster_idx = columns_mask.size() - 5;
    std::vector<ReplicaInfo> replicas;
    bool replicas_belong_to_shared_catalog = false;
    if (replicas_info_getter && (columns_mask[recovery_time_column_idx] || columns_mask[replication_lag_column_idx] || columns_mask[is_unsynced_column_idx] || columns_mask[is_active_column_idx] || columns_mask[is_shared_catalog_cluster_idx]))
    {
        const ReplicasInfo info = replicas_info_getter();
        replicas = info.replicas;
        replicas_belong_to_shared_catalog = info.replicas_belong_to_shared_catalog;
    }

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

            /// make sure these five columns remain the last ones, see is_active_column_idx, etc
            if (columns_mask[src_index++])
            {
                res_columns[res_index++]->insert(replicas_belong_to_shared_catalog);
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
