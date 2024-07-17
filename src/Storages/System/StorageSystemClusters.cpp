#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Databases/DatabaseReplicated.h>

namespace DB
{

ColumnsDescription StorageSystemClusters::getColumnsDescription()
{
    auto description = ColumnsDescription
    {
        {"cluster", std::make_shared<DataTypeString>(), "The cluster name."},
        {"shard_num", std::make_shared<DataTypeUInt32>(), "The shard number in the cluster, starting from 1."},
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
        {"is_active", std::make_shared<DataTypeNullable>(std::make_shared<DataTypeUInt8>()), "The status of the Replicated database replica (for clusters that belong to a Replicated database): 1 means 'replica is online', 0 means 'replica is offline', NULL means 'unknown'."},
    };

    description.setAliases({
        {"name", std::make_shared<DataTypeString>(), "cluster"},
    });

    return description;
}

void StorageSystemClusters::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    for (const auto & name_and_cluster : context->getClusters())
        writeCluster(res_columns, name_and_cluster, {});

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & name_and_database : databases)
    {
        if (const auto * replicated = typeid_cast<const DatabaseReplicated *>(name_and_database.second.get()))
        {

            if (auto database_cluster = replicated->tryGetCluster())
                writeCluster(res_columns, {name_and_database.first, database_cluster},
                             replicated->tryGetAreReplicasActive(database_cluster));

            if (auto database_cluster = replicated->tryGetAllGroupsCluster())
                writeCluster(res_columns, {DatabaseReplicated::ALL_GROUPS_CLUSTER_PREFIX + name_and_database.first, database_cluster},
                             replicated->tryGetAreReplicasActive(database_cluster));
        }
    }
}

void StorageSystemClusters::writeCluster(MutableColumns & res_columns, const NameAndCluster & name_and_cluster,
                                         const std::vector<UInt8> & is_active)
{
    const String & cluster_name = name_and_cluster.first;
    const ClusterPtr & cluster = name_and_cluster.second;
    const auto & shards_info = cluster->getShardsInfo();
    const auto & addresses_with_failover = cluster->getShardsAddresses();

    size_t replica_idx = 0;
    for (size_t shard_index = 0; shard_index < shards_info.size(); ++shard_index)
    {
        const auto & shard_info = shards_info[shard_index];
        const auto & shard_addresses = addresses_with_failover[shard_index];
        const auto pool_status = shard_info.pool->getStatus();

        for (size_t replica_index = 0; replica_index < shard_addresses.size(); ++replica_index)
        {
            size_t i = 0;
            const auto & address = shard_addresses[replica_index];

            res_columns[i++]->insert(cluster_name);
            res_columns[i++]->insert(shard_info.shard_num);
            res_columns[i++]->insert(shard_info.weight);
            res_columns[i++]->insert(shard_info.has_internal_replication);
            res_columns[i++]->insert(replica_index + 1);
            res_columns[i++]->insert(address.host_name);
            auto resolved = address.getResolvedAddress();
            res_columns[i++]->insert(resolved ? resolved->host().toString() : String());
            res_columns[i++]->insert(address.port);
            res_columns[i++]->insert(address.is_local);
            res_columns[i++]->insert(address.user);
            res_columns[i++]->insert(address.default_database);
            res_columns[i++]->insert(pool_status[replica_index].error_count);
            res_columns[i++]->insert(pool_status[replica_index].slowdown_count);
            res_columns[i++]->insert(pool_status[replica_index].estimated_recovery_time.count());
            res_columns[i++]->insert(address.database_shard_name);
            res_columns[i++]->insert(address.database_replica_name);
            if (is_active.empty())
                res_columns[i++]->insertDefault();
            else
                res_columns[i++]->insert(is_active[replica_idx++]);
        }
    }
}
}
