#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/Context.h>
#include <Storages/System/StorageSystemClusters.h>
#include <Databases/DatabaseReplicated.h>

namespace DB
{

NamesAndTypesList StorageSystemClusters::getNamesAndTypes()
{
    return
    {
        {"cluster", std::make_shared<DataTypeString>()},
        {"shard_num", std::make_shared<DataTypeUInt32>()},
        {"shard_weight", std::make_shared<DataTypeUInt32>()},
        {"replica_num", std::make_shared<DataTypeUInt32>()},
        {"host_name", std::make_shared<DataTypeString>()},
        {"host_address", std::make_shared<DataTypeString>()},
        {"port", std::make_shared<DataTypeUInt16>()},
        {"is_local", std::make_shared<DataTypeUInt8>()},
        {"user", std::make_shared<DataTypeString>()},
        {"default_database", std::make_shared<DataTypeString>()},
        {"errors_count", std::make_shared<DataTypeUInt32>()},
        {"slowdowns_count", std::make_shared<DataTypeUInt32>()},
        {"estimated_recovery_time", std::make_shared<DataTypeUInt32>()}
    };
}


void StorageSystemClusters::fillData(MutableColumns & res_columns, ContextPtr context, const SelectQueryInfo &) const
{
    for (const auto & name_and_cluster : context->getClusters()->getContainer())
        writeCluster(res_columns, name_and_cluster);

    const auto databases = DatabaseCatalog::instance().getDatabases();
    for (const auto & name_and_database : databases)
    {
        if (const auto * replicated = typeid_cast<const DatabaseReplicated *>(name_and_database.second.get()))
        {
            // A quick fix for stateless tests with DatabaseReplicated. Its ZK
            // node can be destroyed at any time. If another test lists
            // system.clusters to get client command line suggestions, it will
            // get an error when trying to get the info about DB from ZK.
            // Just ignore these inaccessible databases. A good example of a
            // failing test is `01526_client_start_and_exit`.
            try
            {
                writeCluster(res_columns, {name_and_database.first, replicated->getCluster()});
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }
}

void StorageSystemClusters::writeCluster(MutableColumns & res_columns, const NameAndCluster & name_and_cluster)
{
    const String & cluster_name = name_and_cluster.first;
    const ClusterPtr & cluster = name_and_cluster.second;
    const auto & shards_info = cluster->getShardsInfo();
    const auto & addresses_with_failover = cluster->getShardsAddresses();

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
        }
    }
}
}
