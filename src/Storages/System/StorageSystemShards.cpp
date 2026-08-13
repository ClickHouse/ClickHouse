#include <Storages/System/StorageSystemShards.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Clusters/ClusterMetadataManager.h>
#include <Interpreters/Context.h>
#include <Common/ZooKeeper/ZooKeeperCommon.h>

namespace DB
{

ColumnsDescription StorageSystemShards::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"name", std::make_shared<DataTypeString>(), "Name of the SQL shard (`CREATE SHARD`)."},
        {
            "endpoints",
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Ordered list of endpoint names referenced by this shard.",
        },
        {"weight", std::make_shared<DataTypeUInt32>(), "Shard weight used when this shard is part of a SQL cluster."},
        {
            "internal_replication",
            std::make_shared<DataTypeUInt8>(),
            "Whether distributed writes use internal replication for this shard when used in a SQL cluster.",
        },
        {
            "referenced_by_clusters",
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "SQL cluster names that reference this shard as a member.",
        },
    };
}

void StorageSystemShards::fillData(MutableColumns & res_columns, ContextPtr, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto component_guard = Coordination::setCurrentComponent("StorageSystemShards::fillData");

    for (const auto & shard : ClusterMetadataManager::instance().listShardsForSystemTable())
    {
        res_columns[0]->insert(shard.name);

        Array endpoint_names;
        endpoint_names.reserve(shard.endpoint_names.size());
        for (const auto & endpoint_name : shard.endpoint_names)
            endpoint_names.push_back(endpoint_name);
        res_columns[1]->insert(endpoint_names);

        res_columns[2]->insert(shard.weight);
        res_columns[3]->insert(static_cast<UInt8>(shard.internal_replication ? 1 : 0));

        Array ref_clusters;
        ref_clusters.reserve(shard.referenced_by_clusters.size());
        for (const auto & cluster_name : shard.referenced_by_clusters)
            ref_clusters.push_back(cluster_name);
        res_columns[4]->insert(ref_clusters);
    }
}

}
