#include <Storages/System/StorageSystemShards.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/Clusters/ClusterFactory.h>
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
            "replica_collections",
            std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
            "Ordered list of named collections used as replicas for this shard.",
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

    for (const auto & row : ClusterFactory::instance().listShardsForSystemTable())
    {
        res_columns[0]->insert(row.name);

        Array replica_cols;
        replica_cols.reserve(row.replica_collections.size());
        for (const auto & c : row.replica_collections)
            replica_cols.push_back(c);
        res_columns[1]->insert(replica_cols);

        res_columns[2]->insert(row.weight);
        res_columns[3]->insert(static_cast<UInt8>(row.internal_replication ? 1 : 0));

        Array ref_clusters;
        ref_clusters.reserve(row.referenced_by_clusters.size());
        for (const auto & c : row.referenced_by_clusters)
            ref_clusters.push_back(c);
        res_columns[4]->insert(ref_clusters);
    }
}

}
