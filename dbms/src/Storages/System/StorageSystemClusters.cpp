#include <Storages/System/StorageSystemClusters.h>
#include <Interpreters/Cluster.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataStreams/OneBlockInputStream.h>
#include <Interpreters/Context.h>

namespace DB
{


StorageSystemClusters::StorageSystemClusters(const std::string & name_)
    : name(name_)
{
    setColumns(ColumnsDescription({
        { "cluster",      std::make_shared<DataTypeString>() },
        { "shard_num",    std::make_shared<DataTypeUInt32>() },
        { "shard_weight", std::make_shared<DataTypeUInt32>() },
        { "replica_num",  std::make_shared<DataTypeUInt32>() },
        { "host_name",    std::make_shared<DataTypeString>() },
        { "host_address", std::make_shared<DataTypeString>() },
        { "port",         std::make_shared<DataTypeUInt16>() },
        { "is_local",     std::make_shared<DataTypeUInt8>() },
        { "user",         std::make_shared<DataTypeString>() },
        { "default_database", std::make_shared<DataTypeString>() },
    }));
}


BlockInputStreams StorageSystemClusters::read(
    const Names & column_names,
    const SelectQueryInfo &,
    const Context & context,
    QueryProcessingStage::Enum & processed_stage,
    const size_t /*max_block_size*/,
    const unsigned /*num_streams*/)
{
    check(column_names);
    processed_stage = QueryProcessingStage::FetchColumns;

    MutableColumns res_columns = getSampleBlock().cloneEmptyColumns();

    auto updateColumns = [&](const std::string & cluster_name, const Cluster::ShardInfo & shard_info,
                             const Cluster::Address & address)
    {
        size_t i = 0;
        res_columns[i++]->insert(cluster_name);
        res_columns[i++]->insert(static_cast<UInt64>(shard_info.shard_num));
        res_columns[i++]->insert(static_cast<UInt64>(shard_info.weight));
        res_columns[i++]->insert(static_cast<UInt64>(address.replica_num));
        res_columns[i++]->insert(address.host_name);
        res_columns[i++]->insert(address.resolved_address.host().toString());
        res_columns[i++]->insert(static_cast<UInt64>(address.port));
        res_columns[i++]->insert(static_cast<UInt64>(shard_info.isLocal()));
        res_columns[i++]->insert(address.user);
        res_columns[i++]->insert(address.default_database);
    };

    auto clusters = context.getClusters().getContainer();
    for (const auto & entry : clusters)
    {
        const std::string cluster_name = entry.first;
        const ClusterPtr cluster = entry.second;
        const auto & addresses_with_failover = cluster->getShardsAddresses();
        const auto & shards_info = cluster->getShardsInfo();

        if (!addresses_with_failover.empty())
        {
            auto it1 = addresses_with_failover.cbegin();
            auto it2 = shards_info.cbegin();

            while (it1 != addresses_with_failover.cend())
            {
                const auto & addresses = *it1;
                const auto & shard_info = *it2;

                for (const auto & address : addresses)
                    updateColumns(cluster_name, shard_info, address);

                ++it1;
                ++it2;
            }
        }
    }

    return BlockInputStreams(1, std::make_shared<OneBlockInputStream>(getSampleBlock().cloneWithColumns(std::move(res_columns))));
}

}
