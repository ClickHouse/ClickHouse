#include <DB/Storages/StorageSystemClusters.h>
#include <DB/Interpreters/Cluster.h>
#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataStreams/OneBlockInputStream.h>
#include <DB/Interpreters/Context.h>

namespace DB
{

StorageSystemClusters::StorageSystemClusters(const std::string & name_, Context & context_)
	: name(name_)
	, columns{
		{ "cluster",      new DataTypeString },
		{ "shard_num",    new DataTypeUInt32 },
		{ "shard_weight", new DataTypeUInt32 },
		{ "replica_num",  new DataTypeUInt32 },
		{ "host_name",    new DataTypeString },
		{ "host_address", new DataTypeString },
		{ "port",         new DataTypeUInt16 },
		{ "user",         new DataTypeString }
	}
	, context(context_)
{
}

StoragePtr StorageSystemClusters::create(const std::string & name_, Context & context_)
{
	context_.initClusters();
	return (new StorageSystemClusters{name_, context_})->thisPtr();
}

BlockInputStreams StorageSystemClusters::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context_,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	check(column_names);
	processed_stage = QueryProcessingStage::FetchColumns;

	ColumnPtr cluster_column = new ColumnString;
	ColumnPtr shard_num_column = new ColumnUInt32;
	ColumnPtr shard_weight_column = new ColumnUInt32;
	ColumnPtr replica_num_column = new ColumnUInt32;
	ColumnPtr host_name_column = new ColumnString;
	ColumnPtr host_address_column = new ColumnString;
	ColumnPtr port_column = new ColumnUInt16;
	ColumnPtr user_column = new ColumnString;

	auto updateColumns = [&](const std::string & cluster_name, const Cluster::ShardInfo & shard_info,
							 const Cluster::Address & address)
	{
		cluster_column->insert(cluster_name);
		shard_num_column->insert(static_cast<UInt64>(shard_info.shard_num));
		shard_weight_column->insert(static_cast<UInt64>(shard_info.weight));
		replica_num_column->insert(static_cast<UInt64>(address.replica_num));

		host_name_column->insert(address.host_name);
		host_address_column->insert(address.resolved_address.host().toString());
		port_column->insert(static_cast<UInt64>(address.port));
		user_column->insert(address.user);
	};

	const auto & clusters = context.getClusters();
	for (const auto & entry : clusters->impl)
	{
		const std::string cluster_name = entry.first;
		const Cluster & cluster = entry.second;
		const auto & addresses = cluster.getShardsInfo();
		const auto & addresses_with_failover = cluster.getShardsWithFailoverInfo();
		const auto & shards_info = cluster.shard_info_vec;

		if (!addresses.empty())
		{
			auto it1 = addresses.cbegin();
			auto it2 = shards_info.cbegin();

			while (it1 != addresses.cend())
			{
				const auto & address = *it1;
				const auto & shard_info = *it2;

				updateColumns(cluster_name, shard_info, address);

				++it1;
				++it2;
			}
		}
		else if (!addresses_with_failover.empty())
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

	Block block;

	block.insert(ColumnWithTypeAndName(cluster_column, new DataTypeString, "cluster"));
	block.insert(ColumnWithTypeAndName(shard_num_column, new DataTypeUInt32, "shard_num"));
	block.insert(ColumnWithTypeAndName(shard_weight_column, new DataTypeUInt32, "shard_weight"));
	block.insert(ColumnWithTypeAndName(replica_num_column, new DataTypeUInt32, "replica_num"));
	block.insert(ColumnWithTypeAndName(host_name_column, new DataTypeString, "host_name"));
	block.insert(ColumnWithTypeAndName(host_address_column, new DataTypeString, "host_address"));
	block.insert(ColumnWithTypeAndName(port_column, new DataTypeUInt16, "port"));
	block.insert(ColumnWithTypeAndName(user_column, new DataTypeString, "user"));

	return BlockInputStreams{ 1, new OneBlockInputStream(block) };
}

}
