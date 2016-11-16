#include <DB/Storages/System/StorageSystemClusters.h>
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
		{ "cluster",      std::make_shared<DataTypeString>() },
		{ "shard_num",    std::make_shared<DataTypeUInt32>() },
		{ "shard_weight", std::make_shared<DataTypeUInt32>() },
		{ "replica_num",  std::make_shared<DataTypeUInt32>() },
		{ "host_name",    std::make_shared<DataTypeString>() },
		{ "host_address", std::make_shared<DataTypeString>() },
		{ "port",         std::make_shared<DataTypeUInt16>() },
		{ "is_local",     std::make_shared<DataTypeUInt8>() },
		{ "user",         std::make_shared<DataTypeString>() },
		{ "default_database", std::make_shared<DataTypeString>() }
	}
	, context(context_)
{
}

StoragePtr StorageSystemClusters::create(const std::string & name_, Context & context_)
{
	return make_shared(name_, context_);
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

	ColumnPtr cluster_column = std::make_shared<ColumnString>();
	ColumnPtr shard_num_column = std::make_shared<ColumnUInt32>();
	ColumnPtr shard_weight_column = std::make_shared<ColumnUInt32>();
	ColumnPtr replica_num_column = std::make_shared<ColumnUInt32>();
	ColumnPtr host_name_column = std::make_shared<ColumnString>();
	ColumnPtr host_address_column = std::make_shared<ColumnString>();
	ColumnPtr port_column = std::make_shared<ColumnUInt16>();
	ColumnPtr is_local_column = std::make_shared<ColumnUInt8>();
	ColumnPtr user_column = std::make_shared<ColumnString>();
	ColumnPtr default_database_column = std::make_shared<ColumnString>();

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
		is_local_column->insert(static_cast<UInt64>(shard_info.isLocal()));
		user_column->insert(address.user);
		default_database_column->insert(address.default_database);
	};

	auto clusters = context.getClusters().getContainer();
	for (const auto & entry : clusters)
	{
		const std::string cluster_name = entry.first;
		const ClusterPtr cluster = entry.second;
		const auto & addresses = cluster->getShardsAddresses();
		const auto & addresses_with_failover = cluster->getShardsWithFailoverAddresses();
		const auto & shards_info = cluster->getShardsInfo();

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

	Block block
	{
		{cluster_column, std::make_shared<DataTypeString>(), "cluster"},
		{shard_num_column, std::make_shared<DataTypeUInt32>(), "shard_num"},
		{shard_weight_column, std::make_shared<DataTypeUInt32>(), "shard_weight"},
		{replica_num_column, std::make_shared<DataTypeUInt32>(), "replica_num"},
		{host_name_column, std::make_shared<DataTypeString>(), "host_name"},
		{host_address_column, std::make_shared<DataTypeString>(), "host_address"},
		{port_column, std::make_shared<DataTypeUInt16>(), "port"},
		{is_local_column, std::make_shared<DataTypeUInt8>(), "is_local"},
		{user_column, std::make_shared<DataTypeString>(), "user"},
		{default_database_column, std::make_shared<DataTypeString>(), "default_database"}
	};

	return BlockInputStreams{ 1, std::make_shared<OneBlockInputStream>(block) };
}

}
