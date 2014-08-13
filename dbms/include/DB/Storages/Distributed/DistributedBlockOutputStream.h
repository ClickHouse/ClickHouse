#pragma once

#include <DB/Storages/StorageDistributed.h>
#include <iostream>

namespace DB
{

class DistributedBlockOutputStream : public IBlockOutputStream
{
public:
	DistributedBlockOutputStream(StorageDistributed & storage, Cluster & cluster, const ASTPtr & query)
	: storage(storage), cluster(cluster), query(query)
	{
	}

	virtual void write(const Block & block) override
	{
		if (storage.getShardingKeyExpr() && cluster.shard_info_vec.size() > 1)
			splitWrite(block, block);
		else
			writeImpl(block);
	}

private:
	void splitWrite(const Block & block, Block block_with_key)
	{
		storage.getShardingKeyExpr()->execute(block_with_key);

		const auto & key_column = block_with_key.getByName(storage.getShardingKeyColumnName()).column;
		const auto total_weight = cluster.slot_to_shard.size();

		/// shard => block mapping
		std::unordered_map<size_t, Block> target_blocks;
		/// return iterator to target block, creating one if necessary
		const auto get_target_block = [&target_blocks, &block] (const size_t idx) {
			const auto it = target_blocks.find(idx);

			if (it == std::end(target_blocks))
				return target_blocks.emplace(idx, block.cloneEmpty()).first;

			return it;
		};

		for (size_t row = 0; row < block.rows(); ++row)
		{
			const auto target_block_idx = cluster.slot_to_shard[key_column->get64(row) % total_weight];
			auto & target_block = get_target_block(target_block_idx)->second;;

			for (size_t col = 0; col < block.columns(); ++col)
			{
				target_block.getByPosition(col).column->insertFrom(
					*block.getByPosition(col).column, row
				);
			}
		}

		for (const auto & shard_block_pair : target_blocks)
			writeImpl(shard_block_pair.second, shard_block_pair.first);
	}

	void writeImpl(const Block & block, const size_t shard_id = 0)
	{
		const auto & dir_name = cluster.shard_info_vec[shard_id].dir_name;

		/// ensure shard subdirectory creation and notify storage if necessary
		if (Poco::File(storage.getPath() + dir_name).createDirectory())
			storage.createDirectoryMonitor(dir_name);

		std::cout << "dummy write block of " << block.bytes() << " bytes to shard " << shard_id << std::endl;
	}

	StorageDistributed & storage;
	Cluster & cluster;
	ASTPtr query;
};

}
