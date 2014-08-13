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
		/// seems like cloned blocks have the same underlying container
		Blocks target_blocks(cluster.shard_info_vec.size(), block.cloneEmpty());

		for (size_t row = 0; row < block.rows(); ++row)
		{
			const auto target_block_idx = key_column->get64(row) % total_weight;
			auto & target_block = target_blocks[target_block_idx];

			for (size_t col = 0; col < block.columns(); ++col)
			{
				target_block.getByPosition(col).column->insertFrom(
					*block.getByPosition(col).column, row
				);
			}
		}

		for (size_t i = 0; i < target_blocks.size(); ++i)
			writeImpl(target_blocks[i], i);
	}

	void writeImpl(const Block & block, const size_t shard_num = 0)
	{
		std::cout << "dummy write block of " << block.bytes() << " bytes to shard " << shard_num << std::endl;
	}

	StorageDistributed & storage;
	Cluster & cluster;
	ASTPtr query;
};

}
