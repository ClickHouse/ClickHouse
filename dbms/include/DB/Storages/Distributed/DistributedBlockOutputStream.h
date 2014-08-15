#pragma once

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/Distributed/queryToString.h>

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>

#include <statdaemons/Increment.h>

#include <iostream>

namespace DB
{

class DistributedBlockOutputStream : public IBlockOutputStream
{
public:
	DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast)
	: storage(storage), query_ast(query_ast)
	{
	}

	virtual void write(const Block & block) override
	{
		if (storage.getShardingKeyExpr() && storage.cluster.shard_info_vec.size() > 1)
			return writeSplit(block, block);

		writeImpl(block);
	}

private:
	void writeSplit(const Block & block, Block block_with_key)
	{
		storage.getShardingKeyExpr()->execute(block_with_key);

		const auto & key_column = block_with_key.getByName(storage.getShardingKeyColumnName()).column;
		const auto total_weight = storage.cluster.slot_to_shard.size();

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
			const auto target_block_idx = storage.cluster.slot_to_shard[key_column->get64(row) % total_weight];
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
		const auto & shard_info = storage.cluster.shard_info_vec[shard_id];
		if (shard_info.has_local_node)
			writeToLocal(block);

		if (!shard_info.dir_names.empty())
			writeToShard(block, shard_info.dir_names);
	}

	void writeToLocal(const Block & block)
	{
		InterpreterInsertQuery interp{query_ast, storage.context};

		auto block_io = interp.execute();
		block_io.out->writePrefix();
		block_io.out->write(block);
		block_io.out->writeSuffix();
	}

	void writeToShard(const Block & block, const std::vector<std::string> & dir_names)
	{
		/** tmp directory is used to ensure atomicity of transactions
		 *  and keep monitor thread out from reading incomplete data
		 */
		std::string first_file_tmp_path{};
		std::string first_file_path{};
		auto first = true;
		const auto & query_string = queryToString<ASTInsertQuery>(query_ast);

		/// write first file, hardlink the others
		for (const auto & dir_name : dir_names)
		{
			const auto & path = storage.getPath() + dir_name + '/';

			/// ensure shard subdirectory creation and notify storage if necessary
			if (Poco::File(path).createDirectory())
				storage.createDirectoryMonitor(dir_name);

			const auto & file_name = std::to_string(Increment(path + "increment.txt").get(true)) + ".bin";
			const auto & block_file_path = path + file_name;

			if (first)
			{
				first = false;

				const auto & tmp_path = path + "tmp/";
				Poco::File(tmp_path).createDirectory();
				const auto & block_file_tmp_path = tmp_path + file_name;

				first_file_path = block_file_path;
				first_file_tmp_path = block_file_tmp_path;

				DB::WriteBufferFromFile out{block_file_tmp_path};
				DB::CompressedWriteBuffer compress{out};
				DB::NativeBlockOutputStream stream{compress};

				DB::writeStringBinary(query_string, out);

				stream.writePrefix();
				stream.write(block);
				stream.writeSuffix();
			}
			else if (link(first_file_tmp_path.data(), block_file_path.data()))
				throw Exception{"Could not link " + block_file_path + " to " + first_file_tmp_path};
		}

		Poco::File(first_file_tmp_path).renameTo(first_file_path);
	}

	StorageDistributed & storage;
	ASTPtr query_ast;
};

}
