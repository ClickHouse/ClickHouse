#include <DB/Storages/Distributed/DistributedBlockOutputStream.h>
#include <DB/Storages/StorageDistributed.h>

#include <DB/Parsers/formatAST.h>
#include <DB/Parsers/queryToString.h>

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>
#include <DB/Interpreters/Cluster.h>
#include <DB/Common/BlockFilterCreator.h>

#include <DB/Common/Increment.h>
#include <memory>
#include <common/ClickHouseRevision.h>

#include <iostream>

namespace DB
{

namespace
{

template <typename T>
std::vector<IColumn::Filter> createFiltersImpl(const size_t num_rows, const IColumn * column, const Cluster & cluster)
{
	return BlockFilterCreator<T>::perform(num_rows, column, cluster.getShardsInfo().size(), cluster.getSlotToShard());
}

}

DistributedBlockOutputStream::DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast, const ClusterPtr & cluster_)
	: storage(storage), query_ast(query_ast), cluster(cluster_)
{
}

void DistributedBlockOutputStream::write(const Block & block)
{
	if (storage.getShardingKeyExpr() && (cluster->getShardsInfo().size() > 1))
		return writeSplit(block);

	writeImpl(block);
}

std::vector<IColumn::Filter> DistributedBlockOutputStream::createFilters(Block block)
{
	using create_filters_sig = std::vector<IColumn::Filter>(size_t, const IColumn *, const Cluster &);
	/// hashmap of pointers to functions corresponding to each integral type
	static std::unordered_map<std::string, create_filters_sig *> creators{
		{ TypeName<UInt8>::get(), &createFiltersImpl<UInt8> },
		{ TypeName<UInt16>::get(), &createFiltersImpl<UInt16> },
		{ TypeName<UInt32>::get(), &createFiltersImpl<UInt32> },
		{ TypeName<UInt64>::get(), &createFiltersImpl<UInt64> },
		{ TypeName<Int8>::get(), &createFiltersImpl<Int8> },
		{ TypeName<Int16>::get(), &createFiltersImpl<Int16> },
		{ TypeName<Int32>::get(), &createFiltersImpl<Int32> },
		{ TypeName<Int64>::get(), &createFiltersImpl<Int64> },
	};

	storage.getShardingKeyExpr()->execute(block);

	const auto & key_column = block.getByName(storage.getShardingKeyColumnName());

	/// check that key column has valid type
	const auto it = creators.find(key_column.type->getName());

	return it != std::end(creators)
		? (*it->second)(block.rowsInFirstColumn(), key_column.column.get(), *cluster)
		: throw Exception{
			"Sharding key expression does not evaluate to an integer type",
			ErrorCodes::TYPE_MISMATCH
		};
}

void DistributedBlockOutputStream::writeSplit(const Block & block)
{
	const auto num_cols = block.columns();
	/// cache column pointers for later reuse
	std::vector<const IColumn *> columns(num_cols);
	for (size_t i = 0; i < columns.size(); ++i)
		columns[i] = block.getByPosition(i).column.get();

	auto filters = createFilters(block);

	const auto num_shards = cluster->getShardsInfo().size();

	ssize_t size_hint = ((block.rowsInFirstColumn() + num_shards - 1) / num_shards) * 1.1;	/// Число 1.1 выбрано наугад.

	for (size_t i = 0; i < num_shards; ++i)
	{
		auto target_block = block.cloneEmpty();

		for (size_t col = 0; col < num_cols; ++col)
			target_block.getByPosition(col).column = columns[col]->filter(filters[i], size_hint);

		if (target_block.rowsInFirstColumn())
			writeImpl(target_block, i);
	}
}

void DistributedBlockOutputStream::writeImpl(const Block & block, const size_t shard_id)
{
	const auto & shard_info = cluster->getShardsInfo()[shard_id];
	if (shard_info.getLocalNodeCount() > 0)
		writeToLocal(block, shard_info.getLocalNodeCount());

	/// dir_names is empty if shard has only local addresses
	if (!shard_info.dir_names.empty())
		writeToShard(block, shard_info.dir_names);
}

void DistributedBlockOutputStream::writeToLocal(const Block & block, const size_t repeats)
{
	InterpreterInsertQuery interp{query_ast, storage.context};

	auto block_io = interp.execute();
	block_io.out->writePrefix();

	for (size_t i = 0; i < repeats; ++i)
		block_io.out->write(block);

	block_io.out->writeSuffix();
}

void DistributedBlockOutputStream::writeToShard(const Block & block, const std::vector<std::string> & dir_names)
{
	/** tmp directory is used to ensure atomicity of transactions
		*  and keep monitor thread out from reading incomplete data
		*/
	std::string first_file_tmp_path{};

	auto first = true;
	const auto & query_string = queryToString(query_ast);

	/// write first file, hardlink the others
	for (const auto & dir_name : dir_names)
	{
		const auto & path = storage.getPath() + dir_name + '/';

		/// ensure shard subdirectory creation and notify storage
		if (Poco::File(path).createDirectory())
			storage.requireDirectoryMonitor(dir_name);

		const auto & file_name = toString(Increment{path + "increment.txt"}.get(true)) + ".bin";
		const auto & block_file_path = path + file_name;

		/** on first iteration write block to a temporary directory for subsequent hardlinking to ensure
			*  the inode is not freed until we're done */
		if (first)
		{
			first = false;

			const auto & tmp_path = path + "tmp/";
			Poco::File(tmp_path).createDirectory();
			const auto & block_file_tmp_path = tmp_path + file_name;

			first_file_tmp_path = block_file_tmp_path;

			WriteBufferFromFile out{block_file_tmp_path};
			CompressedWriteBuffer compress{out};
			NativeBlockOutputStream stream{compress, ClickHouseRevision::get()};

			writeStringBinary(query_string, out);

			stream.writePrefix();
			stream.write(block);
			stream.writeSuffix();
		}

		if (link(first_file_tmp_path.data(), block_file_path.data()))
			throwFromErrno("Could not link " + block_file_path + " to " + first_file_tmp_path);
	}

	/** remove the temporary file, enabling the OS to reclaim inode after all threads
		*  have removed their corresponding files */
	Poco::File(first_file_tmp_path).remove();
}

}
