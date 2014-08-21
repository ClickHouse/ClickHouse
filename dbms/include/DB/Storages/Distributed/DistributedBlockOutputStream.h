#pragma once

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/Distributed/queryToString.h>

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>

#include <statdaemons/Increment.h>
#include <statdaemons/stdext.h>

#include <iostream>

namespace DB
{

/** Запись асинхронная - данные сначала записываются на локальную файловую систему, а потом отправляются на удалённые серверы.
 *  Если Distributed таблица использует более одного шарда, то для того, чтобы поддерживалась запись,
 *  при создании таблицы должен быть указан дополнительный параметр у ENGINE - ключ шардирования.
 *  Ключ шардирования - произвольное выражение от столбцов. Например, rand() или UserID.
 *  При записи блок данных разбивается по остатку от деления ключа шардирования на суммарный вес шардов,
 *  и полученные блоки пишутся в сжатом Native формате в отдельные директории для отправки.
 *  Для каждого адреса назначения (каждой директории с данными для отправки), в StorageDistributed создаётся отдельный поток,
 *  который следит за директорией и отправляет данные. */
class DistributedBlockOutputStream : public IBlockOutputStream
{
public:
	DistributedBlockOutputStream(StorageDistributed & storage, const ASTPtr & query_ast)
		: storage(storage), query_ast(query_ast)
	{
	}

	void write(const Block & block) override
	{
		if (storage.getShardingKeyExpr() && storage.cluster.shard_info_vec.size() > 1)
			return writeSplit(block);

		writeImpl(block);
	}

private:
	void writeSplit(const Block & block)
	{
		auto block_with_key = block;
		storage.getShardingKeyExpr()->execute(block_with_key);

		const auto & key_column = block_with_key.getByName(storage.getShardingKeyColumnName()).column;
		const auto total_weight = storage.cluster.slot_to_shard.size();

		/// shard => block mapping
		std::vector<std::unique_ptr<Block>> target_blocks(storage.cluster.shard_info_vec.size());

		const auto num_cols = block.columns();
		std::vector<const IColumn*> columns(num_cols);
		for (size_t i = 0; i < columns.size(); ++i)
			columns[i] = block.getByPosition(i).column;

		for (size_t num_rows = block.rowsInFirstColumn(), row = 0; row < num_rows; ++row)
		{
			const auto target_block_idx = storage.cluster.slot_to_shard[key_column->get64(row) % total_weight];
			auto & target_block = target_blocks[target_block_idx];
			if (!target_block)
				target_block = stdext::make_unique<Block>(block.cloneEmpty());

			for (size_t col = 0; col < num_cols; ++col)
				target_block->getByPosition(col).column->insertFrom(*columns[col], row);
		}

		for (size_t i = 0; i < target_blocks.size(); ++i)
			if (const auto & target_block = target_blocks[i])
				writeImpl(*target_block, i);
	}

	void writeImpl(const Block & block, const size_t shard_id = 0)
	{
		const auto & shard_info = storage.cluster.shard_info_vec[shard_id];
		if (shard_info.has_local_node)
			writeToLocal(block);

		/// dir_names is empty if shard has only local addresses
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

		auto first = true;
		const auto & query_string = queryToString<ASTInsertQuery>(query_ast);

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
				NativeBlockOutputStream stream{compress};

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

	StorageDistributed & storage;
	ASTPtr query_ast;
};

}
