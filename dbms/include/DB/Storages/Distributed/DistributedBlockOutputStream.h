#pragma once

#include <DB/Storages/StorageDistributed.h>

#include <DB/Parsers/formatAST.h>

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/DataStreams/NativeBlockOutputStream.h>
#include <DB/Interpreters/InterpreterInsertQuery.h>
#include <DB/Interpreters/Cluster.h>

#include <statdaemons/Increment.h>
#include <memory>
#include <Yandex/Revision.h>

#include <iostream>
#include <type_traits>


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
	template <typename T>
	static std::vector<IColumn::Filter> createFiltersImpl(const size_t num_rows, const IColumn * column, const Cluster & cluster)
	{
		const auto total_weight = cluster.slot_to_shard.size();
		const auto num_shards = cluster.shard_info_vec.size();
		std::vector<IColumn::Filter> filters(num_shards);

		/** Деление отрицательного числа с остатком на положительное, в C++ даёт отрицательный остаток.
		  * Для данной задачи это не подходит. Поэтому, будем обрабатывать знаковые типы как беззнаковые.
		  * Это даёт уже что-то совсем не похожее на деление с остатком, но подходящее для данной задачи.
		  */
		using UnsignedT = typename std::make_unsigned<T>::type;

		/// const columns contain only one value, therefore we do not need to read it at every iteration
		if (column->isConst())
		{
			const auto data = typeid_cast<const ColumnConst<T> *>(column)->getData();
			const auto shard_num = cluster.slot_to_shard[static_cast<UnsignedT>(data) % total_weight];

			for (size_t i = 0; i < num_shards; ++i)
				filters[i].assign(num_rows, static_cast<UInt8>(shard_num == i));
		}
		else
		{
			const auto & data = typeid_cast<const ColumnVector<T> *>(column)->getData();

			for (size_t i = 0; i < num_shards; ++i)
			{
				filters[i].resize(num_rows);
				for (size_t j = 0; j < num_rows; ++j)
					filters[i][j] = cluster.slot_to_shard[static_cast<UnsignedT>(data[j]) % total_weight] == i;
			}
		}

		return filters;
	}

	std::vector<IColumn::Filter> createFilters(Block block)
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
			? (*it->second)(block.rowsInFirstColumn(), key_column.column.get(), storage.cluster)
			: throw Exception{
				"Sharding key expression does not evaluate to an integer type",
				ErrorCodes::TYPE_MISMATCH
			};
	}

	void writeSplit(const Block & block)
	{
		const auto num_cols = block.columns();
		/// cache column pointers for later reuse
		std::vector<const IColumn*> columns(num_cols);
		for (size_t i = 0; i < columns.size(); ++i)
			columns[i] = block.getByPosition(i).column;

		auto filters = createFilters(block);

		const auto num_shards = storage.cluster.shard_info_vec.size();
		for (size_t i = 0; i < num_shards; ++i)
		{
			auto target_block = block.cloneEmpty();

			for (size_t col = 0; col < num_cols; ++col)
				target_block.getByPosition(col).column = columns[col]->filter(filters[i]);

			if (target_block.rowsInFirstColumn())
				writeImpl(target_block, i);
		}
	}

	void writeImpl(const Block & block, const size_t shard_id = 0)
	{
		const auto & shard_info = storage.cluster.shard_info_vec[shard_id];
		if (shard_info.num_local_nodes)
			writeToLocal(block, shard_info.num_local_nodes);

		/// dir_names is empty if shard has only local addresses
		if (!shard_info.dir_names.empty())
			writeToShard(block, shard_info.dir_names);
	}

	void writeToLocal(const Block & block, const size_t repeats)
	{
		InterpreterInsertQuery interp{query_ast, storage.context};

		auto block_io = interp.execute();
		block_io.out->writePrefix();

		for (size_t i = 0; i < repeats; ++i)
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
				NativeBlockOutputStream stream{compress, Revision::get()};

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
