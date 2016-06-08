#pragma once

#include <DB/IO/createWriteBufferFromFileBase.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/HashingWriteBuffer.h>

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{


class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
	IMergedBlockOutputStream(
		MergeTreeData & storage_,
		size_t min_compress_block_size_,
		size_t max_compress_block_size_,
		CompressionMethod compression_method_,
		size_t aio_threshold_)
		: storage(storage_),
		min_compress_block_size(min_compress_block_size_),
		max_compress_block_size(max_compress_block_size_),
		aio_threshold(aio_threshold_),
		compression_method(compression_method_)
	{
	}

protected:
	using OffsetColumns = std::set<std::string>;

	struct ColumnStream
	{
		ColumnStream(
			const String & escaped_column_name_,
			const String & data_path,
			const std::string & marks_path,
			size_t max_compress_block_size,
			CompressionMethod compression_method,
			size_t estimated_size,
			size_t aio_threshold) :
			escaped_column_name(escaped_column_name_),
			plain_file(createWriteBufferFromFileBase(data_path, estimated_size, aio_threshold, max_compress_block_size)),
			plain_hashing(*plain_file), compressed_buf(plain_hashing, compression_method), compressed(compressed_buf),
			marks_file(marks_path, 4096, O_TRUNC | O_CREAT | O_WRONLY), marks(marks_file) {}

		String escaped_column_name;

		/// compressed -> compressed_buf -> plain_hashing -> plain_file
		std::unique_ptr<WriteBufferFromFileBase> plain_file;
		HashingWriteBuffer plain_hashing;
		CompressedWriteBuffer compressed_buf;
		HashingWriteBuffer compressed;

		/// marks -> marks_file
		WriteBufferFromFile marks_file;
		HashingWriteBuffer marks;

		void finalize()
		{
			compressed.next();
			plain_file->next();
			marks.next();
		}

		void sync()
		{
			plain_file->sync();
			marks_file.sync();
		}

		void addToChecksums(MergeTreeData::DataPart::Checksums & checksums, String name = "")
		{
			if (name == "")
				name = escaped_column_name;

			checksums.files[name + ".bin"].is_compressed = true;
			checksums.files[name + ".bin"].uncompressed_size = compressed.count();
			checksums.files[name + ".bin"].uncompressed_hash = compressed.getHash();
			checksums.files[name + ".bin"].file_size = plain_hashing.count();
			checksums.files[name + ".bin"].file_hash = plain_hashing.getHash();

			checksums.files[name + ".mrk"].file_size = marks.count();
			checksums.files[name + ".mrk"].file_hash = marks.getHash();
		}
	};

	using ColumnStreams = std::map<String, std::unique_ptr<ColumnStream>>;

	void addStream(const String & path, const String & name, const IDataType & type, size_t estimated_size = 0, size_t level = 0, String filename = "")
	{
		String escaped_column_name;
		if (filename.size())
			escaped_column_name = escapeForFileName(filename);
		else
			escaped_column_name = escapeForFileName(name);

		/// Для массивов используются отдельные потоки для размеров.
		if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
		{
			String size_name = DataTypeNested::extractNestedTableName(name)
				+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			String escaped_size_name = escapeForFileName(DataTypeNested::extractNestedTableName(name))
				+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

			column_streams[size_name] = std::make_unique<ColumnStream>(
				escaped_size_name,
				path + escaped_size_name + ".bin",
				path + escaped_size_name + ".mrk",
				max_compress_block_size,
				compression_method,
				estimated_size,
				aio_threshold);

			addStream(path, name, *type_arr->getNestedType(), estimated_size, level + 1);
		}
		else
			column_streams[name] = std::make_unique<ColumnStream>(
				escaped_column_name,
				path + escaped_column_name + ".bin",
				path + escaped_column_name + ".mrk",
				max_compress_block_size,
				compression_method,
				estimated_size,
				aio_threshold);
	}


	/// Записать данные одного столбца.
	void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns, size_t level = 0)
	{
		size_t size = column.size();

		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
		{
			String size_name = DataTypeNested::extractNestedTableName(name)
				+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

			if (offset_columns.count(size_name) == 0)
			{
				offset_columns.insert(size_name);

				ColumnStream & stream = *column_streams[size_name];

				size_t prev_mark = 0;
				while (prev_mark < size)
				{
					size_t limit = 0;

					/// Если есть index_offset, то первая засечка идёт не сразу, а после этого количества строк.
					if (prev_mark == 0 && index_offset != 0)
					{
						limit = index_offset;
					}
					else
					{
						limit = storage.index_granularity;

						/// Уже могло накопиться достаточно данных для сжатия в новый блок.
						if (stream.compressed.offset() >= min_compress_block_size)
							stream.compressed.next();

						writeIntBinary(stream.plain_hashing.count(), stream.marks);
						writeIntBinary(stream.compressed.offset(), stream.marks);
					}

					type_arr->serializeOffsets(column, stream.compressed, prev_mark, limit);

					/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
					stream.compressed.nextIfAtEnd();

					prev_mark += limit;
				}
			}
		}

		{
			ColumnStream & stream = *column_streams[name];

			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				size_t limit = 0;

				/// Если есть index_offset, то первая засечка идёт не сразу, а после этого количества строк.
				if (prev_mark == 0 && index_offset != 0)
				{
					limit = index_offset;
				}
				else
				{
					limit = storage.index_granularity;

					/// Уже могло накопиться достаточно данных для сжатия в новый блок.
					if (stream.compressed.offset() >= min_compress_block_size)
						stream.compressed.next();

					writeIntBinary(stream.plain_hashing.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}

				type.serializeBinary(column, stream.compressed, prev_mark, limit);

				/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
				stream.compressed.nextIfAtEnd();

				prev_mark += limit;
			}
		}
	}

	MergeTreeData & storage;

	ColumnStreams column_streams;

	/// Смещение до первой строчки блока, для которой надо записать индекс.
	size_t index_offset = 0;

	size_t min_compress_block_size;
	size_t max_compress_block_size;

	size_t aio_threshold;

	CompressionMethod compression_method;
};


/** Для записи одного куска.
  * Данные относятся к одному месяцу, и пишутся в один кускок.
  */
class MergedBlockOutputStream : public IMergedBlockOutputStream
{
public:
	MergedBlockOutputStream(
		MergeTreeData & storage_,
		String part_path_,
		const NamesAndTypesList & columns_list_,
		CompressionMethod compression_method)
		: IMergedBlockOutputStream(
			storage_, storage_.context.getSettings().min_compress_block_size,
			storage_.context.getSettings().max_compress_block_size, compression_method,
			storage_.context.getSettings().min_bytes_to_use_direct_io),
		columns_list(columns_list_), part_path(part_path_)
	{
		init();
		for (const auto & it : columns_list)
			addStream(part_path, it.name, *it.type);
	}

	MergedBlockOutputStream(
		MergeTreeData & storage_,
		String part_path_,
		const NamesAndTypesList & columns_list_,
		CompressionMethod compression_method,
		const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
		size_t aio_threshold_)
		: IMergedBlockOutputStream(
			storage_, storage_.context.getSettings().min_compress_block_size,
			storage_.context.getSettings().max_compress_block_size, compression_method,
			aio_threshold_),
		columns_list(columns_list_), part_path(part_path_)
	{
		init();
		for (const auto & it : columns_list)
		{
			size_t estimated_size = 0;
			if (aio_threshold > 0)
			{
				auto it2 = merged_column_to_size_.find(it.name);
				if (it2 != merged_column_to_size_.end())
					estimated_size = it2->second;
			}
			addStream(part_path, it.name, *it.type, estimated_size);
		}
	}

	std::string getPartPath() const
	{
		return part_path;
	}

	/// Если данные заранее отсортированы.
	void write(const Block & block) override
	{
		writeImpl(block, nullptr);
	}

	/** Если данные не отсортированы, но мы заранее вычислили перестановку, после которой они станут сортированными.
	  * Этот метод используется для экономии оперативки, так как не нужно держать одновременно два блока - исходный и отсортированный.
	  */
	void writeWithPermutation(const Block & block, const IColumn::Permutation * permutation)
	{
		writeImpl(block, permutation);
	}

	void writeSuffix() override
	{
		throw Exception("Method writeSuffix is not supported by MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED);
	}

	MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums()
	{
		/// Заканчиваем запись и достаем чексуммы.
		MergeTreeData::DataPart::Checksums checksums;

		if (storage.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
		{
			index_stream->next();
			checksums.files["primary.idx"].file_size = index_stream->count();
			checksums.files["primary.idx"].file_hash = index_stream->getHash();
			index_stream = nullptr;
		}

		for (ColumnStreams::iterator it = column_streams.begin(); it != column_streams.end(); ++it)
		{
			it->second->finalize();
			it->second->addToChecksums(checksums);
		}

		column_streams.clear();

		if (marks_count == 0)
		{
			/// Кусок пустой - все записи удалились.
			Poco::File(part_path).remove(true);
			checksums.files.clear();
			return checksums;
		}

		{
			/// Записываем файл с описанием столбцов.
			WriteBufferFromFile out(part_path + "columns.txt", 4096);
			columns_list.writeText(out);
		}

		{
			/// Записываем файл с чексуммами.
			WriteBufferFromFile out(part_path + "checksums.txt", 4096);
			checksums.write(out);
		}

		return checksums;
	}

	MergeTreeData::DataPart::Index & getIndex()
	{
		return index_columns;
	}

	/// Сколько засечек уже записано.
	size_t marksCount()
	{
		return marks_count;
	}

private:
	void init()
	{
		Poco::File(part_path).createDirectories();

		if (storage.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
		{
			index_file_stream = std::make_unique<WriteBufferFromFile>(
				part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);
			index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
		}
	}

	/** Если задана permutation, то переставляет значения в столбцах при записи.
	  * Это нужно, чтобы не держать целый блок в оперативке для его сортировки.
	  */
	void writeImpl(const Block & block, const IColumn::Permutation * permutation)
	{
		size_t rows = block.rows();

		/// Множество записанных столбцов со смещениями, чтобы не писать общие для вложенных структур столбцы несколько раз
		OffsetColumns offset_columns;

		auto sort_description = storage.getSortDescription();

		/// Сюда будем складывать столбцы, относящиеся к Primary Key, чтобы потом записать индекс.
		std::vector<ColumnWithTypeAndName> primary_columns(sort_description.size());
		std::map<String, size_t> primary_columns_name_to_position;

		for (size_t i = 0, size = sort_description.size(); i < size; ++i)
		{
			const auto & descr = sort_description[i];

			String name = !descr.column_name.empty()
				? descr.column_name
				: block.getByPosition(descr.column_number).name;

			if (!primary_columns_name_to_position.emplace(name, i).second)
				throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

			primary_columns[i] = !descr.column_name.empty()
				? block.getByName(descr.column_name)
				: block.getByPosition(descr.column_number);

			/// Столбцы первичного ключа переупорядочиваем заранее и складываем в primary_columns.
			if (permutation)
				primary_columns[i].column = primary_columns[i].column->permute(*permutation, 0);
		}

		if (index_columns.empty())
		{
			index_columns.resize(sort_description.size());
			for (size_t i = 0, size = sort_description.size(); i < size; ++i)
				index_columns[i] = primary_columns[i].column.get()->cloneEmpty();
		}

		/// Теперь пишем данные.
		for (const auto & it : columns_list)
		{
			const ColumnWithTypeAndName & column = block.getByName(it.name);

			if (permutation)
			{
				auto primary_column_it = primary_columns_name_to_position.find(it.name);
				if (primary_columns_name_to_position.end() != primary_column_it)
				{
					writeData(column.name, *column.type, *primary_columns[primary_column_it->second].column, offset_columns);
				}
				else
				{
					/// Столбцы, не входящие в первичный ключ, переупорядочиваем здесь; затем результат освобождается - для экономии оперативки.
					ColumnPtr permutted_column = column.column->permute(*permutation, 0);
					writeData(column.name, *column.type, *permutted_column, offset_columns);
				}
			}
			else
			{
				writeData(column.name, *column.type, *column.column, offset_columns);
			}
		}

		/// Пишем индекс. Индекс содержит значение Primary Key для каждой index_granularity строки.
		for (size_t i = index_offset; i < rows; i += storage.index_granularity)
		{
			if (storage.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
			{
				for (size_t j = 0, size = primary_columns.size(); j < size; ++j)
				{
					const IColumn & primary_column = *primary_columns[j].column.get();
					index_columns[j].get()->insertFrom(primary_column, i);
					primary_columns[j].type.get()->serializeBinary(primary_column, i, *index_stream);
				}
			}

			++marks_count;
		}

		size_t written_for_last_mark = (storage.index_granularity - index_offset + rows) % storage.index_granularity;
		index_offset = (storage.index_granularity - written_for_last_mark) % storage.index_granularity;
	}

private:
	NamesAndTypesList columns_list;
	String part_path;

	size_t marks_count = 0;

	std::unique_ptr<WriteBufferFromFile> index_file_stream;
	std::unique_ptr<HashingWriteBuffer> index_stream;
	MergeTreeData::DataPart::Index index_columns;
};


/// Записывает только те, столбцы, что лежат в block
class MergedColumnOnlyOutputStream : public IMergedBlockOutputStream
{
public:
	MergedColumnOnlyOutputStream(MergeTreeData & storage_, String part_path_, bool sync_, CompressionMethod compression_method)
		: IMergedBlockOutputStream(
			storage_, storage_.context.getSettings().min_compress_block_size,
			storage_.context.getSettings().max_compress_block_size, compression_method,
			storage_.context.getSettings().min_bytes_to_use_direct_io),
		part_path(part_path_), sync(sync_)
	{
	}

	void write(const Block & block) override
	{
		if (!initialized)
		{
			column_streams.clear();
			for (size_t i = 0; i < block.columns(); ++i)
			{
				addStream(part_path, block.getByPosition(i).name,
					*block.getByPosition(i).type, 0, 0, block.getByPosition(i).name);
			}
			initialized = true;
		}

		size_t rows = block.rows();

		OffsetColumns offset_columns;
		for (size_t i = 0; i < block.columns(); ++i)
		{
			const ColumnWithTypeAndName & column = block.getByPosition(i);
			writeData(column.name, *column.type, *column.column, offset_columns);
		}

		size_t written_for_last_mark = (storage.index_granularity - index_offset + rows) % storage.index_granularity;
		index_offset = (storage.index_granularity - written_for_last_mark) % storage.index_granularity;
	}

	void writeSuffix() override
	{
		throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
	}

	MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums()
	{
		MergeTreeData::DataPart::Checksums checksums;

		for (auto & column_stream : column_streams)
		{
			column_stream.second->finalize();
			if (sync)
				column_stream.second->sync();
			std::string column = escapeForFileName(column_stream.first);
			column_stream.second->addToChecksums(checksums, column);
		}

		column_streams.clear();
		initialized = false;

		return checksums;
	}

private:
	String part_path;

	bool initialized = false;
	bool sync;
};

}
