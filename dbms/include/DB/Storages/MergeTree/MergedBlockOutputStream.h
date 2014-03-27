#pragma once

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/HashingWriteBuffer.h>

#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>


namespace DB
{
class IMergedBlockOutputStream : public IBlockOutputStream
{
public:
	IMergedBlockOutputStream(MergeTreeData & storage_) : storage(storage_), index_offset(0)
	{
	}

protected:
	typedef std::set<std::string> OffsetColumns;
	struct ColumnStream
	{
		ColumnStream(const String & escaped_column_name_, const String & data_path, const std::string & marks_path) :
			escaped_column_name(escaped_column_name_),
			plain_file(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY),
			compressed_buf(plain_file),
			marks_file(marks_path, 4096, O_TRUNC | O_CREAT | O_WRONLY),
			compressed(compressed_buf), marks(marks_file) {}

		String escaped_column_name;
		WriteBufferFromFile plain_file;
		CompressedWriteBuffer compressed_buf;
		WriteBufferFromFile marks_file;
		HashingWriteBuffer compressed;
		HashingWriteBuffer marks;

		void finalize()
		{
			compressed.next();
			plain_file.next();
			marks.next();
		}

		void sync()
		{
			plain_file.sync();
			marks_file.sync();
		}

		void addToChecksums(MergeTreeData::DataPart::Checksums & checksums, String name = "")
		{
			if (name == "")
				name = escaped_column_name;
			checksums.files[name + ".bin"].size = compressed.count();
			checksums.files[name + ".bin"].hash = compressed.getHash();
			checksums.files[name + ".mrk"].size = marks.count();
			checksums.files[name + ".mrk"].hash = marks.getHash();
		}
	};

	typedef std::map<String, SharedPtr<ColumnStream> > ColumnStreams;

	void addStream(const String & path, const String & name, const IDataType & type, size_t level = 0, String filename = "")
	{
		String escaped_column_name;
		if (filename.size())
			escaped_column_name = escapeForFileName(filename);
		else
			escaped_column_name = escapeForFileName(name);

		/// Для массивов используются отдельные потоки для размеров.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = DataTypeNested::extractNestedTableName(name)
				+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			String escaped_size_name = escapeForFileName(DataTypeNested::extractNestedTableName(name))
				+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

			column_streams[size_name] = new ColumnStream(
				escaped_size_name,
				path + escaped_size_name + ".bin",
				path + escaped_size_name + ".mrk");

			addStream(path, name, *type_arr->getNestedType(), level + 1);
		}
		else
			column_streams[name] = new ColumnStream(
				escaped_column_name,
				path + escaped_column_name + ".bin",
				path + escaped_column_name + ".mrk");
	}


	/// Записать данные одного столбца.
	void writeData(const String & name, const IDataType & type, const IColumn & column, OffsetColumns & offset_columns, size_t level = 0)
	{
		size_t size = column.size();

		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
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
						writeIntBinary(stream.plain_file.count(), stream.marks);
						writeIntBinary(stream.compressed.offset(), stream.marks);
					}

					type_arr->serializeOffsets(column, stream.compressed, prev_mark, limit);
					stream.compressed.nextIfAtEnd();	/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
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
					writeIntBinary(stream.plain_file.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}

				type.serializeBinary(column, stream.compressed, prev_mark, limit);
				stream.compressed.nextIfAtEnd();	/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
				prev_mark += limit;
			}
		}
	}

	MergeTreeData & storage;

	ColumnStreams column_streams;

	/// Смещение до первой строчки блока, для которой надо записать индекс.
	size_t index_offset;
};

/** Для записи одного куска. Данные уже отсортированы, относятся к одному месяцу, и пишутся в один кускок.
  */
class MergedBlockOutputStream : public IMergedBlockOutputStream
{
public:
	MergedBlockOutputStream(MergeTreeData & storage_, String part_path_, const NamesAndTypesList & columns_list_)
		: IMergedBlockOutputStream(storage_), columns_list(columns_list_), part_path(part_path_), marks_count(0)
	{
		Poco::File(part_path).createDirectories();
		
		index_file_stream = new WriteBufferFromFile(part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);
		index_stream = new HashingWriteBuffer(*index_file_stream);
		
		columns_list = storage.getColumnsList();
		for (const auto & it : columns_list)
			addStream(part_path, it.first, *it.second);
	}

	void write(const Block & block)
	{
		size_t rows = block.rows();
		
		/// Сначала пишем индекс. Индекс содержит значение Primary Key для каждой index_granularity строки.
		typedef std::vector<const ColumnWithNameAndType *> PrimaryColumns;
		PrimaryColumns primary_columns;
		
		for (const auto & descr : storage.getSortDescription())
			primary_columns.push_back(
				!descr.column_name.empty()
				? &block.getByName(descr.column_name)
				: &block.getByPosition(descr.column_number));
			
		for (size_t i = index_offset; i < rows; i += storage.index_granularity)
		{
			for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
			{
				index_vec.push_back((*(*it)->column)[i]);
				(*it)->type->serializeBinary(index_vec.back(), *index_stream);
			}
			
			++marks_count;
		}
		
		/// Множество записанных столбцов со смещениями, чтобы не писать общие для вложенных структур столбцы несколько раз
		OffsetColumns offset_columns;
		
		/// Теперь пишем данные.
		for (const auto & it : columns_list)
		{
			const ColumnWithNameAndType & column = block.getByName(it.first);
			writeData(column.name, *column.type, *column.column, offset_columns);
		}

		size_t written_for_last_mark = (storage.index_granularity - index_offset + rows) % storage.index_granularity;
		index_offset = (storage.index_granularity - written_for_last_mark) % storage.index_granularity;
	}

	void writeSuffix() override
	{
		throw Exception("Method writeSuffix is not supported by MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED);
	}
	
	MergeTreeData::DataPart::Checksums writeSuffixAndGetChecksums()
	{
		/// Заканчиваем запись и достаем чексуммы.
		MergeTreeData::DataPart::Checksums checksums;

		index_stream->next();
		checksums.files["primary.idx"].size = index_stream->count();
		checksums.files["primary.idx"].hash = index_stream->getHash();

		for (ColumnStreams::iterator it = column_streams.begin(); it != column_streams.end(); ++it)
		{
			it->second->finalize();
			it->second->addToChecksums(checksums);
		}

		index_stream = NULL;
		column_streams.clear();

		if (marks_count == 0)
		{
			/// Кусок пустой - все записи удалились.
			Poco::File(part_path).remove(true);
			checksums.files.clear();
		}
		else
		{
			/// Записываем файл с чексуммами.
			WriteBufferFromFile out(part_path + "checksums.txt", 1024);
			checksums.writeText(out);
		}

		return checksums;
	}

	MergeTreeData::DataPart::Index & getIndex()
	{
		return index_vec;
	}
	
	/// Сколько засечек уже записано.
	size_t marksCount()
	{
		return marks_count;
	}

private:
	NamesAndTypesList columns_list;
	String part_path;

	size_t marks_count;

	SharedPtr<WriteBufferFromFile> index_file_stream;
	SharedPtr<HashingWriteBuffer> index_stream;
	MergeTreeData::DataPart::Index index_vec;
};

typedef Poco::SharedPtr<MergedBlockOutputStream> MergedBlockOutputStreamPtr;

/// Записывает только те, столбцы, что лежат в block
class MergedColumnOnlyOutputStream : public IMergedBlockOutputStream
{
public:
	MergedColumnOnlyOutputStream(MergeTreeData & storage_, String part_path_, bool sync_ = false) :
		IMergedBlockOutputStream(storage_), part_path(part_path_), initialized(false), sync(sync_)
	{
	}

	void write(const Block & block)
	{
		if (!initialized)
		{
			column_streams.clear();
			for (size_t i = 0; i < block.columns(); ++i)
			{
				addStream(part_path, block.getByPosition(i).name,
					*block.getByPosition(i).type, 0, prefix + block.getByPosition(i).name);
			}
			initialized = true;
		}

		size_t rows = block.rows();

		OffsetColumns offset_columns;
		for (size_t i = 0; i < block.columns(); ++i)
		{
			const ColumnWithNameAndType & column = block.getByPosition(i);
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
			Poco::File(part_path + prefix + column + ".bin").renameTo(part_path + column + ".bin");
			Poco::File(part_path + prefix + column + ".mrk").renameTo(part_path + column + ".mrk");
		}

		column_streams.clear();
		initialized = false;

		return checksums;
	}

private:
	String part_path;

	bool initialized;

	const std::string prefix = "tmp_";

	bool sync;
};
	
}
