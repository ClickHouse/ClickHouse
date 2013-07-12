#pragma once

#include <DB/Storages/StorageMergeTree.h>

namespace DB
{
	
/** Для записи куска, полученного слиянием нескольких других.
	* Данные уже отсортированы, относятся к одному месяцу, и пишутся в один кускок.
	*/
class MergedBlockOutputStream : public IBlockOutputStream
{
public:
	MergedBlockOutputStream(StorageMergeTree & storage_,
							UInt16 min_date, UInt16 max_date, UInt64 min_part_id, UInt64 max_part_id, UInt32 level)
	: storage(storage_), marks_count(0), index_offset(0)
	{
		part_name = storage.getPartName(
			Yandex::DayNum_t(min_date), Yandex::DayNum_t(max_date),
										min_part_id, max_part_id, level);
		
		part_tmp_path = storage.full_path + "tmp_" + part_name + "/";
		part_res_path = storage.full_path + part_name + "/";
		
		Poco::File(part_tmp_path).createDirectories();
		
		index_stream = new WriteBufferFromFile(part_tmp_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);
		
		for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
			addStream(it->first, *it->second);
	}
	
	void write(const Block & block)
	{
		size_t rows = block.rows();
		
		/// Сначала пишем индекс. Индекс содержит значение PK для каждой index_granularity строки.
		typedef std::vector<const ColumnWithNameAndType *> PrimaryColumns;
		PrimaryColumns primary_columns;
		
		for (size_t i = 0, size = storage.sort_descr.size(); i < size; ++i)
			primary_columns.push_back(
				!storage.sort_descr[i].column_name.empty()
				? &block.getByName(storage.sort_descr[i].column_name)
				: &block.getByPosition(storage.sort_descr[i].column_number));
			
			for (size_t i = index_offset; i < rows; i += storage.index_granularity)
			{
				for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
				{
					(*it)->type->serializeBinary((*(*it)->column)[i], *index_stream);
				}
				
				++marks_count;
			}
			
			/// Теперь пишем данные.
			for (NamesAndTypesList::const_iterator it = storage.columns->begin(); it != storage.columns->end(); ++it)
			{
				const ColumnWithNameAndType & column = block.getByName(it->first);
				writeData(column.name, *column.type, *column.column);
			}
			
			index_offset = rows % storage.index_granularity
			? (storage.index_granularity - rows % storage.index_granularity)
			: 0;
	}
	
	void writeSuffix()
	{
		/// Заканчиваем запись.
		index_stream = NULL;
		column_streams.clear();
		
		if (marks_count == 0)
			throw Exception("Empty part", ErrorCodes::LOGICAL_ERROR);
		
		/// Переименовываем кусок.
		Poco::File(part_tmp_path).renameTo(part_res_path);
		
		/// А добавление нового куска в набор (и удаление исходных кусков) сделает вызывающая сторона.
	}
	
	/// Сколько засечек уже записано.
	size_t marksCount()
	{
		return marks_count;
	}
	
private:
	StorageMergeTree & storage;
	String part_name;
	String part_tmp_path;
	String part_res_path;
	size_t marks_count;
	
	struct ColumnStream
	{
		ColumnStream(const String & data_path, const std::string & marks_path) :
		plain(data_path, DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY),
		compressed(plain),
		marks(marks_path, 4096, O_TRUNC | O_CREAT | O_WRONLY) {}
		
		WriteBufferFromFile plain;
		CompressedWriteBuffer compressed;
		WriteBufferFromFile marks;
	};
	
	typedef std::map<String, SharedPtr<ColumnStream> > ColumnStreams;
	ColumnStreams column_streams;
	
	SharedPtr<WriteBuffer> index_stream;
	
	/// Смещение до первой строчки блока, для которой надо записать индекс.
	size_t index_offset;
	
	
	void addStream(const String & name, const IDataType & type, size_t level = 0)
	{
		String escaped_column_name = escapeForFileName(name);
		
		/// Для массивов используются отдельные потоки для размеров.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			String escaped_size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			
			column_streams[size_name] = new ColumnStream(
				part_tmp_path + escaped_size_name + ".bin",
				part_tmp_path + escaped_size_name + ".mrk");
			
			addStream(name, *type_arr->getNestedType(), level + 1);
		}
		else if (const DataTypeNested * type_nested = dynamic_cast<const DataTypeNested *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			String escaped_size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			
			column_streams[size_name] = new ColumnStream(
				part_tmp_path + escaped_size_name + ".bin",
				part_tmp_path + escaped_size_name + ".mrk");

			const NamesAndTypesList & columns = *type_nested->getNestedTypesList();
			for (NamesAndTypesList::const_iterator it = columns.begin(); it != columns.end(); ++it)
				addStream(name + "." + it->first, *it->second, level + 1);
		}
		else
			column_streams[name] = new ColumnStream(
				part_tmp_path + escaped_column_name + ".bin",
				part_tmp_path + escaped_column_name + ".mrk");
	}
	
	
	/// Записать данные одного столбца.
	void writeData(const String & name, const IDataType & type, const IColumn & column, size_t level = 0)
	{
		size_t size = column.size();
		
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			
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
					writeIntBinary(stream.plain.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}
				
				type_arr->serializeOffsets(column, stream.compressed, prev_mark, limit);
				prev_mark += limit;
			}
		}
		if (const DataTypeNested * type_nested = dynamic_cast<const DataTypeNested *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			
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
					writeIntBinary(stream.plain.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}
				
				type_nested->serializeOffsets(column, stream.compressed, prev_mark, limit);
				prev_mark += limit;
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
					writeIntBinary(stream.plain.count(), stream.marks);
					writeIntBinary(stream.compressed.offset(), stream.marks);
				}
				
				type.serializeBinary(column, stream.compressed, prev_mark, limit);
				prev_mark += limit;
			}
		}
	}
};

typedef Poco::SharedPtr<MergedBlockOutputStream> MergedBlockOutputStreamPtr;
	
}