#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/MergeTree/PKCondition.h>


#define MERGE_TREE_MARK_SIZE (2 * sizeof(size_t))

namespace DB
{

/// Для чтения из одного куска. Для чтения сразу из многих, Storage использует сразу много таких объектов.
class MergeTreeBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Параметры storage_ и owned_storage разделены, чтобы можно было сделать поток, не владеющий своим storage
	/// (например, поток, сливаящий куски). В таком случае сам storage должен следить, чтобы не удалить данные, пока их читают.
	MergeTreeBlockInputStream(const String & path_,	/// Путь к куску
								size_t block_size_, const Names & column_names_,
						StorageMergeTree & storage_, const StorageMergeTree::DataPartPtr & owned_data_part_,
						const MarkRanges & mark_ranges_, StoragePtr owned_storage)
	: IProfilingBlockInputStream(owned_storage), path(path_), block_size(block_size_), column_names(column_names_),
	storage(storage_), owned_data_part(owned_data_part_),
	mark_ranges(mark_ranges_), current_range(-1), rows_left_in_current_range(0)
	{
		LOG_TRACE(storage.log, "Reading " << mark_ranges.size() << " ranges from part " << owned_data_part->name
		<< ", up to " << (mark_ranges.back().end - mark_ranges.front().begin) * storage.index_granularity
		<< " rows starting from " << mark_ranges.front().begin * storage.index_granularity);
	}
	
	String getName() const { return "MergeTreeBlockInputStream"; }

	String getID() const
	{
		std::stringstream res;
		res << "MergeTree(" << owned_storage->getTableName() << ", " << path << ", columns";

		for (size_t i = 0; i < column_names.size(); ++i)
			res << ", " << column_names[i];

		res << ", marks";

		for (size_t i = 0; i < mark_ranges.size(); ++i)
			res << ", " << mark_ranges[i].begin << ", " << mark_ranges[i].end;

		res << ")";
		return res.str();
	}
	
	/// Получает набор диапазонов засечек, вне которых не могут находиться ключи из заданного диапазона.
	static MarkRanges markRangesFromPkRange(const String & path,
											size_t marks_count,
										StorageMergeTree & storage,
										PKCondition & key_condition)
	{
		MarkRanges res;
		
		/// Если индекс не используется.
		if (key_condition.alwaysTrue())
		{
			res.push_back(MarkRange(0, marks_count));
		}
		else
		{
			/// Читаем индекс.
			typedef AutoArray<Row> Index;
			size_t key_size = storage.sort_descr.size();
			Index index(marks_count);
			for (size_t i = 0; i < marks_count; ++i)
				index[i].resize(key_size);
			
			{
				String index_path = path + "primary.idx";
				ReadBufferFromFile index_file(index_path, std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(index_path).getSize()));
				
				for (size_t i = 0; i < marks_count; ++i)
				{
					for (size_t j = 0; j < key_size; ++j)
						storage.primary_key_sample.getByPosition(j).type->deserializeBinary(index[i][j], index_file);
				}
				
				if (!index_file.eof())
					throw Exception("index file " + index_path + " is unexpectedly long", ErrorCodes::EXPECTED_END_OF_FILE);
			}
			
			/// В стеке всегда будут находиться непересекающиеся подозрительные отрезки, самый левый наверху (back).
			/// На каждом шаге берем левый отрезок и проверяем, подходит ли он.
			/// Если подходит, разбиваем его на более мелкие и кладем их в стек. Если нет - выбрасываем его.
			/// Если отрезок уже длиной в одну засечку, добавляем его в ответ и выбрасываем.
			std::vector<MarkRange> ranges_stack;
			ranges_stack.push_back(MarkRange(0, marks_count));
			while (!ranges_stack.empty())
			{
				MarkRange range = ranges_stack.back();
				ranges_stack.pop_back();
				
				bool may_be_true;
				if (range.end == marks_count)
					may_be_true = key_condition.mayBeTrueAfter(index[range.begin]);
				else
					may_be_true = key_condition.mayBeTrueInRange(index[range.begin], index[range.end]);
				
				if (!may_be_true)
					continue;
				
				if (range.end == range.begin + 1)
				{
					/// Увидели полезный промежуток между соседними засечками. Либо добавим его к последнему диапазону, либо начнем новый диапазон.
					if (res.empty() || range.begin - res.back().end > storage.min_marks_for_seek)
					{
						res.push_back(range);
					}
					else
					{
						res.back().end = range.end;
					}
				}
				else
				{
					/// Разбиваем отрезок и кладем результат в стек справа налево.
					size_t step = (range.end - range.begin - 1) / storage.settings.coarse_index_granularity + 1;
					size_t end;
					for (end = range.end; end > range.begin + step; end -= step)
					{
						ranges_stack.push_back(MarkRange(end - step, end));
					}
					ranges_stack.push_back(MarkRange(range.begin, end));
				}
			}
		}
		
		return res;
	}
	
protected:
	Block readImpl()
	{
		Block res;
		
		/// Если нужно, переходим к следующему диапазону.
		if (rows_left_in_current_range == 0)
		{
			++current_range;
			if (static_cast<size_t>(current_range) == mark_ranges.size())
				return res;
			
			MarkRange & range = mark_ranges[current_range];
			rows_left_in_current_range = (range.end - range.begin) * storage.index_granularity;
			
			for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
				addStream(*it, *storage.getDataTypeByName(*it), range.begin);
		}
		
		/// Сколько строк читать для следующего блока.
		size_t max_rows_to_read = std::min(block_size, rows_left_in_current_range);
		
		/** Для некоторых столбцов файлы с данными могут отсутствовать.
			* Это бывает для старых кусков, после добавления новых столбцов в структуру таблицы.
			*/
		bool has_missing_columns = false;
		bool has_normal_columns = false;
		
		for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it)
		{
			if (streams.end() == streams.find(*it))
			{
				has_missing_columns = true;
				continue;
			}
			
			has_normal_columns = true;
			
			ColumnWithNameAndType column;
			column.name = *it;
			column.type = storage.getDataTypeByName(*it);
			column.column = column.type->createColumn();
			readData(*it, *column.type, *column.column, max_rows_to_read);
			
			if (column.column->size())
				res.insert(column);
		}
		
		if (has_missing_columns && !has_normal_columns)
			throw Exception("All requested columns are missing", ErrorCodes::ALL_REQUESTED_COLUMNS_ARE_MISSING);
		
		if (res)
		{
			rows_left_in_current_range -= res.rows();
			
			/// Заполним столбцы, для которых нет файлов, значениями по-умолчанию.
			if (has_missing_columns)
			{
				size_t pos = 0;	/// Позиция, куда надо вставить недостающий столбец.
				for (Names::const_iterator it = column_names.begin(); it != column_names.end(); ++it, ++pos)
				{
					if (streams.end() == streams.find(*it))
					{
						ColumnWithNameAndType column;
						column.name = *it;
						column.type = storage.getDataTypeByName(*it);
						
						/** Нужно превратить константный столбец в полноценный, так как в части блоков (из других кусков),
							*  он может быть полноценным (а то интерпретатор может посчитать, что он константный везде).
							*/
						column.column = dynamic_cast<IColumnConst &>(*column.type->createConstColumn(
							res.rows(), column.type->getDefault())).convertToFullColumn();
							
							res.insert(pos, column);
					}
				}
			}
		}
		
		if (!res || rows_left_in_current_range == 0)
		{
			rows_left_in_current_range = 0;
			/** Закрываем файлы (ещё до уничтожения объекта).
				* Чтобы при создании многих источников, но одновременном чтении только из нескольких,
				*  буферы не висели в памяти.
				*/
			streams.clear();
		}
		
		return res;
	}
	
private:
	const String path;
	size_t block_size;
	Names column_names;
	StorageMergeTree & storage;
	const StorageMergeTree::DataPartPtr owned_data_part;	/// Кусок не будет удалён, пока им владеет этот объект.
	MarkRanges mark_ranges; /// В каких диапазонах засечек читать.
	
	int current_range; /// Какой из mark_ranges сейчас читаем.
	size_t rows_left_in_current_range; /// Сколько строк уже прочитали из текущего элемента mark_ranges.
	
	struct Stream
	{
		Stream(const String & path_prefix, size_t mark_number)
		: plain(path_prefix + ".bin", std::min(static_cast<size_t>(DBMS_DEFAULT_BUFFER_SIZE), Poco::File(path_prefix + ".bin").getSize())),
		compressed(plain)
		{
			if (mark_number)
			{
				/// Прочитаем из файла с засечками смещение в файле с данными.
				ReadBufferFromFile marks(path_prefix + ".mrk", MERGE_TREE_MARK_SIZE);
				marks.seek(mark_number * MERGE_TREE_MARK_SIZE);
				
				size_t offset_in_compressed_file = 0;
				size_t offset_in_decompressed_block = 0;
				
				readIntBinary(offset_in_compressed_file, marks);
				readIntBinary(offset_in_decompressed_block, marks);
				
				plain.seek(offset_in_compressed_file);
				compressed.next();
				compressed.position() += offset_in_decompressed_block;
			}
		}
		
		ReadBufferFromFile plain;
		CompressedReadBuffer compressed;
	};
	
	typedef std::map<std::string, SharedPtr<Stream> > FileStreams;
	FileStreams streams;
	
	
	void addStream(const String & name, const IDataType & type, size_t mark_number, size_t level = 0)
	{
		String escaped_column_name = escapeForFileName(name);
		
		/** Если файла с данными нет - то не будем пытаться открыть его.
			* Это нужно, чтобы можно было добавлять новые столбцы к структуре таблицы без создания файлов для старых кусков.
			*/
		if (!Poco::File(path + escaped_column_name + ".bin").exists())
			return;
		
		/// Для массивов используются отдельные потоки для размеров.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
			String escaped_size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level);
			
			streams.insert(std::make_pair(size_name, new Stream(
				path + escaped_size_name,
				mark_number)));
			
			addStream(name, *type_arr->getNestedType(), mark_number, level + 1);
		}
		else
			streams.insert(std::make_pair(name, new Stream(
				path + escaped_column_name,
				mark_number)));
	}
	
	void readData(const String & name, const IDataType & type, IColumn & column, size_t max_rows_to_read, size_t level = 0)
	{
		/// Для массивов требуется сначала десериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			type_arr->deserializeOffsets(
				column,
				streams[name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + Poco::NumberFormatter::format(level)]->compressed,
											max_rows_to_read);
			
			if (column.size())
				readData(
					name,
			*type_arr->getNestedType(),
							dynamic_cast<ColumnArray &>(column).getData(),
							dynamic_cast<const ColumnArray &>(column).getOffsets()[column.size() - 1],
							level + 1);
		}
		else
			type.deserializeBinary(column, streams[name]->compressed, max_rows_to_read);
	}
};

}
