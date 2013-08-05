#pragma once

#include <DB/Storages/StorageMergeTree.h>

namespace DB
{
	
class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StoragePtr owned_storage) : IBlockOutputStream(owned_storage), storage(dynamic_cast<StorageMergeTree &>(*owned_storage)), flags(O_TRUNC | O_CREAT | O_WRONLY)
	{
	}
	
	void write(const Block & block)
	{
		storage.check(block, true);
		
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		
		size_t rows = block.rows();
		size_t columns = block.columns();
		
		/// Достаём столбец с датой.
		const ColumnUInt16::Container_t & dates =
			dynamic_cast<const ColumnUInt16 &>(*block.getByName(storage.date_column_name).column).getData();
		
		/// Минимальная и максимальная дата.
		UInt16 min_date = std::numeric_limits<UInt16>::max();
		UInt16 max_date = std::numeric_limits<UInt16>::min();
		for (ColumnUInt16::Container_t::const_iterator it = dates.begin(); it != dates.end(); ++it)
		{
			if (*it < min_date)
				min_date = *it;
			if (*it > max_date)
				max_date = *it;
		}
		
		/// Разделяем на блоки по месяцам. Для каждого ещё посчитаем минимальную и максимальную дату.
		typedef std::map<UInt16, BlockWithDateInterval> BlocksByMonth;
		BlocksByMonth blocks_by_month;
		
		UInt16 min_month = date_lut.toFirstDayNumOfMonth(Yandex::DayNum_t(min_date));
		UInt16 max_month = date_lut.toFirstDayNumOfMonth(Yandex::DayNum_t(max_date));
		
		/// Типичный случай - когда месяц один (ничего разделять не нужно).
		if (min_month == max_month)
			blocks_by_month[min_month] = BlockWithDateInterval(block, min_date, max_date);
		else
		{
			for (size_t i = 0; i < rows; ++i)
			{
				UInt16 month = date_lut.toFirstDayNumOfMonth(Yandex::DayNum_t(dates[i]));
				
				BlockWithDateInterval & block_for_month = blocks_by_month[month];
				if (!block_for_month.block)
					block_for_month.block = block.cloneEmpty();
				
				if (dates[i] < block_for_month.min_date)
					block_for_month.min_date = dates[i];
				if (dates[i] > block_for_month.max_date)
					block_for_month.max_date = dates[i];
				
				for (size_t j = 0; j < columns; ++j)
					block_for_month.block.getByPosition(j).column->insert((*block.getByPosition(j).column)[i]);
			}
		}
		
		/// Для каждого месяца.
		for (BlocksByMonth::iterator it = blocks_by_month.begin(); it != blocks_by_month.end(); ++it)
			writePart(it->second.block, it->second.min_date, it->second.max_date);
	}
	
private:
	StorageMergeTree & storage;
	
	const int flags;
	
	struct BlockWithDateInterval
	{
		Block block;
		UInt16 min_date;
		UInt16 max_date;
		
		BlockWithDateInterval() : min_date(std::numeric_limits<UInt16>::max()), max_date(0) {}
		BlockWithDateInterval(const Block & block_, UInt16 min_date_, UInt16 max_date_)
		: block(block_), min_date(min_date_), max_date(max_date_) {}
	};
	
	typedef std::set<std::string> OffsetColumns;
	
	void writePart(Block & block, UInt16 min_date, UInt16 max_date)
	{
		Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
		
		size_t rows = block.rows();
		size_t columns = block.columns();
		UInt64 part_id = storage.increment.get(true);
		
		String part_name = storage.getPartName(
			Yandex::DayNum_t(min_date), Yandex::DayNum_t(max_date),
												part_id, part_id, 0);
		
		String part_tmp_path = storage.full_path + "tmp_" + part_name + "/";
		String part_res_path = storage.full_path + part_name + "/";
		
		Poco::File(part_tmp_path).createDirectories();
		
		LOG_TRACE(storage.log, "Calculating primary expression.");
		
		/// Если для сортировки надо вычислить некоторые столбцы - делаем это.
		storage.primary_expr->execute(block);
		
		LOG_TRACE(storage.log, "Sorting by primary key.");
		
		/// Сортируем.
		sortBlock(block, storage.sort_descr);
		
		/// Наконец-то можно писать данные на диск.
		LOG_TRACE(storage.log, "Writing index.");
		
		/// Сначала пишем индекс. Индекс содержит значение PK для каждой index_granularity строки.
		{
			WriteBufferFromFile index(part_tmp_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, flags);
			
			typedef std::vector<const ColumnWithNameAndType *> PrimaryColumns;
			PrimaryColumns primary_columns;
			
			for (size_t i = 0, size = storage.sort_descr.size(); i < size; ++i)
				primary_columns.push_back(
					!storage.sort_descr[i].column_name.empty()
					? &block.getByName(storage.sort_descr[i].column_name)
					: &block.getByPosition(storage.sort_descr[i].column_number));
				
			for (size_t i = 0; i < rows; i += storage.index_granularity)
				for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
					(*it)->type->serializeBinary((*(*it)->column)[i], index);
		}
		
		LOG_TRACE(storage.log, "Writing data.");
		
		/// Множество записанных столбцов со смещениями, чтобы не писать общие для вложенных структур столбцы несколько раз
		OffsetColumns offset_columns;
		
		for (size_t i = 0; i < columns; ++i)
		{
			const ColumnWithNameAndType & column = block.getByPosition(i);
			writeData(part_tmp_path, column.name, *column.type, *column.column, offset_columns);
		}
		
		LOG_TRACE(storage.log, "Renaming.");
		
		/// Переименовываем кусок.
		Poco::File(part_tmp_path).renameTo(part_res_path);
		
		/// Добавляем новый кусок в набор.
		{
			Poco::ScopedLock<Poco::FastMutex> lock(storage.data_parts_mutex);
			Poco::ScopedLock<Poco::FastMutex> lock_all(storage.all_data_parts_mutex);
			
			StorageMergeTree::DataPartPtr new_data_part = new StorageMergeTree::DataPart(storage);
			new_data_part->left_date = Yandex::DayNum_t(min_date);
			new_data_part->right_date = Yandex::DayNum_t(max_date);
			new_data_part->left = part_id;
			new_data_part->right = part_id;
			new_data_part->level = 0;
			new_data_part->name = part_name;
			new_data_part->size = (rows + storage.index_granularity - 1) / storage.index_granularity;
			new_data_part->modification_time = time(0);
			new_data_part->left_month = date_lut.toFirstDayNumOfMonth(new_data_part->left_date);
			new_data_part->right_month = date_lut.toFirstDayNumOfMonth(new_data_part->right_date);
			
			storage.data_parts.insert(new_data_part);
			storage.all_data_parts.insert(new_data_part);
		}
		
		/// Если на каждую запись делать по две итерации слияния, то дерево будет максимально компактно.
		storage.merge(2);
	}
	
	/// Записать данные одного столбца.
	void writeData(const String & path, const String & name, const IDataType & type, const IColumn & column,
					OffsetColumns & offset_columns, size_t level = 0)
	{
		String escaped_column_name = escapeForFileName(name);
		size_t size = column.size();
		
		/// Для массивов требуется сначала сериализовать размеры, а потом значения.
		if (const DataTypeArray * type_arr = dynamic_cast<const DataTypeArray *>(&type))
		{
			String size_name = escapeForFileName(DataTypeNested::extractNestedTableName(name))
				+ ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			if (offset_columns.count(size_name) == 0)
			{
				offset_columns.insert(size_name);
				
				WriteBufferFromFile plain(path + size_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
				WriteBufferFromFile marks(path + size_name + ".mrk", 4096, flags);
				CompressedWriteBuffer compressed(plain);
				
				size_t prev_mark = 0;
				while (prev_mark < size)
				{
					/// Каждая засечка - это: (смещение в файле до начала сжатого блока, смещение внутри блока)
					writeIntBinary(plain.count(), marks);
					writeIntBinary(compressed.offset(), marks);
					
					type_arr->serializeOffsets(column, compressed, prev_mark, storage.index_granularity);
					prev_mark += storage.index_granularity;
				}
			}
		}
		if (const DataTypeNested * type_nested = dynamic_cast<const DataTypeNested *>(&type))
		{
			String size_name = escaped_column_name + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
			
			WriteBufferFromFile plain(path + size_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(path + size_name + ".mrk", 4096, flags);
			CompressedWriteBuffer compressed(plain);
			
			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				/// Каждая засечка - это: (смещение в файле до начала сжатого блока, смещение внутри блока)
				writeIntBinary(plain.count(), marks);
				writeIntBinary(compressed.offset(), marks);
				
				type_nested->serializeOffsets(column, compressed, prev_mark, storage.index_granularity);
				prev_mark += storage.index_granularity;
			}
		}
		
		{
			WriteBufferFromFile plain(path + escaped_column_name + ".bin", DBMS_DEFAULT_BUFFER_SIZE, flags);
			WriteBufferFromFile marks(path + escaped_column_name + ".mrk", 4096, flags);
			CompressedWriteBuffer compressed(plain);
			
			size_t prev_mark = 0;
			while (prev_mark < size)
			{
				writeIntBinary(plain.count(), marks);
				writeIntBinary(compressed.offset(), marks);
				
				type.serializeBinary(column, compressed, prev_mark, storage.index_granularity);
				prev_mark += storage.index_granularity;
			}
		}
	}
};
	
}
