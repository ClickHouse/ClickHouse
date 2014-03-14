#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>

namespace DB
{

BlocksWithDateIntervals MergeTreeDataWriter::splitBlockIntoParts(const Block & block, const MergeTreeData::LockedTableStructurePtr & structure)
{
	structure->check(block, true);

	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	size_t rows = block.rows();
	size_t columns = block.columns();

	/// Достаём столбец с датой.
	const ColumnUInt16::Container_t & dates =
		dynamic_cast<const ColumnUInt16 &>(*block.getByName(data.date_column_name).column).getData();

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

	BlocksWithDateIntervals res;

	UInt16 min_month = date_lut.toFirstDayNumOfMonth(DayNum_t(min_date));
	UInt16 max_month = date_lut.toFirstDayNumOfMonth(DayNum_t(max_date));

	/// Типичный случай - когда месяц один (ничего разделять не нужно).
	if (min_month == max_month)
	{
		res.push_back(BlockWithDateInterval(block, min_date, max_date));
		return res;
	}

	/// Разделяем на блоки по месяцам. Для каждого ещё посчитаем минимальную и максимальную дату.
	typedef std::map<UInt16, BlockWithDateInterval *> BlocksByMonth;
	BlocksByMonth blocks_by_month;

	for (size_t i = 0; i < rows; ++i)
	{
		UInt16 month = date_lut.toFirstDayNumOfMonth(DayNum_t(dates[i]));

		BlockWithDateInterval *& block_for_month = blocks_by_month[month];
		if (!block_for_month)
		{
			block_for_month = &*res.insert(res.end(), BlockWithDateInterval());
			block_for_month->block = block.cloneEmpty();
		}

		if (dates[i] < block_for_month->min_date)
			block_for_month->min_date = dates[i];
		if (dates[i] > block_for_month->max_date)
			block_for_month->max_date = dates[i];

		for (size_t j = 0; j < columns; ++j)
			block_for_month->block.getByPosition(j).column->insert((*block.getByPosition(j).column)[i]);
	}

	return res;
}

MergeTreeData::DataPartPtr MergeTreeDataWriter::writeTempPart(BlockWithDateInterval & block_with_dates, UInt64 temp_index,
	const MergeTreeData::LockedTableStructurePtr & structure)
{
	Block & block = block_with_dates.block;
	UInt16 min_date = block_with_dates.min_date;
	UInt16 max_date = block_with_dates.max_date;

	DateLUTSingleton & date_lut = DateLUTSingleton::instance();

	size_t rows = block.rows();
	size_t columns = block.columns();
	size_t part_size = (rows + data.index_granularity - 1) / data.index_granularity;

	String tmp_part_name = "tmp_" + data.getPartName(
		DayNum_t(min_date), DayNum_t(max_date),
		temp_index, temp_index, 0);

	String part_tmp_path = structure->getFullPath() + tmp_part_name + "/";

	Poco::File(part_tmp_path).createDirectories();

	LOG_TRACE(log, "Calculating primary expression.");

	/// Если для сортировки надо вычислить некоторые столбцы - делаем это.
	data.getPrimaryExpression()->execute(block);

	LOG_TRACE(log, "Sorting by primary key.");

	SortDescription sort_descr = data.getSortDescription();

	/// Сортируем.
	stableSortBlock(block, sort_descr);

	/// Наконец-то можно писать данные на диск.
	LOG_TRACE(log, "Writing index.");

	/// Сначала пишем индекс. Индекс содержит значение PK для каждой index_granularity строки.
	MergeTreeData::DataPart::Index index_vec;
	index_vec.reserve(part_size * sort_descr.size());

	{
		WriteBufferFromFile index(part_tmp_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, flags);

		typedef std::vector<const ColumnWithNameAndType *> PrimaryColumns;
		PrimaryColumns primary_columns;

		for (size_t i = 0, size = sort_descr.size(); i < size; ++i)
			primary_columns.push_back(
				!sort_descr[i].column_name.empty()
				? &block.getByName(sort_descr[i].column_name)
				: &block.getByPosition(sort_descr[i].column_number));

		for (size_t i = 0; i < rows; i += data.index_granularity)
		{
			for (PrimaryColumns::const_iterator it = primary_columns.begin(); it != primary_columns.end(); ++it)
			{
				index_vec.push_back((*(*it)->column)[i]);
				(*it)->type->serializeBinary(index_vec.back(), index);
			}
		}

		index.next();
	}

	LOG_TRACE(log, "Writing data.");

	/// Множество записанных столбцов со смещениями, чтобы не писать общие для вложенных структур столбцы несколько раз
	OffsetColumns offset_columns;

	for (size_t i = 0; i < columns; ++i)
	{
		const ColumnWithNameAndType & column = block.getByPosition(i);
		writeData(part_tmp_path, column.name, *column.type, *column.column, offset_columns);
	}

	MergeTreeData::DataPartPtr new_data_part = new MergeTreeData::DataPart(data);
	new_data_part->left_date = DayNum_t(min_date);
	new_data_part->right_date = DayNum_t(max_date);
	new_data_part->left = temp_index;
	new_data_part->right = temp_index;
	new_data_part->level = 0;
	new_data_part->name = tmp_part_name;
	new_data_part->size = part_size;
	new_data_part->modification_time = time(0);
	new_data_part->left_month = date_lut.toFirstDayNumOfMonth(new_data_part->left_date);
	new_data_part->right_month = date_lut.toFirstDayNumOfMonth(new_data_part->right_date);
	new_data_part->index.swap(index_vec);

	return new_data_part;
}

void MergeTreeDataWriter::writeData(const String & path, const String & name, const IDataType & type, const IColumn & column,
				OffsetColumns & offset_columns, size_t level)
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

				type_arr->serializeOffsets(column, compressed, prev_mark, data.index_granularity);
				prev_mark += data.index_granularity;

				compressed.nextIfAtEnd();	/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
			}

			compressed.next();
			plain.next();
			marks.next();
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

			type_nested->serializeOffsets(column, compressed, prev_mark, data.index_granularity);
			prev_mark += data.index_granularity;

			compressed.nextIfAtEnd();	/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
		}

		compressed.next();
		plain.next();
		marks.next();
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

			type.serializeBinary(column, compressed, prev_mark, data.index_granularity);
			prev_mark += data.index_granularity;

			compressed.nextIfAtEnd();	/// Чтобы вместо засечек, указывающих на конец сжатого блока, были засечки, указывающие на начало следующего.
		}

		compressed.next();
		plain.next();
		marks.next();
	}
}

}
