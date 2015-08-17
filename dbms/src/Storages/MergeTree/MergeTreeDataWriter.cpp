#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergedBlockOutputStream.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/DataTypes/DataTypeNested.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/IO/HashingWriteBuffer.h>

namespace DB
{

BlocksWithDateIntervals MergeTreeDataWriter::splitBlockIntoParts(const Block & block)
{
	data.check(block, true);

	const auto & date_lut = DateLUT::instance();

	size_t rows = block.rows();
	size_t columns = block.columns();

	/// Достаём столбец с датой.
	const ColumnUInt16::Container_t & dates =
		typeid_cast<const ColumnUInt16 &>(*block.getByName(data.date_column_name).column).getData();

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

MergeTreeData::MutableDataPartPtr MergeTreeDataWriter::writeTempPart(BlockWithDateInterval & block_with_dates, Int64 temp_index)
{
	Block & block = block_with_dates.block;
	UInt16 min_date = block_with_dates.min_date;
	UInt16 max_date = block_with_dates.max_date;

	const auto & date_lut = DateLUT::instance();

	DayNum_t min_month = date_lut.toFirstDayNumOfMonth(DayNum_t(min_date));
	DayNum_t max_month = date_lut.toFirstDayNumOfMonth(DayNum_t(max_date));

	if (min_month != max_month)
		throw Exception("Logical error: part spans more than one month.");

	size_t part_size = (block.rows() + data.index_granularity - 1) / data.index_granularity;

	String tmp_part_name = "tmp_" + ActiveDataPartSet::getPartName(
		DayNum_t(min_date), DayNum_t(max_date),
		temp_index, temp_index, 0);

	String part_tmp_path = data.getFullPath() + tmp_part_name + "/";

	Poco::File(part_tmp_path).createDirectories();

	MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data);
	new_data_part->name = tmp_part_name;
	new_data_part->is_temp = true;

	/// Если для сортировки надо вычислить некоторые столбцы - делаем это.
	if (data.mode != MergeTreeData::Unsorted)
		data.getPrimaryExpression()->execute(block);

	SortDescription sort_descr = data.getSortDescription();

	/// Сортируем.
	IColumn::Permutation * perm_ptr = nullptr;
	IColumn::Permutation perm;
	if (data.mode != MergeTreeData::Unsorted)
	{
		stableGetPermutation(block, sort_descr, perm);
		perm_ptr = &perm;
	}

	NamesAndTypesList columns = data.getColumnsList().filter(block.getColumnsList().getNames());
	MergedBlockOutputStream out(data, part_tmp_path, columns, CompressionMethod::LZ4);

	out.getIndex().reserve(part_size * sort_descr.size());

	out.writePrefix();
	out.writeWithPermutation(block, perm_ptr);
	MergeTreeData::DataPart::Checksums checksums = out.writeSuffixAndGetChecksums();

	new_data_part->left_date = DayNum_t(min_date);
	new_data_part->right_date = DayNum_t(max_date);
	new_data_part->left = temp_index;
	new_data_part->right = temp_index;
	new_data_part->level = 0;
	new_data_part->size = part_size;
	new_data_part->modification_time = time(0);
	new_data_part->month = min_month;
	new_data_part->columns = columns;
	new_data_part->checksums = checksums;
	new_data_part->index.swap(out.getIndex());
	new_data_part->size_in_bytes = MergeTreeData::DataPart::calcTotalSize(part_tmp_path);

	return new_data_part;
}

}
