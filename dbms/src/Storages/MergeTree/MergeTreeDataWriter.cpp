#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeArray.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/Stopwatch.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <Poco/File.h>


namespace ProfileEvents
{
    extern const Event MergeTreeDataWriterBlocks;
    extern const Event MergeTreeDataWriterBlocksAlreadySorted;
    extern const Event MergeTreeDataWriterRows;
    extern const Event MergeTreeDataWriterUncompressedBytes;
    extern const Event MergeTreeDataWriterCompressedBytes;
}

namespace DB
{

BlocksWithDateIntervals MergeTreeDataWriter::splitBlockIntoParts(const Block & block)
{
    data.check(block, true);

    const auto & date_lut = DateLUT::instance();

    block.checkNumberOfRows();
    size_t rows = block.rows();
    size_t columns = block.columns();

    /// We retrieve column with date.
    const ColumnUInt16::Container_t & dates =
        typeid_cast<const ColumnUInt16 &>(*block.getByName(data.date_column_name).column).getData();

    /// Minimum and maximum date.
    UInt16 min_date = std::numeric_limits<UInt16>::max();
    UInt16 max_date = std::numeric_limits<UInt16>::min();
    for (auto it = dates.begin(); it != dates.end(); ++it)
    {
        if (*it < min_date)
            min_date = *it;
        if (*it > max_date)
            max_date = *it;
    }

    BlocksWithDateIntervals res;

    UInt16 min_month = date_lut.toFirstDayNumOfMonth(DayNum_t(min_date));
    UInt16 max_month = date_lut.toFirstDayNumOfMonth(DayNum_t(max_date));

    /// A typical case is when the month is one (you do not need to split anything).
    if (min_month == max_month)
    {
        res.push_back(BlockWithDateInterval(block, min_date, max_date));
        return res;
    }

    /// Split to blocks for different months. And also will calculate min and max date for each of them.
    using BlocksByMonth = std::map<UInt16, BlockWithDateInterval *>;
    BlocksByMonth blocks_by_month;

    ColumnPlainPtrs src_columns(columns);
    for (size_t i = 0; i < columns; ++i)
        src_columns[i] = block.safeGetByPosition(i).column.get();

    for (size_t i = 0; i < rows; ++i)
    {
        UInt16 month = date_lut.toFirstDayNumOfMonth(DayNum_t(dates[i]));

        BlockWithDateInterval *& block_for_month = blocks_by_month[month];
        if (!block_for_month)
        {
            block_for_month = &*res.insert(res.end(), BlockWithDateInterval());
            block_for_month->block = block.cloneEmpty();
        }

        block_for_month->updateDates(dates[i]);

        for (size_t j = 0; j < columns; ++j)
            block_for_month->block.getByPosition(j).column->insertFrom(*src_columns[j], i);
    }

    return res;
}

MergeTreeData::MutableDataPartPtr MergeTreeDataWriter::writeTempPart(BlockWithDateInterval & block_with_dates, Int64 temp_index)
{
    /// For logging
    Stopwatch stopwatch;

    Block & block = block_with_dates.block;
    UInt16 min_date = block_with_dates.min_date;
    UInt16 max_date = block_with_dates.max_date;

    const auto & date_lut = DateLUT::instance();

    DayNum_t min_month = date_lut.toFirstDayNumOfMonth(DayNum_t(min_date));
    DayNum_t max_month = date_lut.toFirstDayNumOfMonth(DayNum_t(max_date));

    if (min_month != max_month)
        throw Exception("Logical error: part spans more than one month.");

    size_t part_size = (block.rows() + data.index_granularity - 1) / data.index_granularity;


    String part_name = ActiveDataPartSet::getPartName(
        DayNum_t(min_date), DayNum_t(max_date),
        temp_index, temp_index, 0);
    String tmp_part_name = "tmp_" + part_name;

    String part_tmp_path = data.getFullPath() + tmp_part_name + "/";

    Poco::File(part_tmp_path).createDirectories();

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data);
    new_data_part->name = tmp_part_name;
    new_data_part->is_temp = true;

    /// If you need to compute some columns to sort, we do it.
    if (data.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
        data.getPrimaryExpression()->execute(block);

    SortDescription sort_descr = data.getSortDescription();

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocks);

    /// Sort.
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (data.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
    {
        if (!isAlreadySorted(block, sort_descr))
        {
            stableGetPermutation(block, sort_descr, perm);
            perm_ptr = &perm;
        }
        else
            ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocksAlreadySorted);
    }

    NamesAndTypesList columns = data.getColumnsList().filter(block.getColumnsList().getNames());
    MergedBlockOutputStream out(data, part_tmp_path, columns, CompressionMethod::LZ4);

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

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterCompressedBytes, new_data_part->size_in_bytes);

    if (std::shared_ptr<PartLog> part_log = context.getPartLog())
    {
        PartLogElement elem;
        elem.event_time = time(0);

        elem.event_type = PartLogElement::NEW_PART;
        elem.size_in_bytes = new_data_part->size_in_bytes;
        elem.duration_ms = stopwatch.elapsed() / 1000000;

        elem.database_name = new_data_part->storage.getDatabaseName();
        elem.table_name = new_data_part->storage.getTableName();
        elem.part_name = part_name;

        part_log->add(elem);
    }

    return new_data_part;
}

}
