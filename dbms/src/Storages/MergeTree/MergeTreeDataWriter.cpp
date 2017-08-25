#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Common/escapeForFileName.h>
#include <Common/HashTable/HashMap.h>
#include <Interpreters/AggregationCommon.h>
#include <IO/HashingWriteBuffer.h>
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

namespace
{

void buildScatterSelector(
        const ConstColumnPlainPtrs & columns,
        PODArray<size_t> & partition_num_to_first_row,
        IColumn::Selector & selector)
{
    /// Use generic hashed variant since partitioning is unlikely to be a bottleneck.
    using Data = HashMap<UInt128, size_t, UInt128TrivialHash>;
    Data partitions_map;

    size_t num_rows = columns[0]->size();
    size_t partitions_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        Data::key_type key = hash128(i, columns.size(), columns);
        typename Data::iterator it;
        bool inserted;
        partitions_map.emplace(key, it, inserted);

        if (inserted)
        {
            partition_num_to_first_row.push_back(i);
            it->second = partitions_count;

            ++partitions_count;

            /// Optimization for common case when there is only one partition - defer selector initialization.
            if (partitions_count == 2)
            {
                selector = IColumn::Selector(num_rows);
                std::fill(selector.begin(), selector.begin() + i, 0);
            }
        }

        if (partitions_count > 1)
            selector[i] = it->second;
    }
}

}

BlocksWithPartition MergeTreeDataWriter::splitBlockIntoParts(const Block & block)
{
    BlocksWithPartition result;
    if (!block || !block.rows())
        return result;

    data.check(block, true);
    block.checkNumberOfRows();

    if (data.partition_expr_columns.empty()) /// Table is not partitioned.
    {
        result.emplace_back(Block(block), Row());
        return result;
    }

    Block block_copy = block;
    data.partition_expr->execute(block_copy);

    ConstColumnPlainPtrs partition_columns;
    partition_columns.reserve(data.partition_expr_columns.size());
    for (const String & name : data.partition_expr_columns)
        partition_columns.emplace_back(block_copy.getByName(name).column.get());

    PODArray<size_t> partition_num_to_first_row;
    IColumn::Selector selector;
    buildScatterSelector(partition_columns, partition_num_to_first_row, selector);

    size_t partitions_count = partition_num_to_first_row.size();
    result.reserve(partitions_count);

    auto get_partition = [&](size_t num)
    {
        Row partition(partition_columns.size());
        for (size_t i = 0; i < partition_columns.size(); ++i)
            partition[i] = Field((*partition_columns[i])[partition_num_to_first_row[num]]);
        return partition;
    };

    if (partitions_count == 1)
    {
        /// A typical case is when there is one partition (you do not need to split anything).
        result.emplace_back(std::move(block_copy), get_partition(0));
        return result;
    }

    for (size_t i = 0; i < partitions_count; ++i)
        result.emplace_back(block.cloneEmpty(), get_partition(i));

    for (size_t col = 0; col < block.columns(); ++col)
    {
        Columns scattered = block.getByPosition(col).column->scatter(partitions_count, selector);
        for (size_t i = 0; i < partitions_count; ++i)
            result[i].block.getByPosition(col).column = std::move(scattered[i]);
    }

    return result;
}

MergeTreeData::MutableDataPartPtr MergeTreeDataWriter::writeTempPart(BlockWithPartition & block_with_partition)
{
    Block & block = block_with_partition.block;

    size_t part_size = (block.rows() + data.index_granularity - 1) / data.index_granularity;

    static const String TMP_PREFIX = "tmp_insert_";

    /// This will generate unique name in scope of current server process.
    Int64 temp_index = data.insert_increment.get();

    MinMaxIndex minmax_idx;
    minmax_idx.update(block, data.minmax_idx_columns);

    String new_partition_id = data.getPartitionIDFromData(block_with_partition.partition);

    MergeTreePartInfo new_part_info(new_partition_id, temp_index, temp_index, 0);
    String part_name;
    if (data.format_version == 0)
    {
        DayNum_t min_date(minmax_idx.min_column_values[data.minmax_idx_date_column_pos].get<UInt64>());
        DayNum_t max_date(minmax_idx.max_column_values[data.minmax_idx_date_column_pos].get<UInt64>());

        const auto & date_lut = DateLUT::instance();

        DayNum_t min_month = date_lut.toFirstDayNumOfMonth(DayNum_t(min_date));
        DayNum_t max_month = date_lut.toFirstDayNumOfMonth(DayNum_t(max_date));

        if (min_month != max_month)
            throw Exception("Logical error: part spans more than one month.");

        part_name = new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        part_name = new_part_info.getPartName();

    MergeTreeData::MutableDataPartPtr new_data_part = std::make_shared<MergeTreeData::DataPart>(data, part_name, new_part_info);
    new_data_part->partition = std::move(block_with_partition.partition);
    new_data_part->minmax_idx = std::move(minmax_idx);
    new_data_part->relative_path = TMP_PREFIX + part_name;
    new_data_part->is_temp = true;

    /// The name could be non-unique in case of stale files from previous runs.
    String full_path = new_data_part->getFullPath();
    Poco::File dir(full_path);

    if (dir.exists())
    {
        LOG_WARNING(log, "Removing old temporary directory " + full_path);
        dir.remove(true);
    }

    dir.createDirectories();

    /// If you need to calculate some columns to sort, we do it.
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

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_method = data.context.chooseCompressionMethod(0, 0);

    NamesAndTypesList columns = data.getColumnsList().filter(block.getColumnsList().getNames());
    MergedBlockOutputStream out(data, new_data_part->getFullPath(), columns, compression_method);

    out.writePrefix();
    out.writeWithPermutation(block, perm_ptr);
    MergeTreeData::DataPart::Checksums checksums = out.writeSuffixAndGetChecksums();

    new_data_part->size = part_size;
    new_data_part->modification_time = time(nullptr);
    new_data_part->columns = columns;
    new_data_part->checksums = checksums;
    new_data_part->index.swap(out.getIndex());
    new_data_part->size_in_bytes = MergeTreeData::DataPart::calcTotalSize(new_data_part->getFullPath());

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterCompressedBytes, new_data_part->size_in_bytes);

    return new_data_part;
}

}
