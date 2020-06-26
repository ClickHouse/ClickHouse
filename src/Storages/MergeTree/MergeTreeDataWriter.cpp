#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Columns/ColumnConst.h>
#include <Common/HashTable/HashMap.h>
#include <Common/Exception.h>
#include <Disks/createVolume.h>
#include <Interpreters/AggregationCommon.h>
#include <Interpreters/Context.h>
#include <IO/HashingWriteBuffer.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <IO/WriteHelpers.h>
#include <Poco/File.h>
#include <Common/typeid_cast.h>


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

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int TOO_MANY_PARTS;
}

namespace
{

void buildScatterSelector(
        const ColumnRawPtrs & columns,
        PODArray<size_t> & partition_num_to_first_row,
        IColumn::Selector & selector,
        size_t max_parts)
{
    /// Use generic hashed variant since partitioning is unlikely to be a bottleneck.
    using Data = HashMap<UInt128, size_t, UInt128TrivialHash>;
    Data partitions_map;

    size_t num_rows = columns[0]->size();
    size_t partitions_count = 0;
    for (size_t i = 0; i < num_rows; ++i)
    {
        Data::key_type key = hash128(i, columns.size(), columns);
        typename Data::LookupResult it;
        bool inserted;
        partitions_map.emplace(key, it, inserted);

        if (inserted)
        {
            if (max_parts && partitions_count >= max_parts)
                throw Exception("Too many partitions for single INSERT block (more than " + toString(max_parts) + "). The limit is controlled by 'max_partitions_per_insert_block' setting. Large number of partitions is a common misconception. It will lead to severe negative performance impact, including slow server startup, slow INSERT queries and slow SELECT queries. Recommended total number of partitions for a table is under 1000..10000. Please note, that partitioning is not intended to speed up SELECT queries (ORDER BY key is sufficient to make range queries fast). Partitions are intended for data manipulation (DROP PARTITION, etc).", ErrorCodes::TOO_MANY_PARTS);

            partition_num_to_first_row.push_back(i);
            it->getMapped() = partitions_count;

            ++partitions_count;

            /// Optimization for common case when there is only one partition - defer selector initialization.
            if (partitions_count == 2)
            {
                selector = IColumn::Selector(num_rows);
                std::fill(selector.begin(), selector.begin() + i, 0);
            }
        }

        if (partitions_count > 1)
            selector[i] = it->getMapped();
    }
}

/// Computes ttls and updates ttl infos
void updateTTL(
    const TTLDescription & ttl_entry,
    IMergeTreeDataPart::TTLInfos & ttl_infos,
    DB::MergeTreeDataPartTTLInfo & ttl_info,
    Block & block,
    bool update_part_min_max_ttls)
{
    bool remove_column = false;
    if (!block.has(ttl_entry.result_column))
    {
        ttl_entry.expression->execute(block);
        remove_column = true;
    }

    const auto & current = block.getByName(ttl_entry.result_column);

    const IColumn * column = current.column.get();
    if (const ColumnUInt16 * column_date = typeid_cast<const ColumnUInt16 *>(column))
    {
        const auto & date_lut = DateLUT::instance();
        for (const auto & val : column_date->getData())
            ttl_info.update(date_lut.fromDayNum(DayNum(val)));
    }
    else if (const ColumnUInt32 * column_date_time = typeid_cast<const ColumnUInt32 *>(column))
    {
        for (const auto & val : column_date_time->getData())
            ttl_info.update(val);
    }
    else if (const ColumnConst * column_const = typeid_cast<const ColumnConst *>(column))
    {
        if (typeid_cast<const ColumnUInt16 *>(&column_const->getDataColumn()))
        {
            const auto & date_lut = DateLUT::instance();
            ttl_info.update(date_lut.fromDayNum(DayNum(column_const->getValue<UInt16>())));
        }
        else if (typeid_cast<const ColumnUInt32 *>(&column_const->getDataColumn()))
        {
            ttl_info.update(column_const->getValue<UInt32>());
        }
        else
            throw Exception("Unexpected type of result TTL column", ErrorCodes::LOGICAL_ERROR);
    }
    else
        throw Exception("Unexpected type of result TTL column", ErrorCodes::LOGICAL_ERROR);

    if (update_part_min_max_ttls)
        ttl_infos.updatePartMinMaxTTL(ttl_info.min, ttl_info.max);

    if (remove_column)
        block.erase(ttl_entry.result_column);
}

}

BlocksWithPartition MergeTreeDataWriter::splitBlockIntoParts(const Block & block, size_t max_parts, const StorageMetadataPtr & metadata_snapshot)
{
    BlocksWithPartition result;
    if (!block || !block.rows())
        return result;

    metadata_snapshot->check(block, true);

    if (!metadata_snapshot->hasPartitionKey()) /// Table is not partitioned.
    {
        result.emplace_back(Block(block), Row());
        return result;
    }

    Block block_copy = block;
    const auto & partition_key = metadata_snapshot->getPartitionKey();
    partition_key.expression->execute(block_copy);

    ColumnRawPtrs partition_columns;
    partition_columns.reserve(partition_key.sample_block.columns());
    for (const ColumnWithTypeAndName & element : partition_key.sample_block)
        partition_columns.emplace_back(block_copy.getByName(element.name).column.get());

    PODArray<size_t> partition_num_to_first_row;
    IColumn::Selector selector;
    buildScatterSelector(partition_columns, partition_num_to_first_row, selector, max_parts);

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
        /// NOTE: returning a copy of the original block so that calculated partition key columns
        /// do not interfere with possible calculated primary key columns of the same name.
        result.emplace_back(Block(block), get_partition(0));
        return result;
    }

    for (size_t i = 0; i < partitions_count; ++i)
        result.emplace_back(block.cloneEmpty(), get_partition(i));

    for (size_t col = 0; col < block.columns(); ++col)
    {
        MutableColumns scattered = block.getByPosition(col).column->scatter(partitions_count, selector);
        for (size_t i = 0; i < partitions_count; ++i)
            result[i].block.getByPosition(col).column = std::move(scattered[i]);
    }

    return result;
}

MergeTreeData::MutableDataPartPtr MergeTreeDataWriter::writeTempPart(BlockWithPartition & block_with_partition, const StorageMetadataPtr & metadata_snapshot)
{
    Block & block = block_with_partition.block;

    static const String TMP_PREFIX = "tmp_insert_";

    /// This will generate unique name in scope of current server process.
    Int64 temp_index = data.insert_increment.get();

    IMergeTreeDataPart::MinMaxIndex minmax_idx;
    minmax_idx.update(block, data.minmax_idx_columns);

    MergeTreePartition partition(std::move(block_with_partition.partition));

    MergeTreePartInfo new_part_info(partition.getID(metadata_snapshot->getPartitionKey().sample_block), temp_index, temp_index, 0);
    String part_name;
    if (data.format_version < MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        DayNum min_date(minmax_idx.hyperrectangle[data.minmax_idx_date_column_pos].left.get<UInt64>());
        DayNum max_date(minmax_idx.hyperrectangle[data.minmax_idx_date_column_pos].right.get<UInt64>());

        const auto & date_lut = DateLUT::instance();

        auto min_month = date_lut.toNumYYYYMM(min_date);
        auto max_month = date_lut.toNumYYYYMM(max_date);

        if (min_month != max_month)
            throw Exception("Logical error: part spans more than one month.", ErrorCodes::LOGICAL_ERROR);

        part_name = new_part_info.getPartNameV0(min_date, max_date);
    }
    else
        part_name = new_part_info.getPartName();

    /// Size of part would not be greater than block.bytes() + epsilon
    size_t expected_size = block.bytes();

    DB::IMergeTreeDataPart::TTLInfos move_ttl_infos;
    const auto & move_ttl_entries = metadata_snapshot->getMoveTTLs();
    for (const auto & ttl_entry : move_ttl_entries)
        updateTTL(ttl_entry, move_ttl_infos, move_ttl_infos.moves_ttl[ttl_entry.result_column], block, false);

    NamesAndTypesList columns = metadata_snapshot->getColumns().getAllPhysical().filter(block.getNames());
    ReservationPtr reservation = data.reserveSpacePreferringTTLRules(expected_size, move_ttl_infos, time(nullptr));
    VolumePtr volume = data.getStoragePolicy()->getVolume(0);

    auto new_data_part = data.createPart(
        part_name,
        data.choosePartType(expected_size, block.rows()),
        new_part_info,
        createVolumeFromReservation(reservation, volume),
        TMP_PREFIX + part_name);

    new_data_part->setColumns(columns);
    new_data_part->partition = std::move(partition);
    new_data_part->minmax_idx = std::move(minmax_idx);
    new_data_part->is_temp = true;

    if (new_data_part->isStoredOnDisk())
    {
        /// The name could be non-unique in case of stale files from previous runs.
        String full_path = new_data_part->getFullRelativePath();

        if (new_data_part->volume->getDisk()->exists(full_path))
        {
            LOG_WARNING(log, "Removing old temporary directory {}", fullPath(new_data_part->volume->getDisk(), full_path));
            new_data_part->volume->getDisk()->removeRecursive(full_path);
        }

        new_data_part->volume->getDisk()->createDirectories(full_path);
    }

    /// If we need to calculate some columns to sort.
    if (metadata_snapshot->hasSortingKey() || metadata_snapshot->hasSecondaryIndices())
        data.getSortingKeyAndSkipIndicesExpression(metadata_snapshot)->execute(block);

    Names sort_columns = metadata_snapshot->getSortingKeyColumns();
    SortDescription sort_description;
    size_t sort_columns_size = sort_columns.size();
    sort_description.reserve(sort_columns_size);

    for (size_t i = 0; i < sort_columns_size; ++i)
        sort_description.emplace_back(block.getPositionByName(sort_columns[i]), 1, 1);

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocks);

    /// Sort
    IColumn::Permutation * perm_ptr = nullptr;
    IColumn::Permutation perm;
    if (!sort_description.empty())
    {
        if (!isAlreadySorted(block, sort_description))
        {
            stableGetPermutation(block, sort_description, perm);
            perm_ptr = &perm;
        }
        else
            ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterBlocksAlreadySorted);
    }

    if (metadata_snapshot->hasRowsTTL())
        updateTTL(metadata_snapshot->getRowsTTL(), new_data_part->ttl_infos, new_data_part->ttl_infos.table_ttl, block, true);

    for (const auto & [name, ttl_entry] : metadata_snapshot->getColumnTTLs())
        updateTTL(ttl_entry, new_data_part->ttl_infos, new_data_part->ttl_infos.columns_ttl[name], block, true);

    new_data_part->ttl_infos.update(move_ttl_infos);

    /// This effectively chooses minimal compression method:
    ///  either default lz4 or compression method with zero thresholds on absolute and relative part size.
    auto compression_codec = data.global_context.chooseCompressionCodec(0, 0);

    const auto & index_factory = MergeTreeIndexFactory::instance();
    MergedBlockOutputStream out(new_data_part, metadata_snapshot, columns, index_factory.getMany(metadata_snapshot->getSecondaryIndices()), compression_codec);

    out.writePrefix();
    out.writeWithPermutation(block, perm_ptr);
    out.writeSuffixAndFinalizePart(new_data_part);

    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterRows, block.rows());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterUncompressedBytes, block.bytes());
    ProfileEvents::increment(ProfileEvents::MergeTreeDataWriterCompressedBytes, new_data_part->getBytesOnDisk());

    return new_data_part;
}

}
