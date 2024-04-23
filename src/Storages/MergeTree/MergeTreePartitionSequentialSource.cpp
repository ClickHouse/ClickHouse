#include <base/defines.h>

#include <Common/logger_useful.h>

#include <Processors/Chunk.h>

#include <Storages/MarkCache.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreePartitionSequentialSource.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

static void checkSinglePartition(const RangesInDataParts & parts_with_ranges)
{
    if (parts_with_ranges.empty())
        return;

    String partition_id = parts_with_ranges[0].data_part->info.partition_id;

    for (size_t i = 1; i < parts_with_ranges.size(); ++i)
        chassert(partition_id == parts_with_ranges[i].data_part->info.partition_id);
}

MergeTreePartSequentialReader::MergeTreePartSequentialReader(
    const Block & header_,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MarkCachePtr mark_cache_,
    Names columns_to_read_,
    RangesInDataPart part_ranges_)
    : header{header_}
    , storage{storage_}
    , storage_snapshot{storage_snapshot_}
    , mark_cache{std::move(mark_cache_)}
    , columns_to_read{std::move(columns_to_read_)}
    , part_ranges(std::move(part_ranges_))
{
    chassert(part_ranges.ranges.size() == 1);

    initial_mark = part_ranges.ranges.front().begin;
    current_mark = part_ranges.ranges.front().begin;
    current_row = part_ranges.data_part->index_granularity.getMarkStartingRow(current_mark);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(
        LoadedMergeTreeDataPartInfoForReader(part_ranges.data_part, part_ranges.alter_conversions),
        storage_snapshot,
        storage.supportsSubcolumns(),
        columns_to_read);

    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
                       .withExtendedObjects()
                       .withVirtuals()
                       .withSubcolumns(storage.supportsSubcolumns());

    NamesAndTypesList columns_for_reader = storage_snapshot->getColumnsByNames(options, columns_to_read);

    const auto & context = storage.getContext();
    ReadSettings read_settings = context->getReadSettings();

    MergeTreeReaderSettings reader_settings = {
        .read_settings = read_settings,
        .save_marks_in_cache = false,
        .apply_deleted_mask = false,
    };

    reader = part_ranges.data_part->getReader(
        columns_for_reader,
        storage_snapshot,
        part_ranges.ranges,
        /*virtual_fields=*/{},
        /*uncompressed_cache=*/{},
        mark_cache.get(),
        part_ranges.alter_conversions,
        reader_settings,
        {},
        {});
}

bool MergeTreePartSequentialReader::hasSome() const
{
    return current_row < part_ranges.data_part->rows_count;
}

bool MergeTreePartSequentialReader::isEmpty() const
{
    return !hasSome();
}

Chunk MergeTreePartSequentialReader::readNext()
{
    size_t rows_to_read = part_ranges.data_part->index_granularity.getMarkRows(current_mark);
    bool continue_reading = (current_mark != initial_mark);

    const auto & sample = reader->getColumns();
    Columns columns(sample.size());
    size_t rows_read = reader->readRows(current_mark, part_ranges.data_part->getMarksCount(), continue_reading, rows_to_read, columns);
    chassert(rows_read != 0);

    reader->fillVirtualColumns(columns, rows_read);

    current_row += rows_read;
    current_mark += (rows_to_read == rows_read);

    bool should_evaluate_missing_defaults = false;
    reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_read);

    if (should_evaluate_missing_defaults)
        reader->evaluateMissingDefaults({}, columns);

    reader->performRequiredConversions(columns);

    /// Reorder columns and fill result block.
    size_t num_columns = sample.size();
    Columns res_columns;
    res_columns.reserve(num_columns);

    auto it = sample.begin();
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (header.has(it->name))
        {
            columns[i]->assumeMutableRef().shrinkToFit();
            res_columns.emplace_back(std::move(columns[i]));
        }

        ++it;
    }

    return Chunk(std::move(res_columns), rows_read);
}

MergeTreePartitionSequentialSource::MergeTreePartitionSequentialSource(
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    RangesInDataParts parts_with_ranges_,
    Names columns_to_read_)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
    , storage{storage_}
    , storage_snapshot{storage_snapshot_}
    , parts_with_ranges{std::move(parts_with_ranges_)}
    , mark_cache{storage.getContext()->getMarkCache()}
    , columns_to_read{std::move(columns_to_read_)}
{
    checkSinglePartition(parts_with_ranges);

    std::ranges::sort(parts_with_ranges, [](const RangesInDataPart & lhs, const RangesInDataPart & rhs) {
        return lhs.data_part->info.min_block < rhs.data_part->info.min_block;
    });

    for (const auto & part : parts_with_ranges)
        for (const auto & range : part.ranges)
            addTotalRowsApprox(part.data_part->index_granularity.getRowsCountInRange(range));

    initNextReader();
}

Chunk MergeTreePartitionSequentialSource::generate()
{
    while (!isCancelled() && reader.has_value())
    {
        if (reader->isEmpty())
        {
            initNextReader();
            continue;
        }

        Chunk next_chunk = reader->readNext();
        chassert(next_chunk);

        return next_chunk;
    }

    return {};
}

void MergeTreePartitionSequentialSource::initNextReader()
{
    reader.reset();

    if (next_part_to_read < parts_with_ranges.size())
    {
        LOG_INFO(log, "Initializing reader for part: {}", parts_with_ranges[next_part_to_read].data_part->name);

        reader.emplace(
            getPort().getHeader(), storage, storage_snapshot, mark_cache, columns_to_read, std::move(parts_with_ranges[next_part_to_read]));

        next_part_to_read += 1;
    }
}

Pipe createMergeTreePartitionSequentialSource(
    const MergeTreeData & storage, const StorageSnapshotPtr & storage_snapshot, RangesInDataParts parts_with_ranges, Names columns_to_read)
{
    auto partition_source
        = std::make_shared<MergeTreePartitionSequentialSource>(storage, storage_snapshot, parts_with_ranges, columns_to_read);

    return Pipe(std::move(partition_source));
}

}
