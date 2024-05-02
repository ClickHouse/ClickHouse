#include <base/defines.h>

#include <Common/logger_useful.h>

#include <Processors/Chunk.h>

#include <Storages/MarkCache.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/Streaming/MergeTreePartitionSequentialSource.h>
#include <Storages/MergeTree/Streaming/RangesInDataPartStreamSubscription.h>

namespace DB
{

PartitionIdChunkInfo::PartitionIdChunkInfo(String partition_id_) : partition_id(std::move(partition_id_))
{
}

MergeTreePartSequentialReader::MergeTreePartSequentialReader(
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    DynamicBlockTransformer & transformer_,
    MarkCachePtr mark_cache_,
    Names columns_to_read_,
    RangesInDataPart part_ranges_,
    std::shared_ptr<PartitionIdChunkInfo> info_)
    : storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , transformer(transformer_)
    , mark_cache(std::move(mark_cache_))
    , columns_to_read(std::move(columns_to_read_))
    , part_ranges(std::move(part_ranges_))
    , info(std::move(info_))
{
    chassert(part_ranges.ranges.size() == 1);
    chassert(part_ranges.alter_conversions != nullptr);

    initial_mark = part_ranges.ranges.front().begin;
    current_mark = part_ranges.ranges.front().begin;
    current_row = part_ranges.data_part->index_granularity.getMarkStartingRow(current_mark);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(
        LoadedMergeTreeDataPartInfoForReader(part_ranges.data_part, part_ranges.alter_conversions),
        storage_snapshot,
        storage.supportsSubcolumns(),
        columns_to_read);

    NamesAndTypesList columns_for_reader = part_ranges.data_part->getColumns().addTypes(columns_to_read);

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
    ColumnsWithTypeAndName res_columns;
    res_columns.reserve(num_columns);

    auto it = sample.begin();
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (transformer.getHeader().has(it->name))
        {
            columns[i]->assumeMutableRef().shrinkToFit();
            res_columns.emplace_back(std::move(columns[i]), it->type, it->name);
        }

        ++it;
    }

    Block result_block(std::move(res_columns));
    transformer.transform(result_block);

    Chunk chunk(result_block.getColumns(), rows_read);
    chunk.setChunkInfo(info, PartitionIdChunkInfo::INFO_SLOT);

    return chunk;
}

MergeTreePartitionSequentialSource::MergeTreePartitionSequentialSource(
    const MergeTreeData & storage_, StorageSnapshotPtr storage_snapshot_, StreamSubscriptionPtr subscription_, Names columns_to_read_)
    : QueueSubscriptionSourceAdapter<RangesInDataPart>(
        storage_snapshot_->getSampleBlockForColumns(columns_to_read_), *subscription_->as<RangesInDataPartStreamSubscription>(), getName())
    , storage(storage_)
    , subscription_holder(std::move(subscription_))
    , columns_to_read(std::move(columns_to_read_))
    , storage_snapshot(std::move(storage_snapshot_))
    , transformer(getPort().getHeader())
{
}

Chunk MergeTreePartitionSequentialSource::useCachedData()
{
    if (reader.has_value() && reader->hasSome())
    {
        Chunk next_chunk = reader->readNext();
        chassert(next_chunk);
        return next_chunk;
    }

    /// have no reader or it is empty -> must reinit reader
    /// drop it explicitly in any case.
    reader.reset();

    while (!cached_data.empty())
    {
        reader.reset();
        initNextReader();
        chassert(reader.has_value());

        if (reader->hasSome())
        {
            Chunk next_chunk = reader->readNext();
            chassert(next_chunk);
            return next_chunk;
        }
    }

    need_new_data = true;

    return Chunk();
}

void MergeTreePartitionSequentialSource::initNextReader()
{
    chassert(!reader.has_value() && !cached_data.empty());
    storage_snapshot = storage.getStorageSnapshotWithoutData(storage.getInMemoryMetadataPtr(), storage.getContext());

    RangesInDataPart ranges_in_data_part = cached_data.front();
    cached_data.pop_front();

    const auto & partition_id = ranges_in_data_part.data_part->info.partition_id;
    const auto & part_name = ranges_in_data_part.data_part->name;

    LOG_INFO(log, "Initializing reader for part {} in partition {}", part_name, partition_id);

    auto info_it = partition_infos.find(partition_id);
    if (info_it == partition_infos.end())
    {
        auto [insert_it, inserted] = partition_infos.insert({partition_id, std::make_shared<PartitionIdChunkInfo>(partition_id)});
        info_it = insert_it;
    }

    chassert(info_it != partition_infos.end());

    reader.emplace(
        storage,
        storage_snapshot,
        transformer,
        storage.getContext()->getMarkCache(),
        columns_to_read,
        std::move(ranges_in_data_part),
        info_it->second);
}

Pipe createMergeTreePartitionSequentialSource(
    const MergeTreeData & storage, const StorageSnapshotPtr & storage_snapshot, StreamSubscriptionPtr subscription, Names columns_to_read)
{
    auto partition_source = std::make_shared<MergeTreePartitionSequentialSource>(storage, storage_snapshot, subscription, columns_to_read);
    return Pipe(std::move(partition_source));
}

}
