#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>
#include <DataTypes/ObjectUtils.h>
#include <Common/ProfileEventsScope.h>

#include <Storages/MergeTree/Unique/WriteState.h>

namespace ProfileEvents
{
extern const Event DuplicatedInsertedBlocks;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

TableVersionPtr MergeTreeSink::updateDeleteBitmapAndTableVersion(
    MutableDataPartPtr & part,
    const MergeTreePartInfo & part_info,
    PrimaryIndex::DeletesMap & deletes_map,
    const PrimaryIndex::DeletesKeys & deletes_keys)
{
    /// Note: delete bitmap located in the directory of data part(in same disk),
    /// but table version always located in the first disk
    auto current_version = storage.table_version->get();
    auto new_table_version = std::make_unique<TableVersion>(*current_version);
    new_table_version->version++;

    if (!new_table_version->part_versions.insert({part_info, new_table_version->version}).second)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Insert new inserted part version into table version failed, this is a bug, new part name: {}",
            part_info.getPartNameForLogs());
    }

    /// Generate delete bitmap for new part
    auto new_delete_bitmap = std::make_shared<DeleteBitmap>(new_table_version->version);
    if (auto it = deletes_map.find(part_info.min_block); it != deletes_map.end())
    {
        LOG_INFO(storage.log, "{} rows deleted for new inserted part {}", it->second.size(), part_info.getPartNameForLogs());
        new_delete_bitmap->addDels(it->second);
        deletes_map.erase(it);
    }
    new_delete_bitmap->serialize(part->getDataPartStoragePtr());
    auto & delete_bitmap_cache = storage.delete_bitmap_cache;
    delete_bitmap_cache->set({part_info, new_table_version->version}, new_delete_bitmap);

    /// Update delete bitmap
    /// Here find part info by min_block
    for (const auto & [min_block, row_numbers] : deletes_map)
    {
        auto info = storage.findPartInfoByMinBlock(min_block);
        auto update_part = storage.findPartByInfo(info);
        if (!update_part)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not find part in data_parts_by_info, this is a bug, part name: {}",
                info.getPartNameForLogs());
        }
        /// The part is do merging, add its deleted keys to delete buffer
        if (storage.currently_merging_mutating_parts.find(update_part) != storage.currently_merging_mutating_parts.end())
        {
            storage.delete_buffer->insertKeysByInfo(info, deletes_keys.at(min_block));
            /// Here even if the part is merging, we still should update its delete bitmap,
            /// such that even if the merge failed or abort due to too much deletes, the data
            /// still consistent.
            // continue;
        }
        const auto & part_version = current_version->part_versions.find(info);
        if (part_version == current_version->part_versions.end())
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Can not find part versions in table version");
        }

        LOG_INFO(
            storage.log,
            "{} rows deleted for part {} when insert into part {}",
            row_numbers.size(),
            info.getPartNameForLogs(),
            part_info.getPartNameForLogs());

        IMergeTreeDataPart * mutable_part = const_cast<IMergeTreeDataPart *>(update_part.get());

        auto bitmap = delete_bitmap_cache->getOrCreate(update_part, part_version->second);
        auto new_bitmap = bitmap->addDelsAsNewVersion(new_table_version->version, row_numbers);
        new_bitmap->serialize(mutable_part->getDataPartStoragePtr());

        if (auto it = new_table_version->part_versions.find(info); it != new_table_version->part_versions.end())
        {
            it->second = new_table_version->version;
        }
        else
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not find old part version in table version, this is a bug, part name: {}",
                info.getPartNameForLogs());
        }
        delete_bitmap_cache->set({info, new_table_version->version}, new_bitmap);
    }
    return new_table_version;
}

struct MergeTreeSink::DelayedChunk
{
    struct Partition
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        WriteStatePtr write_state;
        UInt64 elapsed_ns;
        String block_dedup_token;
        ProfileEvents::Counters part_counters;
    };

    std::vector<Partition> partitions;
};


MergeTreeSink::~MergeTreeSink() = default;

MergeTreeSink::MergeTreeSink(
    StorageMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    size_t max_parts_per_block_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutParts(metadata_snapshot))
{
}

void MergeTreeSink::onStart()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(nullptr, context);
}

void MergeTreeSink::onFinish()
{
    finishDelayedChunk();
}

void MergeTreeSink::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    if (!storage_snapshot->object_columns.empty())
        convertDynamicColumnsToTuples(block, storage_snapshot);

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context);

    using DelayedPartitions = std::vector<MergeTreeSink::DelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t streams = 0;
    bool support_parallel_write = false;

    for (auto & current_block : part_blocks)
    {
        ProfileEvents::Counters part_counters;

        WriteStatePtr write_state = nullptr;

        if (storage.merging_params.mode == MergeTreeData::MergingParams::Mode::Unique)
        {
            write_state = std::make_shared<WriteState>();
        }
        UInt64 elapsed_ns = 0;
        MergeTreeDataWriter::TemporaryPart temp_part;

        {
            ProfileEventsScope scoped_attach(&part_counters);

            Stopwatch watch;
            temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context, write_state);
            elapsed_ns = watch.elapsed();
        }

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part.part)
            continue;

        if (!support_parallel_write && temp_part.part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        String block_dedup_token;
        if (storage.getDeduplicationLog())
        {
            const String & dedup_token = settings.insert_deduplication_token;
            if (!dedup_token.empty())
            {
                /// multiple blocks can be inserted within the same insert query
                /// an ordinal number is added to dedup token to generate a distinctive block id for each block
                block_dedup_token = fmt::format("{}_{}", dedup_token, chunk_dedup_seqnum);
                ++chunk_dedup_seqnum;
            }
        }

        size_t max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        if (!support_parallel_write || settings.max_insert_delayed_streams_for_parallel_write.changed)
            max_insert_delayed_streams_for_parallel_write = settings.max_insert_delayed_streams_for_parallel_write;

        /// In case of too much columns/parts in block, flush explicitly.
        streams += temp_part.streams.size();
        if (streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayedChunk();
            delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk();

            streams = 0;
            support_parallel_write = false;
            partitions = DelayedPartitions{};
        }

        partitions.emplace_back(MergeTreeSink::DelayedChunk::Partition{
            .temp_part = std::move(temp_part),
            .write_state = write_state,
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = std::move(block_dedup_token),
            .part_counters = std::move(part_counters),
        });
    }

    finishDelayedChunk();
    delayed_chunk = std::make_unique<MergeTreeSink::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);
}

void MergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        ProfileEventsScope scoped_attach(&partition.part_counters);

        partition.temp_part.finalize();

        auto & part = partition.temp_part.part;

        bool added = false;

        /// It's important to create it outside of lock scope because
        /// otherwise it can lock parts in destructor and deadlock is possible.
        MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
        {
            if (storage.merging_params.mode == MergeTreeData::MergingParams::Mode::Unique)
            {
                std::lock_guard<std::mutex> write_merge_lock(storage.write_merge_lock);

                auto tmp_lock = std::unique_lock<std::mutex>();
                /// The lock is useless, since the operation is atomic
                storage.fillNewPartName(part, tmp_lock);

                auto partition_id = part->info.partition_id;
                auto primary_index = storage.primary_index_cache->getOrCreate(partition_id, part->partition);

                const auto & write_state = partition.write_state;
                PrimaryIndex::DeletesMap deletes_map;
                PrimaryIndex::DeletesKeys deletes_keys;

                /// We should fist set it to UPDATE, then updating. Otherwise, if
                /// query cancel or exception happened in update, the state become inconsistent
                primary_index->setState(PrimaryIndex::State::UPDATED);
                if (!storage.merging_params.version_column.empty())
                {
                    primary_index->update(
                        part->info.min_block,
                        write_state->key_column,
                        write_state->version_column,
                        write_state->delete_key_column,
                        write_state->min_key_values,
                        write_state->max_key_values,
                        deletes_map,
                        deletes_keys,
                        context);
                }
                else
                {
                    primary_index->update(
                        part->info.min_block,
                        write_state->key_column,
                        write_state->delete_key_column,
                        write_state->min_key_values,
                        write_state->max_key_values,
                        deletes_map,
                        deletes_keys,
                        context);
                }

                /// Should lock after update PrimaryIndex, otherwise, dead lock will happen in
                /// StorageMergeTree::getFirstAlterMutationCommandsForPart
                std::lock_guard<std::mutex> background_lock(storage.currently_processing_in_background_mutex);
                auto new_table_version = updateDeleteBitmapAndTableVersion(part, part->info, deletes_map, deletes_keys);

                auto lock = storage.lockParts();

                auto * deduplication_log = storage.getDeduplicationLog();
                if (deduplication_log)
                {
                    const String block_id = part->getZeroLevelPartBlockID(partition.block_dedup_token);
                    auto res = deduplication_log->addPart(block_id, part->info);
                    if (!res.second)
                    {
                        ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                        LOG_INFO(
                            storage.log,
                            "Block with ID {} already exists as part {}; ignoring it",
                            block_id,
                            res.first.getPartNameForLogs());
                        continue;
                    }
                }

                added = storage.renameTempPartAndAdd(part, transaction, lock, std::move(new_table_version));
                transaction.commit(&lock);

                /// If add part failed, the primary_index can become INVALID
                if (added)
                    primary_index->setState(PrimaryIndex::State::VALID);
            }

            else
            {
                auto lock = storage.lockParts();
                storage.fillNewPartName(part, lock);

                auto * deduplication_log = storage.getDeduplicationLog();
                if (deduplication_log)
                {
                    const String block_id = part->getZeroLevelPartBlockID(partition.block_dedup_token);
                    auto res = deduplication_log->addPart(block_id, part->info);
                    if (!res.second)
                    {
                        ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                        LOG_INFO(
                            storage.log,
                            "Block with ID {} already exists as part {}; ignoring it",
                            block_id,
                            res.first.getPartNameForLogs());
                        continue;
                    }
                }

                added = storage.renameTempPartAndAdd(part, transaction, lock);
                transaction.commit(&lock);
            }
        }

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (added)
        {
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot));
            storage.incrementInsertedPartsProfileEvent(part->getType());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger();
        }
    }

    delayed_chunk.reset();
}

}
