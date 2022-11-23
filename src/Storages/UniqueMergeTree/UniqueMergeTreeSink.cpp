#include <Interpreters/PartLog.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataPartInMemory.h>
#include <Storages/StorageUniqueMergeTree.h>
#include <Storages/UniqueMergeTree/UniqueMergeTreeSink.h>
#include <Storages/UniqueMergeTree/UniqueMergeTreeWriteState.h>

#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event DuplicatedInsertedBlocks;
}

namespace DB
{

TableVersionPtr UniqueMergeTreeSink::updateDeleteBitmapAndTableVersion(
    MutableDataPartPtr & part,
    const MergeTreePartInfo & part_info,
    PrimaryIndex::DeletesMap & deletes_map,
    const PrimaryIndex::DeletesKeys & deletes_keys)
{
    /// Note: delete bitmap located in the directory of data part(in same disk),
    /// but table version always located in the first disk
    auto new_part_data_path = part->data_part_storage->getRelativePath();
    auto current_version = storage.currentVersion();
    auto new_table_version = std::make_unique<TableVersion>(*current_version);
    new_table_version->version++;

    if (!new_table_version->part_versions.insert({part_info, new_table_version->version}).second)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Insert new inserted part version into table version failed, this is a bug, new part name: {}",
            part_info.getPartName());
    }

    /// Generate delete bitmap for new part
    auto new_delete_bitmap = std::make_shared<DeleteBitmap>();
    new_delete_bitmap->setVersion(new_table_version->version);
    if (auto it = deletes_map.find(part_info.min_block); it != deletes_map.end())
    {
        LOG_INFO(storage.log, "{} rows deleted for new inserted part {}", it->second.size(), part_info.getPartName());
        new_delete_bitmap->addDels(it->second);
        deletes_map.erase(it);
    }
    new_delete_bitmap->serialize(new_part_data_path + StorageUniqueMergeTree::DELETE_DIR_NAME, part->data_part_storage->getDisk());
    auto & delete_bitmap_cache = storage.deleteBitmapCache();
    delete_bitmap_cache.set({part_info, new_table_version->version}, new_delete_bitmap);

    /// Update delete bitmap
    /// leefeng, here find part info by min_block
    for (const auto & [min_block, row_numbers] : deletes_map)
    {
        auto info = storage.findPartInfoByMinBlock(min_block);
        auto update_part = storage.findPartByInfo(info);
        if (!update_part)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Can not find part in data_parts_by_info, this is a bug, part name: {}", info.getPartName());
        }
        /// The part is do merging, add its deleted keys to delete buffer
        if (storage.currently_merging_mutating_parts.find(update_part) != storage.currently_merging_mutating_parts.end())
        {
            storage.delete_buffer.insertKeysByInfo(info, deletes_keys.at(min_block));
            /// Here even if the part is merging, we still should update its delete bitmap,
            /// such that even if the merge failed or abort due to too much deletes, the data
            /// still consistent.
            // continue;
        }
        const auto & part_version = current_version->part_versions.find(info);
        if (part_version == current_version->part_versions.end())
        {
            throw Exception("Can not find part versions in table version", ErrorCodes::LOGICAL_ERROR);
        }

        LOG_INFO(
            storage.log,
            "{} rows deleted for part {} when insert into part {}",
            row_numbers.size(),
            info.getPartName(),
            part_info.getPartName());

        auto bitmap = delete_bitmap_cache.getOrCreate(update_part, part_version->second);
        auto new_bitmap = bitmap->addDelsAsNewVersion(new_table_version->version, row_numbers);
        auto bitmap_path = update_part->data_part_storage->getRelativePath() + StorageUniqueMergeTree::DELETE_DIR_NAME;
        new_bitmap->serialize(bitmap_path, update_part->data_part_storage->getDisk());

        if (auto it = new_table_version->part_versions.find(info); it != new_table_version->part_versions.end())
        {
            it->second = new_table_version->version;
        }
        else
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Can not find old part version in table version, this is a bug, part name: {}",
                info.getPartName());
        }
        delete_bitmap_cache.set({info, new_table_version->version}, new_bitmap);
    }
    return new_table_version;
}

UniqueMergeTreeSink::~UniqueMergeTreeSink() = default;

UniqueMergeTreeSink::UniqueMergeTreeSink(
    StorageUniqueMergeTree & storage_, StorageMetadataPtr metadata_snapshot_, size_t max_parts_per_block_, ContextPtr context_)
    : SinkToStorage(storage_.getSampleBlockWithDeleteOp())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , context(context_)
    , log(&Poco::Logger::get("UniqueMergeTree(" + storage.getStorageID().getFullTableName() + ")"))
{
}

void UniqueMergeTreeSink::onStart()
{
    /// Only check "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(nullptr, context);
}

void UniqueMergeTreeSink::onFinish()
{
    finishDelayedChunk();
}

struct UniqueMergeTreeSink::DelayedChunk
{
    struct Partition
    {
        MergeTreeDataWriter::TemporaryPart temp_part;
        UniqueMergeTreeWriteState write_state;
        UInt64 elapsed_ns;
        String block_dedup_token;
    };

    std::vector<Partition> partitions;
};


void UniqueMergeTreeSink::consume(Chunk chunk)
{
    auto block = getHeader().cloneWithColumns(chunk.detachColumns());
    auto storage_snapshot = storage.getStorageSnapshot(metadata_snapshot, context);

    auto part_blocks = storage.writer.splitBlockIntoParts(block, max_parts_per_block, metadata_snapshot, context, true);

    using DelayedPartitions = std::vector<UniqueMergeTreeSink::DelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t streams = 0;
    bool support_parallel_write = false;

    for (auto & current_block : part_blocks)
    {
        Stopwatch watch;
        String block_dedup_token;

        UniqueMergeTreeWriteState write_state;
        auto temp_part = storage.writer.writeTempPart(current_block, metadata_snapshot, context, write_state);

        /// This Context is used in select of PrimaryIndex update, such that to avoid dead lock betwwen insert
        /// and truncate/drop
        write_state.context = context;

        UInt64 elapsed_ns = watch.elapsed();

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        /// May have delete
        if (!temp_part.part && !write_state.delete_key_column)
            continue;

        if (!support_parallel_write && temp_part.part->data_part_storage->supportParallelWrite())
            support_parallel_write = true;

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
            delayed_chunk = std::make_unique<UniqueMergeTreeSink::DelayedChunk>();
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk();

            streams = 0;
            support_parallel_write = false;
            partitions = DelayedPartitions{};
        }

        partitions.emplace_back(UniqueMergeTreeSink::DelayedChunk::Partition{
            .temp_part = std::move(temp_part),
            .write_state = std::move(write_state),
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = std::move(block_dedup_token)});
    }

    finishDelayedChunk();
    delayed_chunk = std::make_unique<UniqueMergeTreeSink::DelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);
}

void UniqueMergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        /// No new part, just have delete
        if (!partition.temp_part.part && partition.write_state.delete_key_column)
        {
            if (partition.write_state.delete_key_column
                && storage.updatePrimaryIndexAndDeletes(
                    partition.write_state.partition,
                    partition.write_state.delete_key_column,
                    partition.write_state.min_key_values,
                    partition.write_state.max_key_values,
                    partition.write_state.context))
                storage.background_operations_assignee.trigger();
        }
        /// Have new part, may also have delete
        else
        {
            partition.temp_part.finalize();

            auto & part = partition.temp_part.part;

            bool added = false;

            /// It's important to create it outside of lock scope because
            /// otherwise it can lock parts in destructor and deadlock is possible.
            MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
            {
                /// TODO leefeng, update primary index and delete bitmap should in here
                std::lock_guard<std::mutex> write_merge_lock(storage.write_merge_lock);
                /// Here we should lock currently_processing_in_background_mutex, then lockParts(), otherwise deadlock
                /// will happen. since in scheduleDataProcessingJob(), it will first lock currently_processing_in_background_mutex,
                /// then lockParts() in selectPartsToMerge().
                auto tmp_lock = std::unique_lock<std::mutex>();
                storage.fillNewPartName(part, tmp_lock);

                /// leefeng Here should query primary index and generate delete bitmap
                /// Update primary index
                auto partition_id = part->info.partition_id;
                auto primary_index = storage.primaryIndexCache().getOrCreate(partition_id, part->partition);
                auto part_info = part->info;
                const auto & write_state = partition.write_state;
                PrimaryIndex::DeletesMap deletes_map;
                PrimaryIndex::DeletesKeys deletes_keys;
                if (!storage.merging_params.version_column.empty())
                {
                    primary_index->update(
                        part_info.min_block,
                        write_state.key_column,
                        write_state.version_column,
                        write_state.delete_key_column,
                        write_state.min_key_values,
                        write_state.max_key_values,
                        deletes_map,
                        deletes_keys,
                        context);
                }
                else
                {
                    primary_index->update(
                        part_info.min_block,
                        write_state.key_column,
                        write_state.delete_key_column,
                        write_state.min_key_values,
                        write_state.max_key_values,
                        deletes_map,
                        deletes_keys,
                        context);
                }
                /// Now, the cache in primary index has update, we should mark
                /// the primary index to UPDATED state
                if (!deletes_map.empty())
                    primary_index->setState(PrimaryIndex::State::UPDATED);

                /// Should lock after update PrimaryIndex, otherwise, dead lock will happen in
                /// StorageUniqueMergeTree::getFirstAlterMutationCommandsForPart
                std::lock_guard<std::mutex> background_lock(storage.currently_processing_in_background_mutex);
                auto new_table_version = updateDeleteBitmapAndTableVersion(part, part_info, deletes_map, deletes_keys);

                auto lock = storage.lockParts();

                auto * deduplication_log = storage.getDeduplicationLog();
                if (deduplication_log)
                {
                    const String block_id = part->getZeroLevelPartBlockID(partition.block_dedup_token);
                    auto res = deduplication_log->addPart(block_id, part->info);
                    if (!res.second)
                    {
                        ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                        LOG_INFO(storage.log, "Block with ID {} already exists as part {}; ignoring it", block_id, res.first.getPartName());
                        continue;
                    }
                }

                added = storage.renameTempPartAndAdd(part, transaction, partition.temp_part.builder, lock, std::move(new_table_version));
                transaction.commit(&lock);
                primary_index->setState(PrimaryIndex::State::VALID);
            }

            /// Part can be deduplicated, so increment counters and add to part log only if it's really added
            if (added)
            {
                PartLog::addNewPart(storage.getContext(), part, partition.elapsed_ns);
                storage.incrementInsertedPartsProfileEvent(part->getType());

                /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
                storage.background_operations_assignee.trigger();
            }
        }
    }

    delayed_chunk.reset();
}

}
