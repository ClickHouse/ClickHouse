#include <exception>
#include <memory>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Interpreters/InsertDeduplication.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Common/logger_useful.h>
#include <Common/ProfileEventsScope.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
    extern const Event SelfDuplicatedAsyncInserts;
    extern const Event DuplicatedAsyncInserts;
    extern const Event DuplicationElapsedMicroseconds;
}


namespace DB
{

namespace ErrorCodes
{
    extern const int INSERT_WAS_DEDUPLICATED;
}

namespace Setting
{
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsUInt64 non_replicated_deduplication_window;
}

MergeTreeSink::~MergeTreeSink()
{
    if (!delayed_chunk)
        return;

    chassert(isCancelled() || std::uncaught_exceptions());

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part->cancel();
    }

    delayed_chunk.reset();
}

MergeTreeSink::MergeTreeSink(
    StorageMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    size_t max_parts_per_block_,
    ContextPtr context_)
    : SinkToStorage(std::make_shared<const Block>(metadata_snapshot_->getSampleBlock()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , max_parts_per_block(max_parts_per_block_)
    , context(context_)
    , storage_snapshot(storage.getStorageSnapshotWithoutData(metadata_snapshot, context_))
    , deduplicate((*storage.getSettings())[MergeTreeSetting::non_replicated_deduplication_window] > 0 && storage.getDeduplicationLog() != nullptr)
{
    LOG_DEBUG(storage.log, "Create MergeTreeSink, deduplicate={}", deduplicate);
}

void MergeTreeSink::onStart()
{
    /// It's only allowed to throw "too many parts" before write,
    /// because interrupting long-running INSERT query in the middle is not convenient for users.
    storage.delayInsertOrThrowIfNeeded(nullptr, context, true);
}

void MergeTreeSink::onFinish()
{
    if (isCancelled())
        return;

    finishDelayedChunk();
}

void MergeTreeSink::consume(Chunk & chunk)
{
    if (num_blocks_processed > 0)
        storage.delayInsertOrThrowIfNeeded(nullptr, context, false);

    auto block = getHeader().cloneWithColumns(chunk.getColumns());

    auto deduplication_info = chunk.getChunkInfos().getSafe<DeduplicationInfo>();
    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    using DelayedPartitions = std::vector<MergeTreeDelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t total_streams = 0;
    bool support_parallel_write = false;

    std::vector<UInt128> all_partwriter_hashes;
    all_partwriter_hashes.reserve(part_blocks.size());

    for (auto & current_block : part_blocks)
    {
        ProfileEvents::Counters part_counters;
        auto partition_scope = std::make_unique<ProfileEventsScope>(&part_counters);

        auto current_deduplication_info = deduplication_info->cloneSelf();

        {
            ProfileEventTimeIncrement<Microseconds> duplication_elapsed(ProfileEvents::DuplicationElapsedMicroseconds);

            auto result = current_deduplication_info->deduplicateSelf(deduplicate, current_block.partition_id, context);
            if (result.removed_rows > 0)
            {
                ProfileEvents::increment(ProfileEvents::SelfDuplicatedAsyncInserts, result.removed_tokens);
                LOG_DEBUG(
                    storage.log,
                    "In partition {} self deduplication removed tokens {} out of {}, left rows {} in tokens {}, debug: {}",
                    current_block.partition_id,
                    result.removed_tokens,
                    current_deduplication_info->getCount(),
                    result.filtered_block->rows(),
                    result.deduplication_info->getCount(),
                    result.deduplication_info->debug());

                current_block.block = std::move(result.filtered_block);
                current_deduplication_info = std::move(result.deduplication_info);
            }
        }

        UInt64 elapsed_ns = 0;
        TemporaryPartPtr temp_part;

        {
            Stopwatch watch;
            temp_part = writeNewTempPart(current_block);
            elapsed_ns = watch.elapsed();
        }

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part->part)
            continue;

        auto hash = temp_part->part->getPartBlockIDHash();
        current_deduplication_info->setPartWriterHashForPartition(hash, current_block.block->rows());
        all_partwriter_hashes.push_back(hash);

        LOG_DEBUG(
            storage.log,
            "Wrote block with {} rows and deduplication blocks: {}, deduplication info: {}",
            current_block.block->rows(),
            fmt::join(current_deduplication_info->getBlockIds(current_block.partition_id, deduplicate), ", "),
            current_deduplication_info->debug());


        // if the token is already defined, it would not be owerrided again
        /// TODO: set part writer hashes for multiple partitions in one chunk

        if (!support_parallel_write && temp_part->part->getDataPartStorage().supportParallelWrite())
            support_parallel_write = true;

        size_t max_insert_delayed_streams_for_parallel_write;

        if (settings[Setting::max_insert_delayed_streams_for_parallel_write].changed)
            max_insert_delayed_streams_for_parallel_write = settings[Setting::max_insert_delayed_streams_for_parallel_write];
        else if (support_parallel_write)
            max_insert_delayed_streams_for_parallel_write = DEFAULT_DELAYED_STREAMS_FOR_PARALLEL_WRITE;
        else
            max_insert_delayed_streams_for_parallel_write = 0;

        /// In case of too much columns/parts in block, flush explicitly.
        size_t current_streams = 0;
        for (const auto & stream : temp_part->streams)
            current_streams += stream.stream->getNumberOfOpenStreams();

        if (total_streams + current_streams > max_insert_delayed_streams_for_parallel_write)
        {
            finishDelayedChunk();
            delayed_chunk = std::make_unique<MergeTreeDelayedChunk>();
            delayed_chunk->partitions = std::move(partitions);
            finishDelayedChunk();

            total_streams = 0;
            support_parallel_write = false;
            partitions = DelayedPartitions{};
        }

        // partition_scope must be reset before part_counters is moved
        partition_scope.reset();

        partitions.emplace_back(MergeTreeDelayedChunk::Partition
        {
            .log = storage.log.load(),
            .block_with_partition = std::move(current_block),
            .deduplication_info = std::move(current_deduplication_info),
            .temp_part = std::move(temp_part),
            .elapsed_ns = elapsed_ns,
            .part_counters = std::move(part_counters),
        });

        total_streams += current_streams;
    }
    deduplication_info->setPartWriterHashes(all_partwriter_hashes, chunk.getNumRows());

    finishDelayedChunk();
    delayed_chunk = std::make_unique<MergeTreeDelayedChunk>();
    delayed_chunk->partitions = std::move(partitions);

    ++num_blocks_processed;
}

void MergeTreeSink::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        Stopwatch watch;
        auto profile_events_scope = std::make_unique<ProfileEventsScope>(&partition.part_counters);

        auto retry_times = 0;
        while (true)
        {
            partition.temp_part->finalize();

            auto & part = partition.temp_part->part;
            auto block_ids = partition.deduplication_info->getBlockIds(part->info.getPartitionId(), deduplicate);
            auto conflicts = commitPart(part, block_ids);

            if (conflicts.empty())
            {
                partition.temp_part->prewarmCaches();

                profile_events_scope.reset();
                partition.elapsed_ns += watch.elapsed();
                auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());

                PartLog::addNewPart(
                    storage.getContext(),
                    PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot),
                    block_ids);
                StorageMergeTree::incrementInsertedPartsProfileEvent(part->getType());

                /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
                storage.background_operations_assignee.trigger();
                break;
            }

            ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks, conflicts.size());

            auto result = partition.deduplication_info->deduplicateBlock(
                conflicts,
                partition.block_with_partition.partition_id,
                context);

            if (partition.deduplication_info->isAsyncInsert())
                ProfileEvents::increment(ProfileEvents::DuplicatedAsyncInserts, result.removed_tokens);
            else
                ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks, result.removed_tokens);

            LOG_DEBUG(
                storage.log,
                "After filtering by collision at {} try,"
                " removed rows {}/{}"
                " removed tokets {}/{} from origin block,"
                " after retry remaining rows: {},"
                " remaining tokens: {},"
                " new deduplication info debug: {}",\
                retry_times,
                result.removed_rows,
                partition.deduplication_info->getRows(),
                result.removed_tokens,
                partition.deduplication_info->getCount(),
                result.filtered_block->rows(),
                result.deduplication_info->getCount(),
                result.deduplication_info->debug());

            if (result.filtered_block->rows() == 0)
            {
                LOG_DEBUG(
                    storage.log,
                    "All rows are deduplicated for part with block IDs: {}, skipping the part commit.",
                    fmt::join(conflicts, ", "));

                profile_events_scope.reset();
                partition.elapsed_ns += watch.elapsed();
                auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());

                PartLog::addNewPart(
                    storage.getContext(),
                    PartLog::PartLogEntry(partition.temp_part->part, partition.elapsed_ns, counters_snapshot),
                    block_ids,
                    ExecutionStatus(ErrorCodes::INSERT_WAS_DEDUPLICATED, "The part was deduplicated"));

                break;
            }

            partition.block_with_partition.block = result.filtered_block;
            partition.deduplication_info = std::move(result.deduplication_info);

            partition.temp_part = writeNewTempPart(partition.block_with_partition);

            ++retry_times;
        }
    }

    delayed_chunk.reset();
}

MergeTreeTemporaryPartPtr MergeTreeSink::writeNewTempPart(BlockWithPartition & block)
{
    return storage.writer.writeTempPart(block, metadata_snapshot, context);
}

std::vector<std::string> MergeTreeSink::commitPart(MergeTreeMutableDataPartPtr & part, const std::vector<String> & block_ids)
{
    /// It's important to create it outside of lock scope because
    /// otherwise it can lock parts in destructor and deadlock is possible.
    MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
    {
        auto lock = storage.lockParts();
        auto block_holder = storage.fillNewPartName(part, lock);

        if (!block_ids.empty())
        {
            auto * deduplication_log = storage.getDeduplicationLog();
            chassert(deduplication_log);
            auto result = deduplication_log->addPart(block_ids, part->info);

            std::vector<std::string> conflict_block_ids;
            for (const auto & res : result)
            {
                LOG_INFO(storage.log, "Block with ID {} already exists as part {}; ignoring it", res.block_id, res.part_info.getPartNameForLogs());
                conflict_block_ids.push_back(res.block_id);
            }

            if (!conflict_block_ids.empty())
                return conflict_block_ids;
        }

        /// FIXME: renames for MergeTree should be done under the same lock
        /// to avoid removing extra covered parts after merge.
        ///
        /// Image the following:
        /// - T1: all_2_2_0 is in renameParts()
        /// - T2: merge assigned for [all_1_1_0, all_3_3_0]
        /// - T1: renameParts() finished, part had been added as Active
        /// - T2: merge finished, covered parts removed, and it will include all_2_2_0!
        ///
        /// Hence, for now rename_in_transaction is false.
        storage.renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
        transaction.commit(lock);
    }

    return {};
}

}
