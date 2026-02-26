#include <exception>
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/PartLog.h>
#include <Interpreters/Context.h>
#include <Processors/Transforms/DeduplicationTokenTransforms.h>
#include <Common/ProfileEventsScope.h>
#include <Core/Settings.h>


namespace ProfileEvents
{
    extern const Event DuplicatedInsertedBlocks;
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{
namespace Setting
{
    extern const SettingsBool insert_deduplicate;
    extern const SettingsUInt64 max_insert_delayed_streams_for_parallel_write;
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
{
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
    auto part_blocks = MergeTreeDataWriter::splitBlockIntoParts(std::move(block), max_parts_per_block, metadata_snapshot, context);

    using DelayedPartitions = std::vector<MergeTreeDelayedChunk::Partition>;
    DelayedPartitions partitions;

    const Settings & settings = context->getSettingsRef();
    size_t total_streams = 0;
    bool support_parallel_write = false;

    auto token_info = chunk.getChunkInfos().get<DeduplicationToken::TokenInfo>();
    if (!token_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "TokenInfo is expected for consumed chunk in MergeTreeSink for table: {}",
            storage.getStorageID().getNameForLogs());

    const bool need_to_define_dedup_token = !token_info->isDefined();

    String block_dedup_token;
    if (token_info->isDefined())
        block_dedup_token = token_info->getToken();

    for (auto & current_block : part_blocks)
    {
        ProfileEvents::Counters part_counters;

        UInt64 elapsed_ns = 0;
        TemporaryPartPtr temp_part;

        {
            ProfileEventsScope scoped_attach(&part_counters);

            Stopwatch watch;
            temp_part = writeNewTempPart(current_block);
            elapsed_ns = watch.elapsed();
        }

        /// Reset earlier to free memory
        current_block.block.clear();
        current_block.partition = {};

        /// If optimize_on_insert setting is true, current_block could become empty after merge
        /// and we didn't create part.
        if (!temp_part->part)
            continue;

        if (need_to_define_dedup_token)
        {
            chassert(temp_part->part);
            const auto hash_value = temp_part->part->getPartBlockIDHash();
            token_info->addChunkHash(toString(hash_value.items[0]) + "_" + toString(hash_value.items[1]));
        }

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

        partitions.emplace_back(MergeTreeDelayedChunk::Partition
        {
            .temp_part = std::move(temp_part),
            .elapsed_ns = elapsed_ns,
            .block_dedup_token = block_dedup_token,
            .part_counters = std::move(part_counters),
        });

        total_streams += current_streams;
    }

    if (need_to_define_dedup_token)
    {
        token_info->finishChunkHashes();
    }

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
        ProfileEventsScope scoped_attach(&partition.part_counters);

        partition.temp_part->finalize();

        auto & part = partition.temp_part->part;
        bool added = commitPart(part, partition.block_dedup_token);

        /// Part can be deduplicated, so increment counters and add to part log only if it's really added
        if (added)
        {
            auto block_id = String{};
            if (context->getSettingsRef()[Setting::insert_deduplicate] && storage.getDeduplicationLog())
                block_id = part->getNewPartBlockID(partition.block_dedup_token);

            partition.temp_part->prewarmCaches();

            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(
                storage.getContext(),
                PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot),
                {block_id});
            StorageMergeTree::incrementInsertedPartsProfileEvent(part->getType());

            /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
            storage.background_operations_assignee.trigger();
        }
    }

    delayed_chunk.reset();
}

MergeTreeTemporaryPartPtr MergeTreeSink::writeNewTempPart(BlockWithPartition & block)
{
    return storage.writer.writeTempPart(block, metadata_snapshot, context);
}

bool MergeTreeSink::commitPart(MergeTreeMutableDataPartPtr & part, const String & deduplication_token)
{
    bool added = false;

    /// It's important to create it outside of lock scope because
    /// otherwise it can lock parts in destructor and deadlock is possible.
    MergeTreeData::Transaction transaction(storage, context->getCurrentTransaction().get());
    {
        auto lock = storage.lockParts();
        auto block_holder = storage.fillNewPartName(part, lock);

        auto * deduplication_log = storage.getDeduplicationLog();

        if (context->getSettingsRef()[Setting::insert_deduplicate] && deduplication_log)
        {
            const String block_id = part->getNewPartBlockID(deduplication_token);
            auto res = deduplication_log->addPart(block_id, part->info);
            if (!res.second)
            {
                ProfileEvents::increment(ProfileEvents::DuplicatedInsertedBlocks);
                LOG_INFO(storage.log, "Block with ID {} already exists as part {}; ignoring it", block_id, res.first.getPartNameForLogs());
                return false;
            }
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
        added = storage.renameTempPartAndAdd(part, transaction, lock, /*rename_in_transaction=*/ false);
        transaction.commit(lock);
    }

    return added;
}

}
