#include <Storages/MergeTree/MergeTreeSinkPatch.h>
#include <Storages/StorageMergeTree.h>
#include <Interpreters/InsertDeduplication.h>
#include <Common/setThreadName.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

MergeTreeSinkPatch::MergeTreeSinkPatch(
    StorageMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    PlainLightweightUpdateHolder update_holder_,
    ContextPtr context_)
    : MergeTreeSink(
        storage_,
        std::move(metadata_snapshot_),
        /*max_parts_per_block=*/ 0,
        std::move(context_))
    , update_holder(std::move(update_holder_))
{
}

void MergeTreeSinkPatch::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        auto thread_group_switcher = ThreadGroupSwitcher(partition.thread_group, ThreadName::MERGETREE_WRITE_PART, /*allow_existing_group*/ true);

        partition.temp_part->finalize();

        auto & part = partition.temp_part->part;

        const auto deduplication_hashes = partition.deduplication_info->getDeduplicationHashes(part->info.getPartitionId(), storage.getDeduplicationLog() != nullptr);

        auto conflicts = commitPart(part, deduplication_hashes);

        if (!conflicts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Patch part {} was deduplicated. It's a bug", part->name);

        StorageMergeTree::incrementInsertedPartsProfileEvent(part->getType());

        auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.thread_group->performance_counters.getPartiallyAtomicSnapshot());
        auto block_ids = getDeduplicationBlockIds(deduplication_hashes);
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.thread_group->getGroupElapsedNs(), counters_snapshot), block_ids);

        /// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
        storage.background_operations_assignee.trigger();
    }

    delayed_chunk.reset();
}

TemporaryPartPtr MergeTreeSinkPatch::writeNewTempPart(BlockWithPartition & block)
{
    storage.throwLightweightUpdateIfNeeded(block.block->bytes());

    auto partition_id = getPartitionIdForPatch(block.partition);
    UInt64 block_number = update_holder.block_holder->block.number;

    auto source_parts_set = buildSourceSetForPatch(*block.block, block_number);
    return storage.writer.writeTempPatchPart(block, metadata_snapshot, std::move(partition_id), std::move(source_parts_set), context);
}

}
