#include <Storages/MergeTree/ReplicatedMergeTreeSinkPatch.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Interpreters/InsertDeduplication.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

ReplicatedMergeTreeSinkPatch::ReplicatedMergeTreeSinkPatch(
    StorageReplicatedMergeTree & storage_,
    StorageMetadataPtr metadata_snapshot_,
    LightweightUpdateHolderInKeeper update_holder_,
    ContextPtr context_)
    : ReplicatedMergeTreeSink(
        /*async_insert=*/ false,
        storage_,
        metadata_snapshot_,
        /*quorum=*/ 0,
        /*quorum_timeout_ms=*/ 0,
        /*max_parts_per_block=*/ 0,
        /*quorum_parallel=*/ false,
        /*majority_quorum=*/ false,
        std::move(context_))
    , update_holder(std::move(update_holder_))
{
    deduplicate = false;
}

ReplicatedMergeTreeSinkPatch::~ReplicatedMergeTreeSinkPatch()
{
    auto component_guard = Coordination::setCurrentComponent("ReplicatedMergeTreeSinkPatch::~ReplicatedMergeTreeSinkPatch");
    update_holder.reset();
}

void ReplicatedMergeTreeSinkPatch::finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (delayed_parts.empty())
        return;

    for (auto & partition : delayed_parts)
    {
        partition.temp_part->finalize();
        ProfileEventsScope profile_events_scope;

        auto & part = partition.temp_part->part;
        if (!part->info.isPatch())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch part, got part with name: {}", part->name);

        auto deduplication_hashes = partition.deduplication_info->getDeduplicationHashes(partition.block_with_partition.partition_id, deduplicate);
        auto deduplication_blocks_ids = getDeduplicationBlockIds(deduplication_hashes);
        try
        {
            auto conflicts = commitPart(zookeeper, part, deduplication_hashes, deduplication_blocks_ids);
            if (!conflicts.empty())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Patch part {} was deduplicated. It's a bug", part->name);

            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), deduplication_blocks_ids, ExecutionStatus(0));
            StorageReplicatedMergeTree::incrementInsertedPartsProfileEvent(part->getType());
        }
        catch (...)
        {
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), deduplication_blocks_ids, ExecutionStatus::fromCurrentException(__PRETTY_FUNCTION__));
            throw;
        }
    }

    delayed_parts.clear();
}

TemporaryPartPtr ReplicatedMergeTreeSinkPatch::writeNewTempPart(BlockWithPartition & block)
{
    storage.throwLightweightUpdateIfNeeded(block.block->bytes());

    auto partition_id = getPartitionIdForPatch(block.partition);
    auto data_version = getDataVersionInPartition(partition_id);

    auto source_parts_set = buildSourceSetForPatch(*block.block, data_version);
    return storage.writer.writeTempPatchPart(block, metadata_snapshot, std::move(partition_id), std::move(source_parts_set), context);
}

UInt64 ReplicatedMergeTreeSinkPatch::getDataVersionInPartition(const String & partition_id) const
{
    auto original_partition_id = getOriginalPartitionIdOfPatch(partition_id);

    const auto & block_numbers = update_holder.partition_block_numbers.getBlockNumbers();
    auto it = block_numbers.find(original_partition_id);

    if (it == block_numbers.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Not found block for partition id {}", original_partition_id);

    return it->second;
}

}
