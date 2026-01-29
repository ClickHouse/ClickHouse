#include <Storages/MergeTree/ReplicatedMergeTreeSinkPatch.h>
#include <Storages/StorageReplicatedMergeTree.h>

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
        storage_,
        metadata_snapshot_,
        /*quorum=*/ 0,
        /*quorum_timeout_ms=*/ 0,
        /*max_parts_per_block=*/ 0,
        /*quorum_parallel=*/ false,
        /*deduplicate=*/ false,
        /*majority_quorum=*/ false,
        std::move(context_))
    , update_holder(std::move(update_holder_))
{
}

void ReplicatedMergeTreeSinkPatch::finishDelayedChunk(const ZooKeeperWithFaultInjectionPtr & zookeeper)
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        partition.temp_part->finalize();
        ProfileEventsScope profile_events_scope;

        auto & part = partition.temp_part->part;
        if (!part->info.isPatch())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected patch part, got part with name: {}", part->name);

        try
        {
            bool part_deduplicated = commitPart(zookeeper, part, partition.block_id, false).second;
            if (part_deduplicated)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Patch part {} was deduplicated. It's a bug", part->name);

            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), {partition.block_id}, ExecutionStatus(0));
            StorageReplicatedMergeTree::incrementInsertedPartsProfileEvent(part->getType());
        }
        catch (...)
        {
            auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
            PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), {partition.block_id}, ExecutionStatus::fromCurrentException(__PRETTY_FUNCTION__));
            throw;
        }
    }

    delayed_chunk.reset();
}

TemporaryPartPtr ReplicatedMergeTreeSinkPatch::writeNewTempPart(BlockWithPartition & block)
{
    storage.throwLightweightUpdateIfNeeded(block.block.bytes());

    auto partition_id = getPartitionIdForPatch(block.partition);
    auto data_version = getDataVersionInPartition(partition_id);

    auto source_parts_set = buildSourceSetForPatch(block.block, data_version);
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
