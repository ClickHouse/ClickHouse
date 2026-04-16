#include <Storages/MergeTree/MergeTreeSinkPatch.h>
#include <Storages/StorageMergeTree.h>
#include <Storages/MergeTree/PatchParts/SourcePartsSetForPatch.h>
#include <Interpreters/InsertDeduplication.h>
#include <Common/ProfileEventsScope.h>

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
    String v2_sort_key_expr_list_sql_,
    std::vector<UInt8> v2_sort_key_reverse_flags_,
    ContextPtr context_)
    : MergeTreeSink(
        storage_,
        std::move(metadata_snapshot_),
        /*max_parts_per_block=*/ 0,
        std::move(context_))
    , update_holder(std::move(update_holder_))
    , v2_sort_key_expr_list_sql(std::move(v2_sort_key_expr_list_sql_))
    , v2_sort_key_reverse_flags(std::move(v2_sort_key_reverse_flags_))
{
}

void MergeTreeSinkPatch::finishDelayedChunk()
{
    if (!delayed_chunk)
        return;

    for (auto & partition : delayed_chunk->partitions)
    {
        ProfileEventsScope scoped_attach(&partition.part_counters);
        partition.temp_part->finalize();

        auto & part = partition.temp_part->part;

        const auto deduplication_hashes = partition.deduplication_info->getDeduplicationHashes(part->info.getPartitionId(), storage.getDeduplicationLog() != nullptr);

        auto conflicts = commitPart(part, deduplication_hashes);

        if (!conflicts.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Patch part {} was deduplicated. It's a bug", part->name);

        auto counters_snapshot = std::make_shared<ProfileEvents::Counters::Snapshot>(partition.part_counters.getPartiallyAtomicSnapshot());
        auto block_ids = getDeduplicationBlockIds(deduplication_hashes);
        PartLog::addNewPart(storage.getContext(), PartLog::PartLogEntry(part, partition.elapsed_ns, counters_snapshot), block_ids);
        StorageMergeTree::incrementInsertedPartsProfileEvent(part->getType());

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

    /// Stamp the v2 format marker + sort-key AST (as SQL) + DESC flags so readers can rebuild
    /// the full `KeyDescription` — including the `ExpressionActions` for expression sort keys —
    /// without depending on (possibly-ALTERed) current main metadata. An empty AST SQL means
    /// v1 — we only stamp v2 when the caller decided this UPDATE writes a v2 patch.
    if (!v2_sort_key_expr_list_sql.empty() || !v2_sort_key_reverse_flags.empty())
    {
        source_parts_set.setFormatVersion(SourcePartsSetForPatch::V2_FORMAT_VERSION);
        source_parts_set.setSortKey(v2_sort_key_expr_list_sql, v2_sort_key_reverse_flags);
    }

    return storage.writer.writeTempPatchPart(block, metadata_snapshot, std::move(partition_id), std::move(source_parts_set), context);
}

}
