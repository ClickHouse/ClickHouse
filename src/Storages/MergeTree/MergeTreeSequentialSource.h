#pragma once
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <memory>

namespace DB
{

class MergedPartOffsets;
using MergedPartOffsetsPtr = std::shared_ptr<MergedPartOffsets>;

enum MergeTreeSequentialSourceType
{
    Mutation,
    Merge,
};

/// Create stream for reading single part from MergeTree.
/// If the part has lightweight delete mask then the deleted rows are filtered out.
/// `prefetch` prefetches the beginning of the range. If `allow_prefetch` is false, the source does not prefetch at all.
/// `task_context` is the context of the merge or mutation task. It gives `filesystem_prefetches_limit` of the readers.
Pipe createMergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    RangesInDataPart data_part,
    AlterConversionsPtr alter_conversions,
    MergedPartOffsetsPtr merged_part_offsets,
    Names columns_to_read,
    std::optional<MarkRanges> mark_ranges,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    bool read_with_direct_io,
    bool prefetch,
    bool allow_prefetch,
    const ContextPtr & task_context);

class QueryPlan;

void createReadFromPartStep(
    MergeTreeSequentialSourceType type,
    QueryPlan & plan,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    RangesInDataPart data_part,
    AlterConversionsPtr alter_conversions,
    MergedPartOffsetsPtr merged_part_offsets,
    Names columns_to_read,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    std::optional<ActionsDAG> filter,
    bool read_with_direct_io,
    bool prefetch,
    bool allow_prefetch,
    ContextPtr context,
    LoggerPtr log);

/// Whether a merge or mutation that reads `columns_to_read` of all `parts` at the same time can prefetch. Its estimated
/// prefetch memory, two buffers per column of each part, must fit into `filesystem_prefetch_max_memory_usage` of the task context.
bool canPrefetchInMergeOrMutation(
    const MergeTreeData & storage,
    const MergeTreeData::DataPartsVector & parts,
    const Names & columns_to_read,
    const ContextPtr & task_context);

}
