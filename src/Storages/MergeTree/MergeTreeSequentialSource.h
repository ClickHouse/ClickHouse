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
/// `prefetch_begin_of_range`: when the source is created, start prefetching the beginning of its range
///     (`IMergeTreeReader::prefetchBeginOfRange`). The vertical merge uses it for the next column.
/// `allow_remote_fs_prefetch`: if false, the readers of the source do not prefetch at all, also not
///     the beginning of the range; it overrides `remote_filesystem_read_prefetch`.
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
    bool prefetch_begin_of_range,
    bool allow_remote_fs_prefetch);

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
    bool prefetch_begin_of_range,
    bool allow_remote_fs_prefetch,
    ContextPtr context,
    LoggerPtr log);

/// Whether a merge or mutation that reads `columns_to_read` of all `parts` at the same time can prefetch. The limits are
/// the settings of the task context, as for one query: the number of prefetches, one per column of each part, must fit
/// into `filesystem_prefetches_limit`, and their estimated memory, two buffers each, into `filesystem_prefetch_max_memory_usage`.
bool canPrefetchInMergeOrMutation(
    const MergeTreeData & storage,
    const MergeTreeData::DataPartsVector & parts,
    const Names & columns_to_read,
    const ContextPtr & task_context);

}
