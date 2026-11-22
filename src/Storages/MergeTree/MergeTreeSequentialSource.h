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
    /// A mutation that rewrites every column of the part instead of hardlinking the untouched ones
    /// (e.g. of a `Compact` part or a part with packed storage), so it writes every column it reads,
    /// like a merge does.
    MutationOfAllColumns,
    Merge,
};

/// Create stream for reading single part from MergeTree.
/// If the part has lightweight delete mask then the deleted rows are filtered out.
/// `allow_remote_fs_prefetch`: if false, the source does not prefetch at all.
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
    bool allow_remote_fs_prefetch);

class QueryPlan;
class QueryPipeline;

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
    bool allow_remote_fs_prefetch,
    ContextPtr context,
    LoggerPtr log);

/// Starts reading the first range of every `MergeTreeSequentialSource` of the pipeline in the background.
void prefetchMergeTreeSequentialSources(const QueryPipeline & pipeline);

/// Whether a merge or mutation reading `columns_to_read` of `parts` fits into `filesystem_prefetches_limit`
/// and `filesystem_prefetch_max_memory_usage` of `task_context`, as a query does.
bool canPrefetchInMergeOrMutation(
    const MergeTreeData & storage,
    const MergeTreeData::DataPartsVector & parts,
    const Names & columns_to_read,
    const ContextPtr & task_context);

}
