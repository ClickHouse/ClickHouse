#pragma once
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <memory>

namespace DB
{

enum MergeTreeSequentialSourceType
{
    Mutation,
    Merge,
};

/// Create stream for reading single part from MergeTree.
/// If the part has lightweight delete mask then the deleted rows are filtered out.
Pipe createMergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    AlterConversionsPtr alter_conversions,
    Names columns_to_read,
    std::optional<MarkRanges> mark_ranges,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    bool read_with_direct_io,
    bool prefetch);

class QueryPlan;

void createReadFromPartStep(
    MergeTreeSequentialSourceType type,
    QueryPlan & plan,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    AlterConversionsPtr alter_conversions,
    Names columns_to_read,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    std::optional<ActionsDAG> filter,
    bool read_with_direct_io,
    bool prefetch,
    ContextPtr context,
    LoggerPtr log);

}
