#pragma once
#include <Processors/ISource.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <Storages/MergeTree/MarkRange.h>
#include <memory>

namespace DB
{

/// Create stream for reading single part from MergeTree.
/// If the part has lightweight delete mask then the deleted rows are filtered out.
Pipe createMergeTreeSequentialSource(
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    Names columns_to_read,
    bool read_with_direct_io,
    bool take_column_types_from_storage,
    bool quiet,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count);

class QueryPlan;

void createMergeTreeSequentialSource(
    QueryPlan & plan,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    Names columns_to_read,
    bool apply_deleted_mask,
    ActionsDAGPtr filter,
    ContextPtr context,
    Poco::Logger * log);

}
