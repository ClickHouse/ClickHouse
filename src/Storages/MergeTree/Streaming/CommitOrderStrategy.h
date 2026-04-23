#pragma once

#include <Storages/StorageSnapshot.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>

namespace DB
{

/// Produces per-part sub-pipelines that read a part's rows in commit order,
/// i.e. ascending by `(_block_number, _block_offset)`.
class CommitOrderReadStrategy
{
    enum class Kind : uint8_t
    {
        /// Reading the part in its native storage order already yields commit order.
        Native,
        /// The part must be sorted by `(_block_number, _block_offset)` after reading.
        Sort,
    };

    struct Decision
    {
        Kind kind = Kind::Sort;
        RangesInDataPart ranges_to_read;
        StorageMetadataPtr metadata_to_use;
    };

    Decision chooseKind(const RangesInDataPart & ranges) const;
    Pipe readSinglePartInOrder(const RangesInDataPart & ranges, const StorageMetadataPtr & metadata) const;
    void addSortTransforms(Pipe & pipe) const;

public:
    CommitOrderReadStrategy(
        const MergeTreeData & storage_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
        Names columns_to_read_,
        MergeTreeReaderSettings reader_settings_,
        ExpressionActionsSettings actions_settings_,
        MergeTreeReadTask::BlockSizeParams block_size_params_,
        VirtualFields shared_virtual_fields_,
        IndexReadTasks index_read_tasks_,
        PrewhereInfoPtr prewhere_info_,
        FilterDAGInfoPtr row_level_filter_,
        ContextPtr context_);

    /// Builds a sub-pipeline that reads one part and emits its rows in commit order.
    Pipe createReadStream(const RangesInDataPart & ranges) const;

private:
    const MergeTreeData & storage;
    const StorageSnapshotPtr storage_snapshot;
    const MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    const Names columns_to_read;
    const MergeTreeReaderSettings reader_settings;
    const ExpressionActionsSettings actions_settings;
    const MergeTreeReadTask::BlockSizeParams block_size_params;
    const VirtualFields shared_virtual_fields;
    const IndexReadTasks index_read_tasks;
    const PrewhereInfoPtr prewhere_info;
    const FilterDAGInfoPtr row_level_filter;
    const ContextPtr context;
};

using CommitOrderReadStrategyPtr = std::shared_ptr<const CommitOrderReadStrategy>;

}
