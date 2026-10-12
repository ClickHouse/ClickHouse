#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

namespace DB
{

class MergeTreeReadPoolInOrder : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolInOrder(
        bool has_hard_limit_below_one_block_,
        bool has_soft_limit_below_one_block_,
        MergeTreeReadType read_type_,
        RangesInDataParts parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const IndexReadTasks & index_read_tasks_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_,
        RuntimeDataflowStatisticsCacheUpdaterPtr updater_);

    String getName() const override { return "ReadPoolInOrder"; }
    bool preservesOrderOfRanges() const override { return true; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    /// Hard limit case (no filter): we will stop reading exactly at the limit, so always emit
    /// single-range tasks to avoid reading more rows than necessary.
    const bool has_hard_limit_below_one_block;

    /// Soft limit case (WHERE + LIMIT): the limit is only an estimation since the filter may
    /// drop rows. Emit a single-range task only for the first call per part - if it didn't fill
    /// the limit, the filter is likely selective and we should switch to regular block size.
    const bool has_soft_limit_below_one_block;

    const MergeTreeReadType read_type;
    RuntimeDataflowStatisticsCacheUpdaterPtr updater;

    std::vector<MarkRanges> per_part_mark_ranges;
};

}
