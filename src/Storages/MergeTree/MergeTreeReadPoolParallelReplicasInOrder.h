#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

namespace DB
{

class MergeTreeReadPoolParallelReplicasInOrder : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolParallelReplicasInOrder(
        ParallelReadingExtension extension_,
        CoordinationMode mode_,
        RangesInDataParts parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const IndexReadTasks & index_read_tasks_,
        bool has_hard_limit_below_one_block_,
        bool has_soft_limit_below_one_block_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolParallelReplicasInOrder"; }
    bool preservesOrderOfRanges() const override { return true; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

private:
    LoggerPtr log = getLogger("MergeTreeReadPoolParallelReplicasInOrder");
    const ParallelReadingExtension extension;
    const CoordinationMode mode;

    /// Hard limit case (no filter): we will stop reading exactly at the limit, so always emit
    /// single-range tasks to avoid reading more rows than necessary.
    const bool has_hard_limit_below_one_block;

    /// Soft limit case (WHERE + LIMIT): the limit is only an estimation since the filter may
    /// drop rows. Emit a single-range task only for the first call per part - if it didn't fill
    /// the limit, the filter is likely selective and we should switch to regular block size.
    const bool has_soft_limit_below_one_block;

    size_t min_marks_per_task{0};
    bool no_more_tasks{false};
    RangesInDataPartsDescription request;
    RangesInDataPartsDescription buffered_tasks;

    /// See the comment in MergeTreeReadPoolParallelReplicas::getTask method.
    bool failed_to_get_task{false};

    mutable std::mutex mutex;
    std::vector<size_t> per_part_marks_in_range;
};

};
