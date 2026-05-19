#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

namespace DB
{

class MergeTreeReadPoolParallelReplicas : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolParallelReplicas(
        ParallelReadingExtension extension_,
        RangesInDataParts && parts_,
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
        const ContextPtr & context_);

    ~MergeTreeReadPoolParallelReplicas() override = default;

    String getName() const override { return "ReadPoolParallelReplicas"; }
    bool preservesOrderOfRanges() const override { return false; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

    /// Aggregate per-part `min_marks_per_task` into a single value (max-across-parts). Throws if
    /// the resulting value is zero. Used by the caller before sending the initial announcement.
    static size_t getMinMarksPerTask(size_t min_marks_for_concurrent_read, const std::vector<MergeTreeReadTaskInfoPtr> & infos);

    /// Pick `mark_segment_size` based on the user-provided value (0 = auto), aggregate
    /// `min_marks_per_task * threads`, and `sum_marks / number_of_replicas^2`. See the comment
    /// in `chooseSegmentSize` for the heuristic.
    static size_t chooseSegmentSize(
        LoggerPtr log,
        size_t mark_segment_size,
        size_t min_marks_per_task,
        size_t threads,
        size_t sum_marks,
        size_t number_of_replicas);

    size_t getMinMarksPerRequest() const { return min_marks_per_request; }
    size_t getMarkSegmentSize() const { return mark_segment_size; }

private:
    mutable std::mutex mutex;

    LoggerPtr log = getLogger("MergeTreeReadPoolParallelReplicas");
    const ParallelReadingExtension extension;
    const CoordinationMode coordination_mode;

    /// Retained for backward compatibility with old initiators that read it from each read request.
    /// New initiators (protocol >= DBMS_PARALLEL_REPLICAS_MIN_VERSION_WITH_MIN_MARKS_PER_TASK)
    /// use the value from the initial announcement instead.
    size_t min_marks_per_request{0};
    size_t mark_segment_size{0};

    RangesInDataPartsDescription buffered_ranges;
    bool no_more_tasks_available{false};

    /// See the comment in getTask method.
    bool failed_to_get_task{false};
};

}
