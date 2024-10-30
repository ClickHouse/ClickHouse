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
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const ContextPtr & context_);

    ~MergeTreeReadPoolParallelReplicas() override = default;

    String getName() const override { return "ReadPoolParallelReplicas"; }
    bool preservesOrderOfRanges() const override { return false; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

private:
    mutable std::mutex mutex;

    LoggerPtr log = getLogger("MergeTreeReadPoolParallelReplicas");
    const ParallelReadingExtension extension;
    const CoordinationMode coordination_mode;
    size_t min_marks_per_task{0};
    size_t mark_segment_size{0};
    RangesInDataPartsDescription buffered_ranges;
    bool no_more_tasks_available{false};
};

}
