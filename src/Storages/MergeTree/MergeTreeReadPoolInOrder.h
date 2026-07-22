#pragma once
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

namespace DB
{

class MergeTreeReadPoolInOrder : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolInOrder(
        bool has_limit_below_one_block_,
        MergeTreeReadType read_type_,
        RangesInDataParts parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolInOrder"; }
    bool preservesOrderOfRanges() const override { return true; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}

private:
    const bool has_limit_below_one_block;
    const MergeTreeReadType read_type;

    std::vector<MarkRanges> per_part_mark_ranges;
};

}
