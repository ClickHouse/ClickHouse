#pragma once

#include <Storages/MergeTree/MergeTreeReadPoolBase.h>

namespace DB
{

/// A read pool specialized for reading from projection index parts.
class MergeTreeReadPoolProjectionIndex : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolProjectionIndex(
        MutationsSnapshotPtr mutations_snapshot_,
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolProjectionIndex"; }
    bool preservesOrderOfRanges() const override { return true; }
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;
    MergeTreeReadTaskPtr getTask(const RangesInDataPart & part);
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
};

}
