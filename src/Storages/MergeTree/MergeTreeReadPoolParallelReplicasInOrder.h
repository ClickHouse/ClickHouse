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
        const StorageSnapshotPtr & storage_snapshot_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const Names & virtual_column_names_,
        const PoolSettings & settings_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolParallelReplicasInOrder"; }
    bool preservesOrderOfRanges() const override { return true; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

private:
    const ParallelReadingExtension extension;
    const CoordinationMode mode;

    bool no_more_tasks{false};
    RangesInDataPartsDescription request;
    RangesInDataPartsDescription buffered_tasks;

    mutable std::mutex mutex;
};

};
