#pragma once

#include <functional>

#include <Core/Names.h>

#include <Storages/MergeTree/BackgroundTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeMutateSelectedEntry.h>

namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct FutureMergedMutatedPart;
using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

class StorageMergeTree;

class MutatePlainMergeTreeTask : public BackgroundTask
{
public:
    MutatePlainMergeTreeTask(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        MergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder & table_lock_holder_)
        : BackgroundTask()
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , merge_mutate_entry(merge_mutate_entry_)
        , table_lock_holder(table_lock_holder_) {}

    bool execute() override;

    void onCompleted() override {}

    StorageID getStorageID() override;

private:

    void prepare();

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH,

        SUCCESS
    };

    State state{State::NEED_PREPARE};

    StorageMergeTree & storage;

    StorageMetadataPtr metadata_snapshot;
    MergeMutateSelectedEntryPtr merge_mutate_entry{nullptr};
    TableLockHolder & table_lock_holder;

    FutureMergedMutatedPartPtr future_part{nullptr};
    std::unique_ptr<Stopwatch> stopwatch;

    MergeTreeData::MutableDataPartPtr new_part;

    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    std::function<void(const ExecutionStatus & execution_status)> write_part_log;

    MutateTaskPtr mutate_task;
};


}
