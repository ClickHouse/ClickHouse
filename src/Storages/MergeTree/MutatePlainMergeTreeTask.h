#pragma once

#include <functional>

#include <Core/Names.h>

#include <Storages/MergeTree/IExecutableTask.h>
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

class MutatePlainMergeTreeTask : public IExecutableTask
{
public:
    MutatePlainMergeTreeTask(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        MergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder table_lock_holder_,
        IExecutableTask::TaskResultCallback & task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , merge_mutate_entry(std::move(merge_mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(task_result_callback_)
    {
        for (auto & part : merge_mutate_entry->future_part->parts)
            priority += part->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() override;
    UInt64 getPriority() override { return priority; }

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
    TableLockHolder table_lock_holder;
    FutureMergedMutatedPartPtr future_part{nullptr};
    std::unique_ptr<Stopwatch> stopwatch;
    MergeTreeData::MutableDataPartPtr new_part;

    UInt64 priority{0};

    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    std::function<void(const ExecutionStatus & execution_status)> write_part_log;

    IExecutableTask::TaskResultCallback task_result_callback;

    ContextMutablePtr fake_query_context;
    MutateTaskPtr mutate_task;
};


}
