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
        IExecutableTask::TaskResultCallback & task_result_callback_);

    bool executeStep() override;
    void cancel() noexcept override;

    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + merge_mutate_entry->future_part->name; }

private:
    void prepare();
    void finish();

    enum class State : uint8_t
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
    MergeTreeData::MutableDataPartPtr new_part;

    Priority priority;

    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    std::function<void(const ExecutionStatus & execution_status)> write_part_log;

    IExecutableTask::TaskResultCallback task_result_callback;
    MutateTaskPtr mutate_task;

    ContextMutablePtr task_context;
    ThreadGroupPtr thread_group;

    ContextMutablePtr createTaskContext() const;
};


}
