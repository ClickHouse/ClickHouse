#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeMutateSelectedEntry.h>
#include <Interpreters/MergeTreeTransactionHolder.h>


namespace DB
{

class StorageMergeTree;

class MergePlainMergeTreeTask : public IExecutableTask
{
public:
    MergePlainMergeTreeTask(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        bool cleanup_,
        MergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder table_lock_holder_,
        IExecutableTask::TaskResultCallback & task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(std::move(deduplicate_by_columns_))
        , cleanup(cleanup_)
        , merge_mutate_entry(std::move(merge_mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(task_result_callback_)
    {
        for (auto & item : merge_mutate_entry->future_part->parts)
            priority.value += item->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() const override;
    Priority getPriority() const override { return priority; }
    String getQueryId() const override { return getStorageID().getShortName() + "::" + merge_mutate_entry->future_part->name; }

    void setCurrentTransaction(MergeTreeTransactionHolder && txn_holder_, MergeTreeTransactionPtr && txn_)
    {
        txn_holder = std::move(txn_holder_);
        txn = std::move(txn_);
    }

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
    bool deduplicate;
    Names deduplicate_by_columns;
    bool cleanup;
    MergeMutateSelectedEntryPtr merge_mutate_entry{nullptr};
    TableLockHolder table_lock_holder;
    FutureMergedMutatedPartPtr future_part{nullptr};
    MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    Priority priority;

    std::function<void(const ExecutionStatus &)> write_part_log;
    std::function<void()> transfer_profile_counters_to_initial_query;
    IExecutableTask::TaskResultCallback task_result_callback;
    MergeTaskPtr merge_task{nullptr};

    MergeTreeTransactionHolder txn_holder;
    MergeTreeTransactionPtr txn;

    ProfileEvents::Counters profile_counters;

    ContextMutablePtr task_context;

    ContextMutablePtr createTaskContext() const;
};


using MergePlainMergeTreeTaskPtr = std::shared_ptr<MergePlainMergeTreeTask>;


[[ maybe_unused ]] static void executeHere(MergePlainMergeTreeTaskPtr task)
{
    while (task->executeStep()) {}
}


}
