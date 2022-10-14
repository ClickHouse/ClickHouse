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
    template <class Callback>
    MergePlainMergeTreeTask(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        MergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder table_lock_holder_,
        Callback && task_result_callback_)
        : storage(storage_)
        , metadata_snapshot(std::move(metadata_snapshot_))
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(std::move(deduplicate_by_columns_))
        , merge_mutate_entry(std::move(merge_mutate_entry_))
        , table_lock_holder(std::move(table_lock_holder_))
        , task_result_callback(std::forward<Callback>(task_result_callback_))
    {
        for (auto & item : merge_mutate_entry->future_part->parts)
            priority += item->getBytesOnDisk();
    }

    bool executeStep() override;
    void onCompleted() override;
    StorageID getStorageID() override;
    UInt64 getPriority() override { return priority; }

    void setCurrentTransaction(MergeTreeTransactionHolder && txn_holder_, MergeTreeTransactionPtr && txn_)
    {
        txn_holder = std::move(txn_holder_);
        txn = std::move(txn_);
    }

private:

    void prepare();
    void finish();

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
    bool deduplicate;
    Names deduplicate_by_columns;
    std::shared_ptr<MergeMutateSelectedEntry> merge_mutate_entry{nullptr};
    TableLockHolder table_lock_holder;
    FutureMergedMutatedPartPtr future_part{nullptr};
    MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    using MergeListEntryPtr = std::unique_ptr<MergeListEntry>;
    MergeListEntryPtr merge_list_entry;

    UInt64 priority{0};

    std::function<void(const ExecutionStatus &)> write_part_log;
    IExecutableTask::TaskResultCallback task_result_callback;
    MergeTaskPtr merge_task{nullptr};

    MergeTreeTransactionHolder txn_holder;
    MergeTreeTransactionPtr txn;
};


using MergePlainMergeTreeTaskPtr = std::shared_ptr<MergePlainMergeTreeTask>;


[[ maybe_unused ]] static void executeHere(MergePlainMergeTreeTaskPtr task)
{
    while (task->executeStep()) {}
}


}
