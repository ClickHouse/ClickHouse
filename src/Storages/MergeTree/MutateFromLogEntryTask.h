#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ZeroCopyLock.h>
#include <Storages/StorageReplicatedMergeTree.h>

namespace DB
{

class MutateFromLogEntryTask : public ReplicatedMergeMutateTaskBase
{
public:
    template <typename Callback>
    MutateFromLogEntryTask(
        ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_,
        StorageReplicatedMergeTree & storage_,
        Callback && task_result_callback_)
        : ReplicatedMergeMutateTaskBase(
            getLogger(storage_.getStorageID().getShortName() + "::" + selected_entry_->log_entry->new_part_name + " (MutateFromLogEntryTask)"),
            storage_,
            selected_entry_,
            task_result_callback_)
        {}


    Priority getPriority() const override { return priority; }

    void cancel() noexcept override
    {
        if (mutate_task)
            mutate_task->cancel();
    }

private:

    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;

    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;

    bool executeInnerTask() override
    {
        return mutate_task->execute();
    }

    Priority priority;

    TableLockHolder table_lock_holder{nullptr};
    ReservationSharedPtr reserved_space{nullptr};

    MergeTreePartInfo new_part_info;
    MutationCommandsConstPtr commands;

    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;
    StopwatchUniquePtr stopwatch_ptr{nullptr};

    MergeTreeData::MutableDataPartPtr new_part{nullptr};
    FutureMergedMutatedPartPtr future_mutated_part{nullptr};

    MutateTaskPtr mutate_task;
};


}
