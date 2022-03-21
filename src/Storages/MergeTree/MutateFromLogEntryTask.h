#pragma once

#include <base/shared_ptr_helper.h>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ZeroCopyLock.h>

namespace DB
{

class MutateFromLogEntryTask : public shared_ptr_helper<MutateFromLogEntryTask>, public ReplicatedMergeMutateTaskBase
{
public:
    template <typename Callback>
    MutateFromLogEntryTask(
        ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_,
        StorageReplicatedMergeTree & storage_,
        Callback && task_result_callback_)
        : ReplicatedMergeMutateTaskBase(&Poco::Logger::get("MutateFromLogEntryTask"), storage_, selected_entry_, task_result_callback_) {}

    UInt64 getPriority() override { return priority; }

private:
    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;
    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;

    bool executeInnerTask() override
    {
        return mutate_task->execute();
    }

    UInt64 priority{0};

    TableLockHolder table_lock_holder{nullptr};
    ReservationSharedPtr reserved_space{nullptr};

    MergeTreePartInfo new_part_info;
    MutationCommandsConstPtr commands;

    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;
    StopwatchUniquePtr stopwatch_ptr{nullptr};

    MergeTreeData::MutableDataPartPtr new_part{nullptr};
    FutureMergedMutatedPartPtr future_mutated_part{nullptr};

    ContextMutablePtr fake_query_context;
    MutateTaskPtr mutate_task;
};


}
