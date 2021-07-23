#pragma once

#include <Storages/MergeTree/BackgroundTask.h>
#include <Storages/MergeTree/MutateTask.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>

namespace DB
{

class StorageReplicatedMergeTree;

class MutateFromLogEntryTask : public ReplicatedMergeMutateTaskBase
{

public:
    MutateFromLogEntryTask(ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_, StorageReplicatedMergeTree & storage_);


private:
    bool prepare() override;

    bool executeInnerTask() override
    {
        return mutate_task->execute();
    }

    bool finalize() override;

    TableLockHolder table_lock_holder{nullptr};
    ReservationSharedPtr reserved_space{nullptr};

    MergeTreePartInfo new_part_info;
    MutationCommandsConstPtr commands;

    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    StopwatchUniquePtr stopwatch_ptr{nullptr};

    MergeTreeData::MutableDataPartPtr new_part{nullptr};
    FutureMergedMutatedPartPtr future_mutated_part{nullptr};

    MutateTaskPtr mutate_task;
};


}
