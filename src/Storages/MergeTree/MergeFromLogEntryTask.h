#pragma once

#include <memory>
#include <utility>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <Storages/MergeTree/ReplicatedMergeMutateTaskBase.h>
#include <Storages/MergeTree/ZeroCopyLock.h>


namespace DB
{

class MergeFromLogEntryTask : public ReplicatedMergeMutateTaskBase
{
public:
    MergeFromLogEntryTask(
        ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry_,
        StorageReplicatedMergeTree & storage_,
        IExecutableTask::TaskResultCallback & task_result_callback_);

    UInt64 getPriority() override { return priority; }

protected:
    /// Both return false if we can't execute merge.
    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;
    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;

    bool executeInnerTask() override
    {
        return merge_task->execute();
    }

private:
    TableLockHolder table_lock_holder{nullptr};

    MergeTreeData::DataPartsVector parts;
    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;

    StopwatchUniquePtr stopwatch_ptr{nullptr};
    MergeTreeData::MutableDataPartPtr part;

    UInt64 priority{0};

    MergeTaskPtr merge_task;
};


using MergeFromLogEntryTaskPtr = std::shared_ptr<MergeFromLogEntryTask>;


}
