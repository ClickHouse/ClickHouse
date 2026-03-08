#pragma once

#include <memory>

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

    Priority getPriority() const override { return priority; }

    void cancel() noexcept override
    {
        if (merge_task)
            merge_task->cancel();

        if (part)
            part->removeIfNeeded();
    }

protected:
    /// Both return false if we can't execute merge.
    ReplicatedMergeMutateTaskBase::PrepareResult prepare() override;
    bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) override;

    bool executeInnerTask() override
    {
        return merge_task->execute();
    }

private:
    ContextMutablePtr createTaskContext() const override;

    TableLockHolder table_lock_holder{nullptr};

    MergeTreeData::DataPartsVector parts;
    MergeTreeData::DataPartsVector patch_parts;
    MergeTreeData::TransactionUniquePtr transaction_ptr{nullptr};
    std::optional<ZeroCopyLock> zero_copy_lock;

    MergeTreeData::MutableDataPartPtr part;

    Priority priority;

    MergeTaskPtr merge_task;
};


using MergeFromLogEntryTaskPtr = std::shared_ptr<MergeFromLogEntryTask>;


}
