#pragma once

#include <base/logger_useful.h>

#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>

namespace DB
{

class StorageReplicatedMergeTree;

/**
 * This is used as a base of MergeFromLogEntryTask and MutateFromLogEntryTaskBase
 */
class ReplicatedMergeMutateTaskBase : public IExecutableTask
{
public:
    template <class Callback>
    ReplicatedMergeMutateTaskBase(
        Poco::Logger * log_,
        StorageReplicatedMergeTree & storage_,
        ReplicatedMergeTreeQueue::SelectedEntryPtr & selected_entry_,
        Callback && task_result_callback_)
        : selected_entry(selected_entry_)
        , entry(*selected_entry->log_entry)
        , log(log_)
        , storage(storage_)
        /// This is needed to ask an asssignee to assign a new merge/mutate operation
        /// It takes bool argument and true means that current task is successfully executed.
        , task_result_callback(task_result_callback_) {}

    ~ReplicatedMergeMutateTaskBase() override = default;
    void onCompleted() override;
    StorageID getStorageID() override;
    bool executeStep() override;

protected:
    using PartLogWriter =  std::function<void(const ExecutionStatus &)>;

    virtual std::pair<bool, PartLogWriter> prepare() = 0;
    virtual bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) = 0;

    /// Will execute a part of inner MergeTask or MutateTask
    virtual bool executeInnerTask() = 0;

    /// This is important not to execute the same mutation in parallel
    /// selected_entry is a RAII class, so the time of living must be the same as for the whole task
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry;
    ReplicatedMergeTreeLogEntry & entry;
    MergeList::EntryPtr merge_mutate_entry{nullptr};
    Poco::Logger * log;
    StorageReplicatedMergeTree & storage;

private:

    enum class CheckExistingPartResult
    {
        PART_EXISTS,
        OK
    };

    CheckExistingPartResult checkExistingPart();
    bool executeImpl() ;

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE_INNER_MERGE,
        NEED_FINALIZE,

        SUCCESS
    };

    PartLogWriter part_log_writer{};
    State state{State::NEED_PREPARE};
    IExecutableTask::TaskResultCallback task_result_callback;
};

}
