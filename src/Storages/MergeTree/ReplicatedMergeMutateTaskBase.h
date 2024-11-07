#pragma once


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
    ReplicatedMergeMutateTaskBase(
        LoggerPtr log_,
        StorageReplicatedMergeTree & storage_,
        ReplicatedMergeTreeQueue::SelectedEntryPtr & selected_entry_,
        IExecutableTask::TaskResultCallback & task_result_callback_)
        : storage(storage_)
        , selected_entry(selected_entry_)
        , entry(*selected_entry->log_entry)
        , log(log_)
        /// This is needed to ask an asssignee to assign a new merge/mutate operation
        /// It takes bool argument and true means that current task is successfully executed.
        , task_result_callback(task_result_callback_)
    {
    }

    ~ReplicatedMergeMutateTaskBase() override = default;
    void onCompleted() override;
    StorageID getStorageID() const override;
    String getQueryId() const override { return getStorageID().getShortName() + "::" + selected_entry->log_entry->new_part_name; }
    bool executeStep() override;

    bool printExecutionException() const override { return print_exception; }

protected:
    using PartLogWriter =  std::function<void(const ExecutionStatus &)>;

    struct PrepareResult
    {
        bool prepared_successfully;
        bool need_to_check_missing_part_in_fetch;
        PartLogWriter part_log_writer;
    };

    virtual PrepareResult prepare() = 0;
    virtual bool finalize(ReplicatedMergeMutateTaskBase::PartLogWriter write_part_log) = 0;

    /// Will execute a part of inner MergeTask or MutateTask
    virtual bool executeInnerTask() = 0;

    StorageReplicatedMergeTree & storage;

    /// A callback to reschedule merge_selecting_task after destroying merge_mutate_entry
    /// The order is important, because merge_selecting_task may rely on the number of entries in MergeList
    scope_guard finish_callback;

    /// This is important not to execute the same mutation in parallel
    /// selected_entry is a RAII class, so the time of living must be the same as for the whole task
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry;
    ReplicatedMergeTreeLogEntry & entry;
    MergeList::EntryPtr merge_mutate_entry{nullptr};
    LoggerPtr log;
    /// ProfileEvents for current part will be stored here
    ProfileEvents::Counters profile_counters;
    ContextMutablePtr task_context;

private:
    enum class CheckExistingPartResult : uint8_t
    {
        PART_EXISTS,
        OK
    };

    CheckExistingPartResult checkExistingPart();
    bool executeImpl();

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE_INNER_MERGE,
        NEED_FINALIZE,

        SUCCESS
    };

    PartLogWriter part_log_writer{};
    State state{State::NEED_PREPARE};
    IExecutableTask::TaskResultCallback task_result_callback;
    bool print_exception = true;
};

}
