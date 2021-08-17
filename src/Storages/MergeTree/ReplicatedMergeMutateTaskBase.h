#pragma once

#include <common/logger_useful.h>

#include <Storages/MergeTree/BackgroundTask.h>
#include <Storages/MergeTree/ReplicatedMergeTreeQueue.h>

namespace DB
{

class StorageReplicatedMergeTree;

/**
 * This is used as a base of MergeFromLogEntryTask and MutateFromLogEntryTaskBase
 */
class ReplicatedMergeMutateTaskBase : public BackgroundTask
{
public:
    ReplicatedMergeMutateTaskBase(Poco::Logger * log_, StorageReplicatedMergeTree & storage_, ReplicatedMergeTreeQueue::SelectedEntryPtr & selected_entry_)
        : BackgroundTask()
        , selected_entry(selected_entry_)
        , entry(*selected_entry->log_entry)
        , log(log_)
        , storage(storage_) {}

    ~ReplicatedMergeMutateTaskBase() override = default;

    void onCompleted() override;

    StorageID getStorageID() override;

    bool execute() override;

protected:
    void prepareCommon();
    virtual bool prepare() = 0;
    bool executeImpl() ;
    virtual bool finalize() = 0;

    /// Will execute a part of inner MergeTask or MutateTask
    virtual bool executeInnerTask() = 0;

    /// This is important not to execute the same mutation in parallel
    /// selected_entry is a RAII class, so the time of living must be the same as for the whole task
    ReplicatedMergeTreeQueue::SelectedEntryPtr selected_entry;
    ReplicatedMergeTreeLogEntry & entry;

    MergeList::EntryPtr merge_mutate_entry{nullptr};

    Poco::Logger * log;
    std::function<void(const ExecutionStatus &)> write_part_log;

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE_INNER_MERGE,
        NEED_COMMIT,

        SUCCESS,

        CANT_MERGE_NEED_FETCH
    };

    State state{State::NEED_PREPARE};
    StorageReplicatedMergeTree & storage;
private:

};

}
