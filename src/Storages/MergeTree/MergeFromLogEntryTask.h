#pragma once

#include <memory>

#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>


namespace DB
{

class StorageReplicatedMergeTree;


class MergeFromLogEntryTask : public BackgroundTask
{
public:
    MergeFromLogEntryTask(ReplicatedMergeTreeLogEntry::Ptr entry_, StorageReplicatedMergeTree & storage_);

    bool execute() override;

private:

    bool executeImpl();

    /// Returs false if we can't execute merge.
    bool prepare();
    bool commit();


    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE_INNER_MERGE,
        NEED_COMMIT,

        SUCCESS,

        CANT_MERGE_NEED_FETCH
    };

    State state{State::NEED_PREPARE};
    ReplicatedMergeTreeLogEntry::Ptr entry;
    StorageReplicatedMergeTree & storage;
    Poco::Logger * log;

    MergeTreeData::DataPartsVector parts;

    std::unique_ptr<MergeTreeData::Transaction> transaction_ptr{nullptr};
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
    MergeTreeData::MutableDataPartPtr part;

    std::function<void(const ExecutionStatus &)> write_part_log;

    MergeTaskPtr merge_task;
};


using MergeFromLogEntryTaskPtr = std::shared_ptr<MergeFromLogEntryTask>;


}
