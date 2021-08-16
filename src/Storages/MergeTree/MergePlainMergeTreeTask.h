#pragma once

#include <Storages/MergeTree/BackgroundTask.h>
#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeMutateSelectedEntry.h>

namespace DB
{

class StorageMergeTree;

class MergePlainMergeTreeTask : public BackgroundTask
{
public:
    MergePlainMergeTreeTask(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        MergeMutateSelectedEntryPtr merge_mutate_entry_,
        TableLockHolder table_lock_holder_)
        : BackgroundTask()
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(deduplicate_by_columns_)
        , merge_mutate_entry(merge_mutate_entry_)
        , table_lock_holder(table_lock_holder_) {}

    bool execute() override;

    bool completedSuccessfully() override
    {
        return state == State::SUCCESS;
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

    std::function<void(const ExecutionStatus &)> write_part_log;

    MergeTaskPtr merge_task{nullptr};
};


using MergePlainMergeTreeTaskPtr = std::shared_ptr<MergePlainMergeTreeTask>;


[[ maybe_unused ]] static void executeHere(MergePlainMergeTreeTaskPtr task)
{
    while (task->execute()) {}
}


}
