#pragma once

#include <Common/ThreadPool.h>

#include <Storages/MergeTree/MergeTask.h>
#include <Storages/MutationCommands.h>


namespace DB
{

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct FutureMergedMutatedPart;
using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;

class StorageMergeTree;


struct CurrentlyMergingPartsTagger
{
    FutureMergedMutatedPartPtr future_part;
    ReservationPtr reserved_space;
    StorageMergeTree & storage;
    // Optional tagger to maintain volatile parts for the JBOD balancer
    std::optional<CurrentlySubmergingEmergingTagger> tagger;

    CurrentlyMergingPartsTagger(
        FutureMergedMutatedPartPtr future_part_,
        size_t total_size,
        StorageMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot,
        bool is_mutation);

    ~CurrentlyMergingPartsTagger();
};

using CurrentlyMergingPartsTaggerPtr = std::unique_ptr<CurrentlyMergingPartsTagger>;

struct MergeMutateSelectedEntry
{
    FutureMergedMutatedPartPtr future_part;
    CurrentlyMergingPartsTaggerPtr tagger;
    MutationCommands commands;
    MergeMutateSelectedEntry(FutureMergedMutatedPartPtr future_part_, CurrentlyMergingPartsTaggerPtr && tagger_, const MutationCommands & commands_)
        : future_part(std::move(future_part_))
        , tagger(std::move(tagger_))
        , commands(commands_)
    {}
};


class MergePlainMergeTreeTask : public PriorityJobContainer::JobWithPriority
{

public:
    MergePlainMergeTreeTask(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        bool deduplicate_,
        const Names & deduplicate_by_columns_,
        std::shared_ptr<MergeMutateSelectedEntry> merge_mutate_entry_,
        TableLockHolder & table_lock_holder_)
        : PriorityJobContainer::JobWithPriority(0) // FIXME: equal priority
        , storage(storage_)
        , metadata_snapshot(metadata_snapshot_)
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(deduplicate_by_columns_)
        , merge_mutate_entry(merge_mutate_entry_)
        , table_lock_holder(table_lock_holder_) {}

    bool execute() override;

private:

    void prepare();
    bool executeImpl();
    void finish();

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH
    };

    State state{State::NEED_PREPARE};

    StorageMergeTree & storage;

    StorageMetadataPtr metadata_snapshot;
    bool deduplicate;
    const Names & deduplicate_by_columns;
    std::shared_ptr<MergeMutateSelectedEntry> merge_mutate_entry{nullptr};

    TableLockHolder & table_lock_holder;

    FutureMergedMutatedPartPtr future_part{nullptr};
    MergeTreeData::MutableDataPartPtr new_part;
    std::unique_ptr<Stopwatch> stopwatch_ptr{nullptr};
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
