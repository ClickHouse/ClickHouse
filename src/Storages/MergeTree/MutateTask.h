#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MutationCommands.h>
#include <Interpreters/MutationsInterpreter.h>


namespace DB
{


class MutateTask;
using MutateTaskPtr = std::shared_ptr<MutateTask>;\


class MergeTreeDataMergerMutator;

struct MutationContext;

class MutateTask
{
public:
    MutateTask(
        FutureMergedMutatedPartPtr future_part_,
        StorageMetadataPtr metadata_snapshot_,
        MutationCommandsConstPtr commands_,
        MergeListEntry * mutate_entry_,
        time_t time_of_mutation_,
        ContextPtr context_,
        ReservationSharedPtr space_reservation_,
        TableLockHolder & table_lock_holder_,
        const MergeTreeTransactionPtr & txn,
        MergeTreeData & data_,
        MergeTreeDataMergerMutator & mutator_,
        ActionBlocker & merges_blocker_);

    bool execute();

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return promise.get_future();
    }

private:

    bool prepare();

    enum class State
    {
        NEED_PREPARE,
        NEED_EXECUTE
    };

    State state{State::NEED_PREPARE};


    std::promise<MergeTreeData::MutableDataPartPtr> promise;

    std::shared_ptr<MutationContext> ctx;
    ExecutableTaskPtr task;

};

[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MutateTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
