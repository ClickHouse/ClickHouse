#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/PartitionActionBlocker.h>
#include <Storages/MutationCommands.h>


namespace DB
{


class MutateTask;
using MutateTaskPtr = std::shared_ptr<MutateTask>;


class MergeTreeDataMergerMutator;

struct MutationContext;

class MutateTask
{
public:
    static constexpr auto TEMP_DIRECTORY_PREFIX = "tmp_mut_";

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
        PartitionActionBlocker & merges_blocker_);

    bool execute();
    void cancel() noexcept;

    void updateProfileEvents() const;

    MergeTreeData::MutableDataPartPtr takeResultPart() { return std::exchange(result_part, nullptr); }

    [[nodiscard]] const HardlinkedFiles & getHardlinkedFiles() const;

private:

    bool prepare();
    void setResultPart(MergeTreeData::MutableDataPartPtr part);

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE
    };

    State state{State::NEED_PREPARE};

    /// The finished part until the owner takes it. `cancel` rolls back a part that is still here.
    MergeTreeData::MutableDataPartPtr result_part;

    std::shared_ptr<MutationContext> ctx;
    ExecutableTaskPtr task;

};

[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MutateTaskPtr task)
{
    try
    {
        while (task->execute()) {}
    }
    catch (...)
    {
        task->cancel();
        throw;
    }
    return task->takeResultPart();
}

}
