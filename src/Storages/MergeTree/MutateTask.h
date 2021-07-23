#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MutationCommands.h>


namespace DB
{


class MutateTask;
using MutateTaskPtr = std::shared_ptr<MutateTask>;\


class MergeTreeDataMergerMutator;


class MutateTask
{
public:
    MutateTask
    (
        FutureMergedMutatedPartPtr future_part_,
        StorageMetadataPtr metadata_snapshot_,
        MutationCommands commands_,
        MergeListEntry & mutate_entry_,
        time_t time_of_mutation_,
        ContextPtr context_,
        ReservationSharedPtr space_reservation_,
        TableLockHolder & table_lock_holder_,
        MergeTreeData & data_,
        MergeTreeDataMergerMutator & mutator_,
        ActionBlocker & merges_blocker_)
        : future_part(future_part_)
        , metadata_snapshot(metadata_snapshot_)
        , commands(commands_)
        , mutate_entry(mutate_entry_)
        , time_of_mutation(time_of_mutation_)
        , context(context_)
        , space_reservation(space_reservation_)
        , holder(table_lock_holder_)
        , data(data_)
        , mutator(mutator_)
        , merges_blocker(merges_blocker_)
        {}

    bool execute();

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return promise.get_future();
    }

private:

    MergeTreeData::MutableDataPartPtr main();

    /// Override all columns of new part using mutating_stream
    void mutateAllPartColumns(
        MergeTreeData::MutableDataPartPtr new_data_part,
        // const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeIndices & skip_indices,
        const MergeTreeProjections & projections_to_build,
        BlockInputStreamPtr mutating_stream,
        // time_t time_of_mutation,
        const CompressionCodecPtr & compression_codec,
        MergeListEntry & merge_entry,
        bool need_remove_expired_values,
        bool need_sync
        // ReservationSharedPtr space_reservation,
        // TableLockHolder & holder,
        // ContextPtr context
        );

    /// Mutate some columns of source part with mutation_stream
    void mutateSomePartColumns(
        const MergeTreeDataPartPtr & source_part,
        // const StorageMetadataPtr & metadata_snapshot,
        const std::set<MergeTreeIndexPtr> & indices_to_recalc,
        const std::set<MergeTreeProjectionPtr> & projections_to_recalc,
        const Block & mutation_header,
        MergeTreeData::MutableDataPartPtr new_data_part,
        BlockInputStreamPtr mutating_stream,
        // time_t time_of_mutation,
        const CompressionCodecPtr & compression_codec,
        MergeListEntry & merge_entry,
        bool need_remove_expired_values,
        bool need_sync
        // ReservationSharedPtr space_reservation,
        // TableLockHolder & holder,
        // ContextPtr context
        );

    void writeWithProjections(
        MergeTreeData::MutableDataPartPtr new_data_part,
        // const StorageMetadataPtr & metadata_snapshot,
        const MergeTreeProjections & projections_to_build,
        BlockInputStreamPtr mutating_stream,
        IMergedBlockOutputStream & out,
        // time_t time_of_mutation,
        MergeListEntry & merge_entry,
        // ReservationSharedPtr space_reservation,
        // TableLockHolder & holder,
        // ContextPtr context,
        IMergeTreeDataPart::MinMaxIndex * minmax_idx = nullptr);


    static bool checkOperationIsNotCanceled(ActionBlocker & merges_blocker, MergeListEntry & mutate_entry);


    /** Split mutation commands into two parts:
    * First part should be executed by mutations interpreter.
    * Other is just simple drop/renames, so they can be executed without interpreter.
    */
    static void splitMutationCommands(
        MergeTreeData::DataPartPtr part,
        const MutationCommands & commands,
        MutationCommands & for_interpreter,
        MutationCommands & for_file_renames);


    std::promise<MergeTreeData::MutableDataPartPtr> promise;

    FutureMergedMutatedPartPtr future_part;
    StorageMetadataPtr metadata_snapshot;
    MutationCommands commands;
    MergeListEntry & mutate_entry;
    time_t time_of_mutation;
    ContextPtr context;
    ReservationSharedPtr space_reservation;
    TableLockHolder & holder;

    Poco::Logger * log{&Poco::Logger::get("MutateTask")};

    MergeTreeData & data;
    MergeTreeDataMergerMutator & mutator;
    ActionBlocker & merges_blocker;
};

[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MutateTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
