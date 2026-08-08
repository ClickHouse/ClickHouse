#pragma once

#include <Common/MemoryTracker.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MutationCommands.h>

namespace DB
{

class StorageMergeTree;

struct StorageInMemoryMetadata;
using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
struct FutureMergedMutatedPart;
using FutureMergedMutatedPartPtr = std::shared_ptr<FutureMergedMutatedPart>;


struct CurrentlyMergingPartsTagger
{
    FutureMergedMutatedPartPtr future_part;
    ReservationSharedPtr reserved_space;
    /// Memory reserved in advance for the merge's input/output IO buffers. Released when the tagger
    /// (and thus the whole selected entry) is destroyed, i.e. when the merge finishes.
    MergeMemoryReservation memory_reservation;
    StorageMergeTree & storage;
    // Optional tagger to maintain volatile parts for the JBOD balancer
    std::optional<CurrentlySubmergingEmergingTagger> tagger;
    bool finalized{false};

    CurrentlyMergingPartsTagger(
        FutureMergedMutatedPartPtr future_part_,
        size_t total_size,
        StorageMergeTree & storage_,
        const StorageMetadataPtr & metadata_snapshot,
        bool is_mutation);

    /// The finalize() method acquires the `currently_processing_in_background_mutex` lock
    /// to remove the parts from the `currently_merging_mutating_parts` set.
    /// This might take a lot of time and it's important not to do it in the destructor.
    void finalize();
    ~CurrentlyMergingPartsTagger();
};

using CurrentlyMergingPartsTaggerPtr = std::unique_ptr<CurrentlyMergingPartsTagger>;

struct MergeMutateSelectedEntry
{
    FutureMergedMutatedPartPtr future_part;
    CurrentlyMergingPartsTaggerPtr tagger;
    MutationCommandsConstPtr commands;
    MergeTreeTransactionPtr txn;
    Strings mutation_ids; /// List of mutation version strings being applied
    /// For a merge entry: the timestamp the merge was selected at. MergePlainMergeTreeTask passes it into
    /// MergeTask as time_of_merge (as the replicated path passes entry.create_time), so the merge evaluates
    /// its TTL boundaries against the same clock the up-front memory reservation was estimated with
    /// (see CompactionStatistics::estimateNeededMemoryForMerge) - a merge that waits in the background
    /// queue while a TTL boundary passes must not turn into a row-reducing TTL merge its reservation did
    /// not price. Unused (0) for a mutation entry.
    time_t time_of_merge{0};
    bool finalized{false};
    MergeMutateSelectedEntry(FutureMergedMutatedPartPtr future_part_, CurrentlyMergingPartsTaggerPtr tagger_,
                             MutationCommandsConstPtr commands_, const MergeTreeTransactionPtr & txn_ = NO_TRANSACTION_PTR,
                             Strings mutation_ids_ = {})
        : future_part(future_part_)
        , tagger(std::move(tagger_))
        , commands(commands_)
        , txn(txn_)
        , mutation_ids(std::move(mutation_ids_))
    {}

    void finalize();
    ~MergeMutateSelectedEntry();
};

using MergeMutateSelectedEntryPtr = std::shared_ptr<MergeMutateSelectedEntry>;

}
