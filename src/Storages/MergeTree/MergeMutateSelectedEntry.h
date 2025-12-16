#pragma once

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
    bool finalized{false};
    MergeMutateSelectedEntry(FutureMergedMutatedPartPtr future_part_, CurrentlyMergingPartsTaggerPtr tagger_,
                             MutationCommandsConstPtr commands_, const MergeTreeTransactionPtr & txn_ = NO_TRANSACTION_PTR)
        : future_part(future_part_)
        , tagger(std::move(tagger_))
        , commands(commands_)
        , txn(txn_)
    {}

    void finalize();
    ~MergeMutateSelectedEntry();
};

using MergeMutateSelectedEntryPtr = std::shared_ptr<MergeMutateSelectedEntry>;

}
