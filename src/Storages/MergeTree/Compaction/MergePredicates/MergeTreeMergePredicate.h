#pragma once

#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeCommittingBlock.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

class MergeTreeMergePredicate final : public IMergePredicate
{
public:
    /// `reservations_` must be taken before the constructor reads the patch parts; see `getPatchesToApplyOnMerge`.
    MergeTreeMergePredicate(
        const StorageMergeTree & storage_, const MergeTreeTransactionPtr & tx_, std::unique_lock<std::mutex> & merge_mutate_lock_,
        CommittingBlocksSnapshot reservations_);
    ~MergeTreeMergePredicate() override = default;

    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const override;
    std::expected<void, PreformattedMessage> canUsePartInMerges(const MergeTreeDataPartPtr & part) const;
    PartsRange getPatchesToApplyOnMerge(const PartsRange & range) const override;

    /// A part created after the snapshot; a version allocated in between can lie below it.
    bool isPartAfterSnapshot(const MergeTreeDataPartPtr & part) const;

    const CommittingBlocksSnapshot & getReservations() const { return reservations; }

private:
    const StorageMergeTree & storage;
    std::unique_lock<std::mutex> & merge_mutate_lock;
    PatchInfosByPartition patches_by_partition;
    /// Data versions of the regular parts. Filled only if there are patch parts in the table.
    /// Used to check that a merge of patch parts does not span the data version of an existing part.
    DataVersionsByPartition data_versions_by_partition;
    CommittingBlocksSnapshot reservations;
};

using MergeTreeMergePredicatePtr = std::shared_ptr<const MergeTreeMergePredicate>;

}
