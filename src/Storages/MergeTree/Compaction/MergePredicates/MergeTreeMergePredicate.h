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
    explicit MergeTreeMergePredicate(
        const StorageMergeTree & storage_, const MergeTreeTransactionPtr & tx_, std::unique_lock<std::mutex> & merge_mutate_lock_);
    ~MergeTreeMergePredicate() override = default;

    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const override;
    std::expected<void, PreformattedMessage> canUsePartInMerges(const MergeTreeDataPartPtr & part) const;
    PartsRange getPatchesToApplyOnMerge(const PartsRange & range) const override;

    /// The lowest data version above `data_version` of a patch part of `partition_id` that `applied`
    /// does not contain, or `nullopt`. The merged part must record a version below it: at or above,
    /// the patch would count as applied to rows it never reached.
    std::optional<Int64> firstPatchNotAppliedAfter(
        const String & partition_id, Int64 data_version, const MergeTreeData::DataPartsVector & applied) const;

    /// The block numbers allocated but not visible yet, and the last allocated one, as of construction.
    const CommittingBlocksSnapshot & getReservations() const { return reservations; }

private:
    const StorageMergeTree & storage;
    std::unique_lock<std::mutex> & merge_mutate_lock;
    PatchInfosByPartition patches_by_partition;
    /// Data versions of the regular parts. Filled only if there are patch parts in the table.
    /// Used to check that a merge of patch parts does not span the data version of an existing part.
    DataVersionsByPartition data_versions_by_partition;
    /// Taken before the parts are read, so a part allocated after it is above `lastAllocatedBlock`.
    CommittingBlocksSnapshot reservations;
};

using MergeTreeMergePredicatePtr = std::shared_ptr<const MergeTreeMergePredicate>;

}
