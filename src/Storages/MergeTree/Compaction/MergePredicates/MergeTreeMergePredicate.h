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

private:
    const StorageMergeTree & storage;
    std::unique_lock<std::mutex> & merge_mutate_lock;
    PatchInfosByPartition patches_by_partition;
    /// Data versions a merge of patch parts must not span: those of the regular parts, plus the future
    /// versions of the merges and mutations already in flight. Filled only if there are patch parts.
    DataVersionsByPartition data_versions_by_partition;
    CommittingBlocksSet committing_blocks;
    std::optional<Int64> min_update_block;
};

using MergeTreeMergePredicatePtr = std::shared_ptr<const MergeTreeMergePredicate>;

}
