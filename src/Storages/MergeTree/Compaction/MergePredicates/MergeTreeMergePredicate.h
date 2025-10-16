#pragma once

#include <Storages/MergeTree/Compaction/MergePredicates/IMergePredicate.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/StorageMergeTree.h>

namespace DB
{

class MergeTreeMergePredicate final : public IMergePredicate
{
public:
    explicit MergeTreeMergePredicate(const StorageMergeTree & storage_, std::unique_lock<std::mutex> & merge_mutate_lock_);
    ~MergeTreeMergePredicate() override = default;

    std::expected<void, PreformattedMessage> canMergeParts(const PartProperties & left, const PartProperties & right) const override;
    std::expected<void, PreformattedMessage> canUsePartInMerges(const MergeTreeDataPartPtr & part) const;

private:
    const StorageMergeTree & storage;
    std::unique_lock<std::mutex> & merge_mutate_lock;
};

using MergeTreeMergePredicatePtr = std::shared_ptr<const MergeTreeMergePredicate>;

}
