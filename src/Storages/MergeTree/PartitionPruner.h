#pragma once

#include <unordered_map>

#include <Storages/KeyDescription.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{

/// Pruning partitions in verbatim way using KeyCondition
class PartitionPruner
{
public:
    PartitionPruner(
        const StorageMetadataPtr & metadata,
        const ActionsDAGWithInversionPushDown & filter_dag,
        ContextPtr context,
        bool strict = false,
        bool skip_analysis = false);

    /// Pass `can_prune_empty_parts = false` when the answer is used as a statement about the
    /// partition (mutation / lightweight-update block-number scoping) rather than about the
    /// part: an empty part's partition key still has to be matched against the predicate.
    bool canBePruned(const IMergeTreeDataPart & part, bool can_prune_empty_parts = true) const;

    bool isUseless() const { return useless; }

    const KeyCondition & getKeyCondition() const { return partition_condition; }

private:
    /// Cache already analyzed partitions.
    mutable std::unordered_map<String, bool> partition_filter_map;

    /// partition_key is adjusted here (with substitution from modulo to moduloLegacy).
    KeyDescription partition_key;

    KeyCondition partition_condition;

    bool useless = false;
};

}
