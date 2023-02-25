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
    PartitionPruner(const StorageMetadataPtr & metadata, const SelectQueryInfo & query_info, ContextPtr context, bool strict);

    bool canBePruned(const IMergeTreeDataPart & part);

    bool isUseless() const { return useless; }

    const KeyCondition & getKeyCondition() const { return partition_condition; }

private:
    std::unordered_map<String, bool> partition_filter_map;

    /// partition_key is adjusted here (with substitution from modulo to moduloLegacy).
    KeyDescription partition_key;

    KeyCondition partition_condition;

    bool useless = false;
};

}
