#pragma once

#include <Storages/MergeTree/KeyCondition.h>
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/PartitionCatalog.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

/// A condition on the partition key that can be checked on ranges of partition values, unlike the condition of
/// `PartitionPruner`, which is built for single points. It is used with the partition value index.
struct PartitionRangeCondition
{
    KeyCondition condition;

    /// The types of the partition key columns (after `MergeTreePartition::adjustPartitionKey`).
    DataTypes key_types;
};

struct PartitionValueIndexResult
{
    /// The parts of the selected partitions, in the order of the parts snapshot.
    RangesInDataParts parts;
    size_t num_granules = 0;

    size_t num_selected_partitions = 0;
    size_t num_steps = 0;
    bool reached_step_limit = false;
    MarkRanges::SearchAlgorithm search_algorithm = MarkRanges::SearchAlgorithm::Unknown;
};

/// Selects the parts of the partitions for which `condition` can be true, using the partition values of `catalog`
/// sorted by value: with a binary search if the condition is a single continuous range of the partition key, and with
/// a generic exclusion search otherwise. The result is a superset of the parts of the matching partitions: the parts
/// still have to be checked one by one. `catalog` must be built for `parts`. `max_steps` limits the generic exclusion
/// search, 0 means one step per 16 partitions, but at least 64 steps.
PartitionValueIndexResult selectPartsByPartitionValueIndex(
    const RangesInDataParts & parts,
    const PartitionCatalog & catalog,
    const KeyCondition & condition,
    size_t coarse_index_granularity,
    size_t max_steps);

}
