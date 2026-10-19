#pragma once

#include <Columns/IColumn.h>
#include <Core/Types.h>
#include <DataTypes/IDataType.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <memory>
#include <vector>

namespace DB
{

/// The distinct partitions of a set of data parts, with their partition values sorted by value.
/// Immutable. It is shared between consecutive parts snapshots for as long as the set of partition IDs is unchanged.
struct PartitionSet
{
    /// Partition IDs in ascending order, which is the order of the parts in a parts snapshot.
    std::vector<String> ids;

    /// Partition key types the values are built for (after `MergeTreePartition::adjustPartitionKey`).
    DataTypes types;

    /// Positions in `ids` ordered by the partition value: lexicographically by the partition key columns, NULLs last.
    IColumn::Permutation value_order;

    /// One column per partition key column, with `LowCardinality` removed. Row `i` holds the value of the
    /// partition `ids[value_order[i]]`.
    Columns sorted_values;
};

using PartitionSetPtr = std::shared_ptr<const PartitionSet>;

/// Maps the partitions of one parts snapshot to the ranges of parts they consist of. It allows selecting the parts
/// of the partitions that can match a condition on the partition key without looking at the other parts.
///
/// It relies on the parts snapshot being sorted by `MergeTreePartInfo`, so that the parts of each partition are
/// contiguous and the partitions are in the order of their IDs.
struct PartitionCatalog
{
    /// The parts snapshot the catalog is built for. The reference is weak, so that the catalog does not keep the
    /// parts of an old snapshot alive and does not delay the removal of outdated parts.
    std::weak_ptr<const RangesInDataParts> source;
    const RangesInDataParts * source_raw = nullptr;

    PartitionSetPtr partition_set;

    /// Aligned with `partition_set->ids`: the range [first, second) of positions of the partition's parts in the snapshot.
    std::vector<std::pair<size_t, size_t>> part_ranges;

    /// The number of parts and granules in the snapshot.
    size_t total_parts = 0;
    size_t total_granules = 0;

    /// Whether the catalog is built for this exact parts snapshot object. If the source is still alive, no other
    /// object can have the same address, so the comparison of addresses is sound.
    bool isFor(const RangesInDataParts & parts) const { return source_raw == &parts && !source.expired(); }

    /// Whether the partition values of these types can be ordered in a way consistent with `KeyCondition`.
    /// Floating-point types are not supported because of NaN, as well as compound types.
    static bool canBuildForTypes(const DataTypes & types);

    /// Builds the catalog for the parts snapshot. If the set of partitions is the same as in `previous`, its
    /// partition set is reused. Otherwise, the new partition set is obtained by merging the new partitions
    /// into the previous one, so that only the new partitions are sorted.
    static std::shared_ptr<const PartitionCatalog> build(
        const RangesInDataPartsPtr & parts, const DataTypes & types, const PartitionSetPtr & previous);
};

using PartitionCatalogPtr = std::shared_ptr<const PartitionCatalog>;

}
