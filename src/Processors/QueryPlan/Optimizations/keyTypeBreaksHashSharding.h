#pragma once

namespace DB
{

class IDataType;

namespace QueryPlanOptimizations
{

/// The shard is picked by the hash of the key's byte representation (`ScatterByPartitionTransform` ->
/// `IColumn::computeHashInto`), while `FullSortingMergeJoin` and `WindowTransform` match keys with
/// `compareAt`. For some types the two disagree - values that compare equal can hash differently - so
/// hash sharding would scatter such values into different shards: a per-shard merge join would lose the
/// match, and a per-shard window would split one logical partition. Known cases:
///   - Floating-point: `-0.0` / `+0.0` (and NaNs) compare equal but have different bit patterns.
///   - `Object('json')` / `JSON` and `Dynamic`: `compareAt` compares the logical value, the hash depends on
///     the physical layout (typed/dynamic subcolumn vs `shared_data`, typed vs shared variant), and that
///     layout can differ between blocks. `Dynamic` keys are rejected earlier by
///     `TableJoin::inferJoinKeyCommonType` unless `allow_dynamic_type_in_join_keys` is enabled.
/// Detected at the top level or nested inside `Nullable`/`LowCardinality`/`Array`/`Tuple`/`Map`/`Variant`.
bool keyTypeBreaksHashSharding(const IDataType & type);

}

}
