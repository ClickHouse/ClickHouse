#pragma once

#include <Core/Field.h>
#include <Storages/Statistics/ColumnStatsProvenance.h>
#include <base/types.h>

#include <optional>
#include <unordered_map>

namespace DB
{

/// The value range and NULL fraction of a column, see `ColumnStats::valueRange`.
struct ColumnValueRange
{
    Field min;
    Field max;
    /// Unset when unknown.
    std::optional<Float64> null_fraction;
};

/// Statistics of one column of a relation. The facts are kept as measured, and their provenance records how the plan
/// transformed them, see `ColumnStatsProvenance`. Consumers that rely on a fact rather than use it as an estimate read
/// it through `distinctCount` or `valueRange`.
struct ColumnStats
{
    /// Estimated number of distinct non-NULL values.
    UInt64 num_distinct_values = 0;
    ColumnStatsProvenance ndv_provenance = {};
    /// Average uncompressed size of one value; 0 means unknown.
    Float64 avg_bytes = 0;
    /// Value range from `basic`/`minmax` statistics and the fraction of NULL values; unset when unknown.
    std::optional<Field> min_value = {};
    std::optional<Field> max_value = {};
    std::optional<Float64> null_fraction = {};
    ColumnStatsProvenance range_provenance = {};

    /// Records `transformation` for both facts.
    void addTransformation(ColumnStatsTransformation transformation);

    /// `num_distinct_values` if it gives `guarantee`.
    std::optional<UInt64> distinctCount(DistinctCountGuarantee guarantee) const;
    /// The value range and NULL fraction if they give `guarantee`.
    std::optional<ColumnValueRange> valueRange(ValueRangeGuarantee guarantee) const;
};

struct RelationProfile
{
    UInt64 rows = 0;
    std::unordered_map<String, ColumnStats> column_stats = {};
};

}
