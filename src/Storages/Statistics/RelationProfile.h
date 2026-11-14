#pragma once

#include <Core/Field.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <base/types.h>

#include <optional>
#include <unordered_map>

namespace DB
{

struct ColumnStats
{
    UInt64 num_distinct_values = 0;
    /// `num_distinct_values` is estimated from the metadata of the data (e.g. the compressed size of the column)
    /// rather than measured by statistics, see `estimate_distinct_values_from_metadata`. It approximates the
    /// effective number of distinct values, which can be far below the actual one for a skewed column.
    bool num_distinct_values_from_metadata = false;
    /// Average uncompressed size of one value; 0 means unknown.
    Float64 avg_bytes = 0;
    /// Value range from `basic`/`minmax` statistics; unset when unknown.
    std::optional<Field> min_value = {};
    std::optional<Field> max_value = {};
    /// Fraction of NULL values; unset when unknown.
    std::optional<Float64> null_fraction = {};
};

struct RelationProfile
{
    UInt64 rows = 0;
    UnorderedMapWithMemoryTracking<String, ColumnStats> column_stats = {};
};

}
