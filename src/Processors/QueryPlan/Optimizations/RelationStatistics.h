#pragma once

#include <algorithm>
#include <optional>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <unordered_map>

#include <Core/Joins.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <base/types.h>

namespace DB
{

class ActionsDAG;

struct RelationStats
{
    std::optional<UInt64> estimated_rows = {};
    std::optional<Float64> avg_row_bytes = {};
    UnorderedMapWithMemoryTracking<String, ColumnStats> column_stats = {};

    String table_name;
    bool imprecise_estimate = false;

    /// `table_name` is a chain of sub-joins that already shows their estimates, so `EXPLAIN` renders
    /// the chain on its own instead of appending one more estimate to it.
    bool composite = false;

    /// Diagnostic annotation of where `estimated_rows` came from; see `RowEstimateSource`.
    /// `NoSource` means the producer of the estimate did not track it; set it wherever it is known.
    RowEstimateSource source = RowEstimateSource::NoSource;
};

/// The largest numbers of distinct values of the key columns of an equality or an equivalence class.
struct KeyDistinctValues
{
    UInt64 max_ndv = 0;
    UInt64 max_ndv_upper_bound = 0;
    /// The largest row count of a key without a known number of distinct values.
    UInt64 max_rows_without_ndv = 0;
    bool has_ndv_from_metadata = false;

    void add(std::optional<UInt64> ndv, UInt64 upper_bound, bool from_metadata)
    {
        max_ndv = std::max(max_ndv, ndv.value_or(0));
        max_ndv_upper_bound = std::max(max_ndv_upper_bound, upper_bound);
        if (!ndv)
            max_rows_without_ndv = std::max(max_rows_without_ndv, upper_bound);
        has_ndv_from_metadata |= ndv && from_metadata;
    }

    /// The number of distinct values that the equality is estimated with, 0 if unknown.
    /// A number of distinct values from metadata can be far below the actual one: a foreign key with 10000 values
    /// of which a few make up most of the rows gets about as many as those few. The key it is joined with, unique in
    /// its relation of 10000 rows and without a known number of distinct values, would then make the join 100 times
    /// larger than its foreign key side. With the row count of that key as well, the join of a foreign key with its
    /// primary key stays as large as the foreign key side, as when no key has a known number of distinct values.
    UInt64 getNumberOfDistinctValues() const
    {
        if (has_ndv_from_metadata)
            return std::max(max_ndv, max_rows_without_ndv);
        return max_ndv;
    }
};

namespace QueryPlanOptimizations
{

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count).
void remapColumnStats(UnorderedMapWithMemoryTracking<String, ColumnStats> & mapped, const ActionsDAG & actions);

/// Tighten equi-join key NDVs to their minimum, respecting which side each join kind preserves.
/// Anti joins and full joins leave both inputs unchanged. An updated side takes the provenance of the minimum
/// (`num_distinct_values_from_metadata`), so a measured key that gets an estimate from metadata is marked as such.
void updateJoinKeyDistinctCounts(
    ColumnStats & left_stats,
    ColumnStats & right_stats,
    JoinKind kind,
    JoinStrictness strictness);

/// Converts a row or distinct-value estimate to `UInt64`. Estimates can exceed the `UInt64` range (a cross
/// product of large inputs), so the result saturates at the maximum; NaN and non-positive values give 0.
UInt64 toUInt64Saturating(Float64 estimate);

}

}
