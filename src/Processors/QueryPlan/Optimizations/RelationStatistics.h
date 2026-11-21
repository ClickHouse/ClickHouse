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
    /// True only when `estimated_rows` is the exact number of rows produced by this node; read it through `exactRows`.
    /// This is independent of `imprecise_estimate`, which describes the estimator path.
    bool rows_exact = false;

    /// `table_name` is a chain of sub-joins that already shows their estimates, so `EXPLAIN` renders
    /// the chain on its own instead of appending one more estimate to it.
    bool composite = false;

    /// Diagnostic annotation of where `estimated_rows` came from; see `RowEstimateSource`.
    /// `NoSource` means the producer of the estimate did not track it; set it wherever it is known.
    RowEstimateSource source = RowEstimateSource::NoSource;

    /// The number of rows produced by this node, if it is known exactly.
    [[nodiscard]] std::optional<UInt64> exactRows() const { return rows_exact ? estimated_rows : std::nullopt; }

    /// Records `transformation` on the statistics of every column.
    void addTransformation(ColumnStatsTransformation transformation)
    {
        for (auto & [_, stats] : column_stats)
            stats.addTransformation(transformation);
    }
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
    [[nodiscard]] UInt64 getNumberOfDistinctValues() const
    {
        if (has_ndv_from_metadata)
            return std::max(max_ndv, max_rows_without_ndv);
        return max_ndv;
    }
};

namespace QueryPlanOptimizations
{

/// Clears `min_value` and `max_value` of `stats` unless they still enclose the values of the produced rows, i.e.
/// unless `valueRange` gives them for `ValueRangeGuarantee::Superset`. The NULL fraction and the provenance stay, for
/// diagnostics.
///
/// Not every reader of a value range consults its provenance: DPsub's interesting-order costing reads `min_value` and
/// `max_value` directly to estimate how much of a sorted probe side a runtime filter prunes. The estimator used to erase
/// value ranges at four sites: value-changing expressions, limits, sorts with a limit and grouping keys. There it keeps
/// them now only while they are enclosing bounds, so such a reader never sees a range the estimator used to clear
/// unless the range still bounds the produced values. Call it after the site has recorded all of its transformations.
void keepOnlyEnclosingValueRange(ColumnStats & stats);
void keepOnlyEnclosingValueRanges(UnorderedMapWithMemoryTracking<String, ColumnStats> & column_stats);

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count). A function that may
/// change values records `NDVBoundExpression` and loses the value range, see `keepOnlyEnclosingValueRange`.
void remapColumnStats(UnorderedMapWithMemoryTracking<String, ColumnStats> & mapped, const ActionsDAG & actions);

/// The statistics of a direct grouping key of GROUP BY over `input_relation`, which preserves its distinct
/// set and value range. The distinct count is clamped to the input row count. The input NULL fraction is not
/// preserved because all NULL keys collapse into one output group.
ColumnStats makeGroupingKeyStats(const ColumnStats & input, const RelationStats & input_relation);

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
