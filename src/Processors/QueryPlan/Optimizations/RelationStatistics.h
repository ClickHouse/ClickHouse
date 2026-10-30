#pragma once

#include <optional>
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
    std::unordered_map<String, ColumnStats> column_stats = {};

    String table_name;
    bool imprecise_estimate = false;
    /// True only when `estimated_rows` is the exact number of rows produced by this node.
    /// This is independent of `imprecise_estimate`, which describes the estimator path.
    bool rows_exact = false;

    /// `table_name` is a chain of sub-joins that already shows their estimates, so `EXPLAIN` renders
    /// the chain on its own instead of appending one more estimate to it.
    bool composite = false;

    /// Diagnostic annotation of where `estimated_rows` came from; see `RowEstimateSource`.
    /// `NoSource` means the producer of the estimate did not track it; set it wherever it is known.
    RowEstimateSource source = RowEstimateSource::NoSource;
};

namespace QueryPlanOptimizations
{

/// Propagate per-column statistics through `actions`, rekeying the map in place by output name.
/// An output inherits an input's stats when it is that input, an alias of it, or a deterministic
/// single-argument function of it (which cannot increase the distinct count).
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);

/// Record a transformation on both independent facts carried by every column statistic.
void addTransformation(std::unordered_map<String, ColumnStats> & column_stats, ColumnStatsTransformation transformation);

/// A distinct-count sketch was measured on exactly the rows the relation will produce, or a test-only
/// synthetic override explicitly supplies that planner contract. "Exact" refers to row coverage,
/// not to the estimate's numerical error.
bool isExactDistinctCount(const ColumnStatsProvenance & provenance);
/// The distinct count was measured on a superset of the rows the relation will produce or derived from an
/// exact row count, or a test-only synthetic override explicitly supplies that planner contract. As above,
/// "upper bound" refers to row coverage: rows dropped after the measurement cannot make the value too small.
/// It is not a proven numerical bound. A sketch can underestimate the distinct count of the rows it covers
/// (e.g. `uniq_v2` above 256 distinct values), and NULLs are not counted.
bool isDistinctCountUpperBound(const ColumnStatsProvenance & provenance);

/// The following predicates also govern `ColumnStats::null_fraction`: it is a value fact for the
/// same rows as the range and consumers must require the corresponding range guarantee.
/// The range is exactly the produced rows' range.
bool isExactValueRange(const ColumnStatsProvenance & provenance);
/// The range contains the produced rows' range.
bool isValueRangeSuperset(const ColumnStatsProvenance & provenance);
/// Values are plausibly spread over the range, as required by uniform-distribution consumers.
/// A filtered relation is deliberately not representative: interpolating uniformly over an
/// unfiltered whole-part range can misestimate a predicate that cuts that range.
bool isRepresentativeValueRange(const ColumnStatsProvenance & provenance);

/// GROUP BY preserves the distinct set and value range of a direct grouping key. When the input
/// row count reduces the NDV, record whether that clamp came from an exact count or an estimate.
/// The input NULL fraction is not preserved because all NULL keys collapse into one output group.
inline ColumnStats makeGroupingKeyStats(
    const ColumnStats & input,
    std::optional<UInt64> estimated_input_rows,
    bool rows_exact)
{
    ColumnStats result = input;
    result.null_fraction.reset();
    if (estimated_input_rows && result.num_distinct_values > *estimated_input_rows)
    {
        result.num_distinct_values = *estimated_input_rows;
        result.ndv_provenance.add(rows_exact ? ExactRowCountClamp : EstimatedRowCountClamp);
    }
    return result;
}

/// Tighten equi-join key NDVs to their minimum, respecting which side each join kind preserves.
/// Anti joins and full joins leave both inputs unchanged.
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
