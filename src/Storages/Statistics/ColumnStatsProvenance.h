#pragma once

#include <base/types.h>

namespace DB
{

/// Provenance of a column statistic: where it came from and what the plan did to the rows and values it describes on
/// the way to a consumer.
///
/// Every `ColumnStats` carries two facts with independent provenance: the distinct count, and the value range together
/// with the NULL fraction. They are independent because `basic` statistics may exist without a distinct-count sketch,
/// and because transformations affect them differently: a clamp to the row count changes only the distinct count.
///
/// A transformation does not erase a fact. The fact is kept as measured, for diagnostics, and its provenance records
/// the transformation. Consumers that rely on a fact ask for the guarantee they need through `ColumnStats::distinctCount`
/// or `ColumnStats::valueRange`, which give the fact only if no recorded transformation can have broken that guarantee.
/// A step whose effect is not modeled records `Unmodeled`, which breaks every guarantee, so estimation fails closed.
///
/// The guarantees each transformation keeps:
///
///                                 distinct count             value range
///                                 Exact   UpperBound         Exact   Superset   Representative
///     RowSubset                   no      yes                no      yes        no
///     EstimatedRowCountClamp      no      no                 (recorded only for the distinct count)
///     NDVBoundExpression          no      yes                no      no         no
///     PartialStatisticsCoverage   no      no                 no      no         no
///     Unmodeled                   no      no                 no      no         no
///
/// The distinct-count guarantees also require the origin `MeasuredStatistics` or `SyntheticOverride`, the value-range
/// guarantees the origin `MeasuredStatistics`.

/// Source of a column fact before the plan transforms it.
enum class ColumnStatsOrigin : UInt8
{
    /// No statistic: the fact is a default.
    Unknown,
    /// Read from stored statistics: the statistics of MergeTree parts or the column metrics of data lake manifests.
    /// A distinct count is the estimate of an approximate sketch or derived from the metrics (e.g. from the values of
    /// an identity partition).
    MeasuredStatistics,
    /// A heuristic distinct count, guessed in the absence of a distinct-count statistic.
    SyntheticFallback,
    /// A distinct count from test-only statistics hints or randomization, which the planner trusts as if measured.
    SyntheticOverride,
};

/// What happened to a column fact on its way through the plan. These flags are deliberately unordered: consumers ask
/// whether a transformation happened, not when it did.
enum ColumnStatsTransformation : UInt16
{
    /// Rows were dropped (by a filter, a limit, deleted rows, ...) or repeated (by ARRAY JOIN) without changing values.
    /// The remaining values are a subset of the measured ones, possibly with a different distribution.
    RowSubset = 1 << 0,
    /// The distinct count was lowered to an estimated row count, which can be below the true distinct count.
    EstimatedRowCountClamp = 1 << 1,
    /// The column is a function of the measured one that cannot increase the distinct count but can change values
    /// (e.g. `roundToExp2(k)` or a `CAST`), so the measured range and NULL fraction do not describe it.
    NDVBoundExpression = 1 << 2,
    /// The statistics cover only some of the rows they describe, e.g. when some parts lack the statistic.
    PartialStatisticsCoverage = 1 << 3,
    /// A step whose effect on the column is not modeled, such as `FINAL` or a join.
    Unmodeled = 1 << 4,
};

struct ColumnStatsProvenance
{
    ColumnStatsOrigin origin{};
    UInt16 transformations = 0;

    void add(ColumnStatsTransformation transformation) { transformations |= transformation; }
    bool has(ColumnStatsTransformation transformation) const { return transformations & transformation; }
    bool hasOnly(UInt16 allowed) const { return (transformations & ~allowed) == 0; }

    /// Stable spelling used by optimizer trace diagnostics and tests.
    String toString() const;
};

/// What a consumer requires of `ColumnStats::num_distinct_values`. Both guarantees refer to the rows the distinct count
/// was measured on, not to its numerical error: a sketch can underestimate the distinct count of the rows it covers
/// (e.g. `uniq_v2` above 256 distinct values), and NULLs are not counted.
enum class DistinctCountGuarantee : UInt8
{
    /// Measured on exactly the rows the relation produces.
    Exact,
    /// Measured on a superset of the rows the relation produces, so the rows dropped since cannot make it too small.
    UpperBound,
};

/// What a consumer requires of the value range of `ColumnStats` (`min_value`, `max_value`) and its NULL fraction.
enum class ValueRangeGuarantee : UInt8
{
    /// Exactly the range of the produced rows.
    Exact,
    /// Contains the range of the produced rows. The NULL fraction of a subset is unknown, so it comes without one.
    Superset,
    /// The values of the produced rows are plausibly spread over the range, as uniform-distribution consumers need.
    /// A filtered relation is deliberately not representative: interpolating uniformly over an unfiltered range can
    /// misestimate a predicate that cuts that range. Today only an exact range is known to be representative, but the
    /// two may diverge (e.g. for a uniform sample), so consumers name the guarantee they need.
    Representative,
};
}
