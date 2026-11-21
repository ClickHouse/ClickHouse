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
/// The facts stay as measured: a transformation records itself in the provenance rather than erasing the fact.
/// Consumers ask for the guarantee they need through `ColumnStats::distinctCount` or `ColumnStats::valueRange`, which give
/// the fact only if no recorded transformation can have broken that guarantee. A step whose effect is not modeled records
/// `Unmodeled`, which breaks every guarantee, so estimation fails closed.
///
/// The guarantees each transformation keeps:
///
///                                 distinct count             value range
///                                 Exact   UpperBound         Exact   Superset   Representative
///     RowSubset                   no      yes                no      yes        no
///     FilterOnOtherColumns        no      yes                no      yes        yes (assumes independent columns)
///     EstimatedRowCountClamp      no      no                 (recorded only for the distinct count)
///     NDVBoundExpression          no      yes                no      no         no
///     PartialStatisticsCoverage   no      no                 no      no         no
///     Unmodeled                   no      no                 no      no         no
///
/// Every guarantee also requires the origin `MeasuredStatistics` or `SyntheticOverride`.
///
/// A flag records only what happened. `FilterOnOtherColumns` says that a predicate dropped rows without reading the
/// column; that the column then keeps a representative range is an assumption of the `Representative` guarantee: that
/// the column is statistically independent of the columns the predicate reads.

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
    /// A fact from test-only statistics hints or randomization, trusted as if measured: hints stand in for the measured
    /// statistics of tables too small to plan as the ones they describe.
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
    /// The statistics cover only some of the rows they describe, e.g. when some parts lack the statistic, or when the
    /// bounds miss the values of some rows (data lake files with a NULL count but without bounds).
    PartialStatisticsCoverage = 1 << 3,
    /// A step whose effect on the column is not modeled, such as `FINAL` or a join.
    Unmodeled = 1 << 4,
    /// Rows were dropped by a predicate that does not read the column; the values are unchanged. Recorded instead of
    /// `RowSubset` only where the columns a predicate reads are known (the estimate of a MergeTree read). A predicate
    /// counts as reading a column if it reads the stored column; correlation between distinct stored columns is not
    /// seen, e.g. a `MATERIALIZED` column computed from another one counts as a column of its own.
    FilterOnOtherColumns = 1 << 5,
};

struct ColumnStatsProvenance
{
    ColumnStatsOrigin origin{};
    UInt16 transformations = 0;

    void add(ColumnStatsTransformation transformation) { transformations |= transformation; }
    [[nodiscard]] bool has(ColumnStatsTransformation transformation) const { return transformations & transformation; }
    [[nodiscard]] bool hasOnly(UInt16 allowed) const { return (transformations & ~allowed) == 0; }

    /// Stable spelling used by optimizer trace diagnostics and tests.
    [[nodiscard]] String toString() const;
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
    /// The values of the produced rows are plausibly spread over the range, as uniform-distribution consumers need,
    /// and the NULL fraction is plausibly theirs. An exact range is representative. A range of a column that rows were
    /// dropped from by a predicate that reads the column is not: interpolating uniformly over the unfiltered range can
    /// misestimate a predicate that cuts that range. A range of a column that rows were dropped from only by predicates
    /// that do not read it (`FilterOnOtherColumns`) is representative under the assumption that the column is
    /// statistically independent of the columns those predicates read, which this guarantee makes. Consumers name the
    /// guarantee they need, so the two may diverge further (e.g. for a uniform sample).
    Representative,
};
}
