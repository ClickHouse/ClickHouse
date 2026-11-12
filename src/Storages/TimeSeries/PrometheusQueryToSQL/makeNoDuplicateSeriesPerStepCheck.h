#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// Helpers for queries which group time series by a new group (for example, after removing the metric name)
/// and so can get multiple series with the same group.
/// Prometheus evaluates each step independently, so two series with the same tags are allowed
/// if they don't have values at the same step, and the result contains the values of both.

/// Returns the condition `timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), group) = 0`
/// for a HAVING clause. It throws an exception if two series with the same group have values at the same step.
/// `values` must refer to the source column, not to the alias of the merged values.
ASTPtr makeNoDuplicateSeriesPerStepCheck(ASTPtr values, ASTPtr group);

/// Returns `anyForEach(values)` which merges the values of the series with the same group step by step.
/// It must be used together with the condition from `makeNoDuplicateSeriesPerStepCheck` which guarantees
/// that at most one of those series has a value at each step.
ASTPtr makeNoDuplicateSeriesPerStepValues(ASTPtr values);

/// Returns the condition `timeSeriesThrowDuplicateSeriesIf(arrayExists((l, r) -> greatest(l, r) >= 2 AND least(l, r) >= 1,
/// left_side_value_counts, right_side_value_counts), group) = 0`.
/// It throws an exception if at some step the matched series of the left and the right sides give more than one pair,
/// i.e. one side has two or more series with values at that step and the other side has at least one.
/// `value_counts` of a side are the counts of its series with a value at each step.
ASTPtr makeNoDuplicateSeriesPerStepCheckMatchingCounts(ASTPtr left_side_value_counts, ASTPtr right_side_value_counts, ASTPtr group);

/// The same for the case when a side has one series per row, so its values are used instead of the counts:
/// `timeSeriesThrowDuplicateSeriesIf(arrayExists((v, c) -> isNotNull(v) AND c >= 2, values, other_side_value_counts), group) = 0`.
ASTPtr makeNoDuplicateSeriesPerStepCheckMatchingValuesAndCounts(ASTPtr values, ASTPtr other_side_value_counts, ASTPtr group);

}
