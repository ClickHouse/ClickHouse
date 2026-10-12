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

}
