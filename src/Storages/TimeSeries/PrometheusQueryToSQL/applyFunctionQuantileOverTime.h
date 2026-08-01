#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks if the specified string is the name of the `quantile_over_time` function.
/// `quantile_over_time` is special among range-vector functions because it takes two arguments:
/// a scalar (the quantile parameter `phi`) and a range vector. It therefore cannot be dispatched
/// through `applyFunctionOverRange` (which handles single-argument range functions).
bool isFunctionQuantileOverTime(std::string_view function_name);

/// Applies the `quantile_over_time(phi scalar, v range-vector)` function.
///
/// It computes the phi-quantile (0 <= phi <= 1) of all float sample values in the specified range
/// for each series. The metric name is dropped.
///
/// Edge cases (matching Prometheus semantics):
///   - phi < 0   -> the result is -Inf for every grid point that has at least one sample in its window
///   - phi > 1   -> the result is +Inf for every grid point that has at least one sample in its window
///   - phi = NaN -> the result is NaN for every grid point that has at least one sample in its window
/// Grid points whose window contains no samples keep NULL (the series is absent at that point).
SQLQueryPiece applyFunctionQuantileOverTime(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
