#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks if the specified string is the name of the `double_exponential_smoothing` function.
inline bool isFunctionDoubleExponentialSmoothing(std::string_view function_name) { return function_name == "double_exponential_smoothing"; }

/// Applies the `double_exponential_smoothing(v range-vector, sf scalar, tf scalar)` function: for each series it
/// returns the last smoothed value of the samples in the range. The metric name is dropped.
SQLQueryPiece applyFunctionDoubleExponentialSmoothing(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
