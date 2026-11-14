#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{

/// Checks if a function name is double_exponential_smoothing.
bool isDoubleExponentialSmoothing(std::string_view function_name);

/// Applies the double_exponential_smoothing function to its arguments:
/// double_exponential_smoothing(range_vector, smoothing_factor, trend_factor).
SQLQueryPiece applyDoubleExponentialSmoothing(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context);

}
