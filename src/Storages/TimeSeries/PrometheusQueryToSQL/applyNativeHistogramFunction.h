#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{

/// Checks if a function name is a native-histogram function (histogram_count or histogram_sum).
bool isNativeHistogramFunction(std::string_view function_name);

/// Applies a native-histogram function to its arguments (see isNativeHistogramFunction).
SQLQueryPiece applyNativeHistogramFunction(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context);

}
