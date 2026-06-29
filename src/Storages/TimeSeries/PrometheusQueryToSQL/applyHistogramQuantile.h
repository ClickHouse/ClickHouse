#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{

/// Checks if a function name is histogram_quantile.
bool isHistogramQuantile(std::string_view function_name);

/// Applies the histogram_quantile function to its arguments.
SQLQueryPiece applyHistogramQuantile(
    const PQT::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context);

}
