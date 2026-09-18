#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

namespace DB::PrometheusQueryToSQL
{

/// Checks if a function name is histogram_fraction.
bool isHistogramFraction(std::string_view function_name);

/// Applies the histogram_fraction function to its arguments.
SQLQueryPiece applyHistogramFraction(
    const PrometheusQueryTree::Function * function_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context);

}
