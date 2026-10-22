#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionStepRange(std::string_view function_name)
{
    return function_name == "step" || function_name == "range";
}

/// Returns a scalar from the outer query's evaluation settings.
SQLQueryPiece fromFunctionStepRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
