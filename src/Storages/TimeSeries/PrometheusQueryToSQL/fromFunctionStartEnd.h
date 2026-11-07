#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionStartEnd(std::string_view function_name)
{
    return function_name == "start" || function_name == "end";
}

/// Returns a scalar from the outer query's evaluation settings.
SQLQueryPiece fromFunctionStartEnd(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
