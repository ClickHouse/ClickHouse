#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Throws UNKNOWN_FUNCTION if a node or any of its children calls a function with an unknown name.
void checkFunctionNames(const Node * node);

/// Applies a prometheus function.
SQLQueryPiece applyFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
