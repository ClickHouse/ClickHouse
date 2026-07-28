#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of the PromQL function timestamp().
bool isFunctionTimestamp(std::string_view function_name);

/// Applies the PromQL function timestamp(): returns the timestamp (in seconds since epoch) of the sample
/// selected for its argument at each point of the query's grid.
///
/// Only a narrow set of argument shapes is supported: a bare instant selector, optionally wrapped in a unary
/// +/-, a binary operation against a scalar literal, and/or a nested timestamp() call - any combination of
/// these that bottoms out at a bare instant selector. These are exactly the shapes for which the argument's
/// samples keep the identity (and so the timestamp) of the underlying selector's samples; anything else (e.g.
/// binary operations of two vectors, aggregations, other functions) throws NOT_IMPLEMENTED, same as before this
/// function existed.
SQLQueryPiece applyFunctionTimestamp(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
