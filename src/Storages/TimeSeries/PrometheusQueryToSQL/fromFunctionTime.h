#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionTime(std::string_view function_name) { return function_name == "time"; }

/// Makes a SQL query to return the current evaluation time (the number of seconds since January 1, 1970 UTC).
SQLQueryPiece fromFunctionTime(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// Makes a query piece returning the evaluation time of the specified node, the same as the function time() would return.
SQLQueryPiece makeTimeQueryPiece(const PrometheusQueryTree::Node * node, ConverterContext & context);

/// Same as makeTimeQueryPiece(), but keeps the evaluation time in `context.timestamp_data_type` (native DateTime64
/// precision) instead of casting it down to `context.scalar_data_type` (which can be Float32). Intended for callers
/// that only need the evaluation time to immediately extract a calendar component from it (e.g. 0-argument date/time
/// functions like hour(), minute()) and must not lose timestamp precision through an intermediate float scalar cast
/// before doing so. Not suitable for time() itself, which legitimately wants a scalar/float value the user can do
/// arithmetic on.
SQLQueryPiece makeTimeQueryPieceNative(const PrometheusQueryTree::Node * node, ConverterContext & context);

/// Returns the `time()` call that `node` is after peeling off any nesting of `scalar(...)`/`vector(...)`/unary `+`/
/// `@`/`offset` wrappers (all value-preserving), or nullptr if `node` isn't such a call.
const PrometheusQueryTree::Function * findTimeCallThroughScalarVectorWrappers(const PrometheusQueryTree::Node * node);

/// Rebuilds a varying (SCALAR_GRID) scalar argument so that a grid of evaluation instants keeps its precision: that
/// grid is otherwise `Array(context.scalar_data_type)`, and Float32 resolves only ~128s at today's epoch magnitude.
SQLQueryPiece makeVaryingScalarPrecisionSafe(
    std::string_view function_name, const PrometheusQueryTree::Node * argument_node, SQLQueryPiece && argument, ConverterContext & context);

}
