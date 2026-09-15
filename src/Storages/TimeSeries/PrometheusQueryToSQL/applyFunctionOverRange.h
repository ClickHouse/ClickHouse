#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Parsers/IAST_fwd.h>

#include <vector>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector.
/// Examples: rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

SQLQueryPiece applyFunctionOverRange(
    const Node * node, std::string_view function_name, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// Lowers a range-vector `argument` to the aggregate `ch_function_name` computed over the range's window on the
/// time grid. `extra_aggregate_params` are appended after the (start, end, step, window) parameters of the
/// aggregate function - used by range functions that take extra scalar parameters (for example
/// double_exponential_smoothing, which passes its smoothing and trend factors).
SQLQueryPiece applyAggregateFunctionOverRange(
    const Node * node,
    std::string_view ch_function_name,
    bool drop_metric_name,
    SQLQueryPiece && argument,
    std::vector<ASTPtr> extra_aggregate_params,
    ConverterContext & context);

}
