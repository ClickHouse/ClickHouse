#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <Parsers/IAST_fwd.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is the name of a prometheus function taking a range vector.
/// Examples: rate(), idelta(), last_over_time().
bool isFunctionOverRange(std::string_view function_name);

/// Applies a prometheus function taking a range vector.
SQLQueryPiece applyFunctionOverRange(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// `drop_metric_name` overrides the function's own metric-name policy. Internal callers that reuse a
/// translation for a private intermediate (e.g. absent_over_time's presence grid) pass `false`: dropping
/// the name there could only manufacture duplicate label sets, which the public path rejects.
SQLQueryPiece applyFunctionOverRange(
    const Node * node,
    std::string_view function_name,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context,
    std::optional<bool> drop_metric_name = std::nullopt);

/// Computes the aggregate `ch_function_name` over the range's window on the time grid, with `extra_aggregate_arguments`
/// after the samples. `needs_cast_to_float64` casts the result to `Array(Nullable(Float64))`.
SQLQueryPiece applyAggregateFunctionOverRange(
    const Node * node,
    std::string_view ch_function_name,
    bool drop_metric_name,
    bool needs_cast_to_float64,
    SQLQueryPiece && argument,
    ASTs extra_aggregate_arguments,
    ConverterContext & context);

}
