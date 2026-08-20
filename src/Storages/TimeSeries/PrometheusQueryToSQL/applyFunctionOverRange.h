#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

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

/// Returns the fixed @ modifier directly applied to a range-vector argument, if any. Range-vector pieces keep that
/// node after setEvaluationTime(), so the fixed evaluation time is resolvable without hidden state in SQLQueryPiece.
const PrometheusQueryTree::Offset * getFixedAtModifier(const SQLQueryPiece & argument);

/// Returns the grid a range function is aggregated on: its own evaluation range, or - when the range-vector argument
/// carries a fixed @ modifier - the single point (step 0) at the fixed timestamp, where PromQL freezes the window.
NodeEvaluationRange getRangeAggregationRange(
    const PrometheusQueryTree::Offset * fixed_at_node, const NodeEvaluationRange & node_range, ConverterContext & context);

/// Repeats the single value aggregated on a fixed @ grid over the `result_grid_size` points of the outer query grid.
ASTPtr repeatFixedAtResultOverGrid(
    ASTPtr && aggregate_values, const NodeEvaluationRange & aggregation_range, size_t result_grid_size);

}
