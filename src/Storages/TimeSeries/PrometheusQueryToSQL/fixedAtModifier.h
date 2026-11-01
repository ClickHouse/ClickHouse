#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/NodeEvaluationRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// A range function whose range-vector argument carries a fixed @ modifier, e.g. `rate(v[5m] @ 1600000000)`, is
/// step-invariant: PromQL evaluates it once for the window frozen at the fixed timestamp and repeats the result at
/// every step of the outer query.
/// For example, `rate(v[5m] @ 1600000000)[1h:10m]` evaluated at 1700003600 is a
/// subquery with six steps at 1700000600, 1700001200, ..., 1700003600, and each of them gets the rate calculated over
/// the same window (1599999700, 1600000000], not over a window ending at the step's own timestamp.
/// The functions below implement that for the translators of the range functions. The exception is `predict_linear`:
/// its result depends on the evaluation time, so PromQL evaluates it at every step even with a fixed @ (see
/// AtModifierUnsafeFunctions in Prometheus); its translator uses the frozen window but shifts the prediction per step.

/// Returns the fixed @ modifier directly applied to a range-vector argument, or nullptr if the argument is not a range
/// vector or has no fixed @ modifier. This relies on `applyOffset` keeping the `Offset` node as the node of a range-vector piece.
const PrometheusQueryTree::Offset * getFixedAtModifier(const SQLQueryPiece & argument);

/// Returns the grid a range function is aggregated on: its own evaluation range, or - when the range-vector argument
/// carries a fixed @ modifier - the single point (step 0) at the fixed timestamp, where PromQL freezes the window.
NodeEvaluationRange getRangeAggregationRange(
    const PrometheusQueryTree::Offset * fixed_at_node, const NodeEvaluationRange & node_range, ConverterContext & context);

/// Repeats the single value aggregated on a fixed @ grid over the `result_grid_size` points of the outer query grid.
ASTPtr repeatFixedAtResultOverGrid(
    ASTPtr && aggregate_values, const NodeEvaluationRange & aggregation_range, size_t result_grid_size);

}
