#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Whether all the series of the expression `node` have the same metric name: the selector it reads has an equality matcher
/// on `__name__` and nothing between the selector and `node` changes labels (element-wise functions, unary operators,
/// operators with a scalar). Dropping the metric name from such series can't make two of them equal.
bool hasSingleMetricName(const PrometheusQueryTree::Node * node);

/// Emits the SQL for a deferred drop of the metric name, see `SQLQueryPiece::metric_name_drop_pending`.
/// Returns `query_piece` unchanged if no drop is pending.
/// Must be called by every consumer which reads `group` of a VECTOR_GRID and relies on its uniqueness (toVectorGrid() does it).
SQLQueryPiece resolveMetricNameDrop(SQLQueryPiece && query_piece, ConverterContext & context);

/// Returns the `group` AST to pass to transformGroupASTForAggregationOperator() for an aggregation over `argument`
/// which drops the metric name itself (`drop_metric_name = true`).
/// If a drop is pending and all the series of `argument` have the same metric name, the drop is folded into the aggregation key
/// and `argument` is marked as resolved; otherwise the caller's toVectorGrid() resolves the pending drop with the duplicate check.
ASTPtr foldMetricNameDropIntoAggregation(const PrometheusQueryTree::AggregationOperator * operator_node, SQLQueryPiece & argument);

}
