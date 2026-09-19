#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies the `by(labels)` or `without(labels)` modifier from an aggregation operator to a `group` expression,
/// produces a new group expression with the appropriate tags, and updates `metric_name_dropped` accordingly.
/// If `drop_metric_name` is true, the metric name `__name__` is automatically added to the remove list
/// in the `without(labels)` case even if it is not explicitly listed.
ASTPtr transformGroupASTForAggregationOperator(
    const PQT::AggregationOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped);

}
