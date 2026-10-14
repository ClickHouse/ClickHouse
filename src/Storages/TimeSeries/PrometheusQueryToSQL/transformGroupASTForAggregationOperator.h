#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies the `by(labels)` or `without(labels)` modifier from an aggregation operator to a `group` expression,
/// produces a new group expression with the appropriate tags, and updates `metric_name_dropped` accordingly.
/// If `drop_metric_name` is true, the metric name `__name__` is automatically added to the remove list
/// in the `without(labels)` case even if it is not explicitly listed.
ASTPtr transformGroupASTForAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node,
    ASTPtr && group,
    bool drop_metric_name,
    bool & metric_name_dropped);

/// Returns an aggregate expression which adds the marker of a dropped metric name to `bucket_group`
/// if any series aggregated into that bucket has the marker. The query must group by `bucket_group`.
ASTPtr copyDroppedMetricNameMarkerToBucket(const ASTPtr & bucket_group);

}
