#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether a binary operator applies two one-argument aggregation operators with the same grouping
/// to the same expression, for example `sum(x) - max(x)`.
bool canFuseAggregationBinaryOperator(const PrometheusQueryTree::BinaryOperator * operator_node, const ConverterContext & context);

/// Applies a binary operator to two aggregations of the same expression, calculating both aggregations
/// in a single aggregation over `argument` instead of joining them.
/// `argument` must be the converted expression both aggregations are applied to,
/// and canFuseAggregationBinaryOperator() must return true for `operator_node`.
SQLQueryPiece applyFusedAggregationBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context);

}
