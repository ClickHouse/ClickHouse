#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Whether both sides are one-argument aggregations with the same grouping of the same expression, e.g. `sum(x) - max(x)`.
bool canFuseAggregationBinaryOperator(const PrometheusQueryTree::BinaryOperator * operator_node, const ConverterContext & context);

/// Calculates both aggregations and the operator in one aggregation over `argument`, the converted shared expression.
SQLQueryPiece applyFusedAggregationBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context);

}
