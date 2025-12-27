#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Applies an aggregation operator, for example "sum by (instance) (http_errors)".
SQLQueryPiece applyAggregationOperator(
    const PQT::AggregationOperator * operator_node,
    std::vector<SQLQueryPiece> && arguments,
    ConverterContext & context);

}
