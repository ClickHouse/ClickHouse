#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether a specified string is the name of a one-argument aggregation operator,
/// i.e. one of these: "sum", "min", "max", "avg", "count", "stddev", "stdvar", "group".
bool isOneArgumentAggregationOperator(std::string_view operator_name);

/// Applies a one-argument aggregation operator.
SQLQueryPiece applyOneArgumentAggregationOperator(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
