#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of a limit aggregation operator,
/// i.e. one of these: "topk", "bottomk", "limitk".
bool isLimitAggregationOperator(std::string_view operator_name);

/// Applies a limit aggregation operator ("topk", "bottomk", or "limitk").
SQLQueryPiece applyLimitAggregationOperator(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
