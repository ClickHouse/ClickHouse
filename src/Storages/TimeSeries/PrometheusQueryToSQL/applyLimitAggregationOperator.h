#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of a limit aggregation operator,
/// i.e. one of these: "topk", "bottomk", "limitk", "limit_ratio".
bool isLimitAggregationOperator(std::string_view operator_name);

/// Applies a limit aggregation operator ("topk", "bottomk", "limitk", or "limit_ratio").
SQLQueryPiece applyLimitAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
