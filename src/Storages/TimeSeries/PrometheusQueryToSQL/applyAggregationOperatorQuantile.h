#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of the `quantile` aggregation operator.
inline bool isAggregationOperatorQuantile(std::string_view operator_name) { return operator_name == "quantile"; }

/// Applies the `quantile` aggregation operator.
SQLQueryPiece applyAggregationOperatorQuantile(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
