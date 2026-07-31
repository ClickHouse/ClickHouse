#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether the specified string is the name of the `count_values` aggregation operator.
inline bool isAggregationOperatorCountValues(std::string_view operator_name)
{
    return operator_name == "count_values";
}

/// Applies the `count_values` aggregation operator.
SQLQueryPiece applyAggregationOperatorCountValues(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
