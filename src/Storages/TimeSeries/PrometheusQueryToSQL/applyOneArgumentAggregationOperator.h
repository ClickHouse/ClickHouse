#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Builds the aggregate expression of a one-argument aggregation operator over the `values` column of a vector grid.
using OneArgumentAggregationTransform = ASTPtr (*)(ASTPtr && values);

/// Returns whether a specified string is the name of a one-argument aggregation operator,
/// i.e. one of these: "sum", "min", "max", "avg", "count", "stddev", "stdvar", "group".
bool isOneArgumentAggregationOperator(std::string_view operator_name);

/// Builds the aggregate expression of a one-argument aggregation operator over the `values` column of its argument.
ASTPtr makeOneArgumentAggregationAST(const PrometheusQueryTree::AggregationOperator * operator_node, const SQLQueryPiece & argument);

/// Applies a one-argument aggregation operator.
SQLQueryPiece applyOneArgumentAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
