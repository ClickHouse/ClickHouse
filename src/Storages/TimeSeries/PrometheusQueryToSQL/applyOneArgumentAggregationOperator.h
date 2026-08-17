#pragma once

#include <DataTypes/IDataType.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Builds the aggregate expression of a one-argument aggregation operator from the `values` column of a vector grid.
using OneArgumentAggregationTransform = ASTPtr (*)(ASTPtr && values, const DataTypePtr & scalar_data_type);

/// Returns whether a specified string is the name of a one-argument aggregation operator,
/// i.e. one of these: "sum", "min", "max", "avg", "count", "stddev", "stdvar", "group".
bool isOneArgumentAggregationOperator(std::string_view operator_name);

/// Returns the aggregate expression builder for a one-argument aggregation operator,
/// or nullptr if `operator_name` is not the name of such an operator.
OneArgumentAggregationTransform getOneArgumentAggregationTransform(std::string_view operator_name);

/// Applies a one-argument aggregation operator.
SQLQueryPiece applyOneArgumentAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
