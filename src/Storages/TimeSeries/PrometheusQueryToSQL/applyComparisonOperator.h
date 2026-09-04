#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether a specified string is the name of a prometheus comparison operator:
/// '==', '!=', '>', '<', '>=', '<='
bool isComparisonOperator(std::string_view operator_name);

/// Applies a prometheus comparison operator.
SQLQueryPiece applyComparisonOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

}
