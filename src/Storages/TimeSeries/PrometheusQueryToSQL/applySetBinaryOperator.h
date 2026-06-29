#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether a specified string is the name of a prometheus set binary operator:
/// `and`, `or`, `unless`.
bool isSetBinaryOperator(std::string_view operator_name);

/// Applies a prometheus set binary operator.
SQLQueryPiece applySetBinaryOperator(
    const PQT::BinaryOperator * operator_node, SQLQueryPiece && left_argument, SQLQueryPiece && right_argument, ConverterContext & context);

}
