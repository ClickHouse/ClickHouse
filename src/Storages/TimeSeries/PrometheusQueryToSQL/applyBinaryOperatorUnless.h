#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isBinaryOperatorUnless(std::string_view operator_name) { return operator_name == "unless"; }

/// Applies a prometheus operator "unless".
SQLQueryPiece applyBinaryOperatorUnless(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

}
