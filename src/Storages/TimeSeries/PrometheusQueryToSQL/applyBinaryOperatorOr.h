#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isBinaryOperatorOr(std::string_view operator_name) { return operator_name == "or"; }

/// Applies a prometheus operator "or".
SQLQueryPiece applyBinaryOperatorOr(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

}
