#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isBinaryOperatorAnd(std::string_view operator_name) { return operator_name == "and"; }

/// Applies a prometheus operator "and".
SQLQueryPiece applyBinaryOperatorAnd(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

}
