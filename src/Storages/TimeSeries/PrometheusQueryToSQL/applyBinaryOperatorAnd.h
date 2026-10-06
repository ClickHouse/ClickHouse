#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isBinaryOperatorAnd(std::string_view operator_name) { return operator_name == "and"; }

/// Applies a prometheus operator "and".
SQLQueryPiece applyBinaryOperatorAnd(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context);

/// Check argument types for set binary operator (i.e. one of "and", "or", "unless").
void checkArgumentTypesForSetBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    const SQLQueryPiece & left_argument,
    const SQLQueryPiece & right_argument,
    const ConverterContext & context);

}
