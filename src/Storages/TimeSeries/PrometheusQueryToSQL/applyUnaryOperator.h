#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Applies an unary operator (i.e. either '+' or '-') to a SQL query built to calculate its argument.
SQLQueryPiece applyUnaryOperator(const PQT::UnaryOperator * operator_node, SQLQueryPiece && argument, ConverterContext & context);

}
