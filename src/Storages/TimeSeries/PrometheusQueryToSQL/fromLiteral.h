#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Makes a SQL query to return a string literal.
SQLQueryPiece fromLiteral(const PQT::StringLiteral * string_node, ConverterContext & context);

/// Makes a SQL query to return a scalar literal.
SQLQueryPiece fromLiteral(const PQT::ScalarLiteral * scalar_node, ConverterContext & context);

/// Makes a SQL query to return an interval literal.
SQLQueryPiece fromLiteral(const PQT::IntervalLiteral * interval_node, ConverterContext & context);

}
