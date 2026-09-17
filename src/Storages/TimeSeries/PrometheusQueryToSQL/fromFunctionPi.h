#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionPi(std::string_view function_name) { return function_name == "pi"; }

/// Makes a SQL query to return pi = 3.14159...
SQLQueryPiece fromFunctionPi(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
