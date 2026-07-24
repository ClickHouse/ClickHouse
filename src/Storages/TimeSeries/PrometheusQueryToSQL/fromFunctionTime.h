#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionTime(std::string_view function_name) { return function_name == "time"; }

/// Makes a SQL query to return the current evaluation time (the number of seconds since January 1, 1970 UTC).
SQLQueryPiece fromFunctionTime(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// Makes a query piece returning the evaluation time of the specified node, the same as the function time() would return.
SQLQueryPiece makeTimeQueryPiece(const PQT::Node * node, ConverterContext & context);

}
