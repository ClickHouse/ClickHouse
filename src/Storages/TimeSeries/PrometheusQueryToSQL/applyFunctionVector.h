#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionVector(std::string_view function_name) { return function_name == "vector"; }

/// Applies prometheus function vector().
SQLQueryPiece applyFunctionVector(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
