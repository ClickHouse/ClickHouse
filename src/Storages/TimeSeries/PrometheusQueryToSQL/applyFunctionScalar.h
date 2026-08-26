#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isFunctionScalar(std::string_view function_name) { return function_name == "scalar"; }

/// Applies prometheus function scalar().
SQLQueryPiece applyFunctionScalar(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
