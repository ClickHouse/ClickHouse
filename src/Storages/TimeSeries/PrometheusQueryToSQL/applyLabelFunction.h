#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

inline bool isLabelFunction(std::string_view function_name) { return (function_name == "label_replace") || (function_name == "label_join"); }

/// Applies prometheus functions label_replace() and label_join().
SQLQueryPiece applyLabelFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
