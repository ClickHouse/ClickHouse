#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Returns whether the specified string is 'label_replace' or 'label_join'.
bool isLabelManipulationFunction(std::string_view function_name);

/// Applies prometheus function label_replace() or label_join().
SQLQueryPiece applyLabelManipulationFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
