#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Returns whether a specified string is the name of a prometheus date/time function.
/// Examples: minute(), day_of_week(), etc.
bool isDateTimeFunction(std::string_view function_name);

/// Applies a prometheus date/time function.
SQLQueryPiece applyDateTimeFunction(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

}
