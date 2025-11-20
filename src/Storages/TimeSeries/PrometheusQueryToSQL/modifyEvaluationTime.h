#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Applies an offset of the evaluation time, for example
/// <expression> offset 1d
/// or
/// <expression> @ 1609746000
SQLQueryPiece modifyEvaluationTime(const PQT::At * at_node, SQLQueryPiece && expression, ConverterContext & context);

}
