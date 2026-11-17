#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Applies an offset of the evaluation time, for example
/// <expression> offset 1d
/// or
/// <expression> @ 1609746000
///
/// `NodeEvaluationRangeGetter` considers offsets and modifies the evaluation range for the `expression`,
/// but we also need to modify the timestamps of the result which we do here in this function.
SQLQueryPiece applyOffset(const PQT::Offset * offset_node, SQLQueryPiece && expression, ConverterContext & context);

}
