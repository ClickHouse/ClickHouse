#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Modifies the result type after applying a subquery, for example
/// <expression>[1h:5m]
///
/// Subquery is evaluated by evaluating its expression at multiple times, and to do that
/// `NodeEvaluationRangeGetter` considers subqueries and modifies the evaluation range of the `expression`,
/// so this function does only one thing - it changes the result type to RANGE_VECTOR.
SQLQueryPiece applySubquery(const PQT::Subquery * subquery_node, SQLQueryPiece && expression, ConverterContext & context);

}
