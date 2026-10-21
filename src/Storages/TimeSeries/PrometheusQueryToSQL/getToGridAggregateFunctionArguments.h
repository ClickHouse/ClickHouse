#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Converts a range vector to the two arguments of a `timeSeries*ToGrid` aggregate function: the expressions for the
/// timestamps and the values (as single values or as arrays), referring to the columns of the range vector's subquery.
/// The range vector must not be empty.
ASTs getToGridAggregateFunctionArguments(const SQLQueryPiece & range_vector, ConverterContext & context);

}
