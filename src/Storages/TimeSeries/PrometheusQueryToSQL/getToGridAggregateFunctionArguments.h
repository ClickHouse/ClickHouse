#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Converts a range vector to the arguments of a `timeSeries*ToGrid` aggregate function, referring to the columns of the
/// range vector's subquery: the expressions for the timestamps and the values (as single values or as arrays),
/// or the single expression for the samples as an array of tuples (timestamp, value) for StoreMethod::SAMPLES.
/// The range vector must not be empty.
ASTs getToGridAggregateFunctionArguments(const SQLQueryPiece & range_vector, ConverterContext & context);

}
