#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// A scalar converted to an argument of a `timeSeries*ToGrid` aggregate function.
struct ToGridAggregateFunctionScalarArgument
{
    /// nullptr if the scalar is statically empty, then the result of the function is empty too.
    ASTPtr ast;

    /// Whether the scalar is the same at every grid point. Then `ast` is a number: a literal or a reference to a single-row
    /// scalar subquery. Otherwise (e.g. `time()` in a range query) it varies with the evaluation time and `ast` is an array
    /// with one value per grid point.
    bool is_constant = true;
};

/// Converts a scalar to an argument of a `timeSeries*ToGrid` aggregate function that follows the samples.
/// A scalar that is not a constant is registered in `context` as a scalar subquery, which takes the query of `scalar`.
ToGridAggregateFunctionScalarArgument getToGridAggregateFunctionScalarArgument(SQLQueryPiece & scalar, ConverterContext & context);

}
