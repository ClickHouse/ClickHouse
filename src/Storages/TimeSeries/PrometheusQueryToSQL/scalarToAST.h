#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// A scalar converted to an AST: one value for the whole grid or one value per grid point.
struct ScalarAST
{
    /// nullptr if the scalar is statically empty.
    ASTPtr ast;

    /// Whether the scalar is the same at every grid point: then `ast` is a number, otherwise an array with one value
    /// per grid point.
    bool is_constant = true;
};

/// Converts a scalar to an AST. A scalar that is not a constant is registered in `context` as a scalar subquery,
/// which takes the query of `scalar`.
ScalarAST scalarToAST(SQLQueryPiece && scalar, ConverterContext & context);

}
