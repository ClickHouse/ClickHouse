#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks whether the specified AST is the zero group, i.e. either the literal 0 or CAST(0, 'UInt64').
/// Group #0 always means a group with no tags.
bool isZeroGroupAST(const ASTPtr & group);

/// Checks whether a SELECT query built for StoreMethod::VECTOR_GRID provably outputs
/// the constant zero group (i.e. a group with no tags) in its `group` column.
/// Since values of `group` are unique in a VECTOR_GRID resultset (see StoreMethod::VECTOR_GRID),
/// such a query provably returns at most one row.
bool producesConstantZeroGroup(const ASTPtr & select_query);

}
