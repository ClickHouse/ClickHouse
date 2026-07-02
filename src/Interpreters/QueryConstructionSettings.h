#pragma once

#include <Parsers/IAST_fwd.h>

#include <cstddef>

namespace DB
{

class IAST;

/// The query-construction settings (`select` / `filter` / `order` / `sort` / `limit` / `offset` /
/// `page`) shape the result a query returns. They are not applied by the interpreter; instead they are
/// materialized into the query AST by wrapping the (sub)query they belong to as a derived table with an
/// outer `SELECT` / `WHERE` / `ORDER BY` / `LIMIT` / `OFFSET`. `executeQuery` does this for directly
/// executed queries; the declarations below let other places that re-enter execution with a raw AST
/// (notably stored views, whose inner query bypasses `executeQuery`) perform the same materialization.

/// Materialize the construction settings that a (sub)query carries in its OWN `SETTINGS` clause, by
/// wrapping that (sub)query as a derived table. Recurses through subqueries, `UNION` arms, and into the
/// source `SELECT` of an `INSERT … SELECT` / immediate `CREATE … AS SELECT`. A `SETTINGS` clause applies
/// to its own scope only — not to deeper subqueries, nor to the outer query.
void wrapNestedConstructionSettings(
    ASTPtr & ast, size_t max_query_size, size_t max_parser_depth, size_t max_parser_backtracks);

/// True if any `SETTINGS` clause anywhere in the AST subtree carries a query-construction setting.
bool hasConstructionSettings(const IAST & ast);

}
