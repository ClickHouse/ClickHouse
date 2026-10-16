#pragma once

#include <Parsers/IAST_fwd.h>
#include <base/types.h>
#include <vector>


namespace DB::PrometheusQueryToSQL
{

enum class SQLSubqueryType
{
    /// Named subquery: "WITH name AS (SELECT ...)"
    TABLE,

    /// Named subquery evaluated once: "WITH name AS MATERIALIZED (SELECT ...)".
    ///
    /// Must be used for a subquery referenced more than once as a table expression (i.e. in FROM or JOIN),
    /// because ClickHouse inlines WITH subqueries per use, and re-evaluating a step repeats its whole
    /// subtree including selector scans. A debug-build check in finalizeSQL() enforces this.
    ///
    /// The mark is a hint: it has effect only if the setting `enable_materialized_cte` is enabled on the
    /// executing context (the entry points running the generated SQL enable it), and the analyzer falls
    /// back to inlining a marked subquery if it turns out to be referenced only once.
    ///
    /// Only per-series grids (one row per series) should be marked: their materialized size is bounded
    /// by the series count, while a raw-data (per-sample) stream could buffer arbitrary amounts of data.
    MATERIALIZED_TABLE,

    /// Scalar subquery: "WITH (SELECT ...) AS name"
    SCALAR,
};

/// Subqueries are used to calculate steps of prometheus query evaluation.
struct SQLSubquery
{
    SQLSubquery(size_t index_, ASTPtr ast_, SQLSubqueryType subquery_type_);
    String name;
    ASTPtr ast;
    SQLSubqueryType subquery_type;
};

using SQLSubqueries = std::vector<SQLSubquery>;

}
