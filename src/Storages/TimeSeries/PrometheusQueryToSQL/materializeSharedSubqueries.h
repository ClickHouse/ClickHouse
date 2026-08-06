#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// Marks named subqueries (`WITH <name> AS (...)`) which the query references more than once as MATERIALIZED.
///
/// ClickHouse inlines WITH subqueries per use, so a subquery referenced twice is evaluated twice.
/// The converter emits such plans for topk/bottomk/limitk (the operand grid feeds both the group-selecting
/// aggregation and the join masking the non-selected values) and for the `or` binary operator (the left side
/// feeds both the per-group counting step and the final merge join) - in both cases re-evaluating the whole
/// operand subtree, including its selector scan.
///
/// `MATERIALIZED` makes such a subquery being evaluated once, into a temporary table which is then read by all
/// its consumers. The mark is a hint: it has effect only if the setting `enable_materialized_cte` is enabled on
/// the executing context (the entry points running the generated SQL enable it), and the analyzer falls back to
/// inlining a marked subquery if it turns out to be referenced only once.
///
/// Subqueries referenced once are never marked: for them inlining is both correct and free, and materializing
/// per-sample (raw data) streams could buffer arbitrary amounts of data. (Multi-referenced subqueries are
/// per-series grids - one row per series - so their buffered size is bounded by the series count.)
void materializeSharedSubqueries(const ASTPtr & final_query);

}
