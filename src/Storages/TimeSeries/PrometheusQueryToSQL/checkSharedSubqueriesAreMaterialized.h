#pragma once

#include <Parsers/IAST_fwd.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks that every named subquery referenced more than once was added with
/// SQLSubqueryType::MATERIALIZED_TABLE. ClickHouse inlines WITH subqueries per use, so a missed mark
/// means the subquery would be evaluated repeatedly, re-running its whole subtree including selector
/// scans. The code which makes a subquery referenced more than once is responsible for the mark.
///
/// For example, this query passes the check:
///
///   WITH prometheus_query_step_1 AS MATERIALIZED (SELECT ... FROM timeSeriesSelector(...)),
///        prometheus_query_step_2 AS (SELECT ... FROM prometheus_query_step_1),
///        prometheus_query_step_3 AS (SELECT ... FROM prometheus_query_step_1 JOIN prometheus_query_step_2 ...)
///   SELECT ... FROM prometheus_query_step_3
///
/// but without MATERIALIZED it fails: `prometheus_query_step_1` is referenced twice (by steps 2 and 3),
/// so it would be inlined and evaluated twice.
///
/// The check runs in debug and sanitizer builds only; in release builds this function does nothing.
void checkSharedSubqueriesAreMaterialized(const ASTPtr & final_query);

}
