#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// When a query has the trivial GROUP BY ... LIMIT shape (see getTrivialGroupByLimit) and no
/// aggregate functions in the projection, we can optimize it by setting max_rows_to_group_by
/// to LIMIT + OFFSET with group_by_overflow_mode = 'any'. The optimization is suppressed when
/// the user has explicitly set a non-ANY group_by_overflow_mode or a tighter
/// max_rows_to_group_by, to preserve their explicit contract.
///
/// The settings-based rewrite is unsound with aggregate functions in the projection (the
/// per-stream cutoff undercounts the values of the kept keys); that case is handled by the
/// planner via the shared kept-keys cutoff instead (see addAggregationStep).
class OptimizeTrivialGroupByLimitPass final : public IQueryTreePass
{
public:
    String getName() override { return "optimizeTrivialGroupByLimit"; }

    String getDescription() override { return "Optimizes trivial GROUP BY LIMIT queries with implicit max_rows_to_group_by setting."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr /* context */) override;
};

}
