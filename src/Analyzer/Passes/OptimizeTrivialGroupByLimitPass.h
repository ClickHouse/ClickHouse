#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// When a query has GROUP BY and LIMIT without HAVING, ORDER BY, WINDOW, QUALIFY, LIMIT BY,
/// DISTINCT clauses, GROUP BY modifiers, or aggregate functions, window functions and arrayJoin
/// in the projection, its aggregation can be capped at LIMIT + OFFSET keys. The pass only permits
/// that to the planners of this server and of the shards with the internal setting `trivial_group_by_limit`;
/// a planner caps only a query that has this shape and this limit, and only if its own settings allow that.
/// The optimization is suppressed when the user has explicitly set a non-ANY group_by_overflow_mode
/// or a tighter max_rows_to_group_by, to preserve their explicit contract.
class OptimizeTrivialGroupByLimitPass final : public IQueryTreePass
{
public:
    String getName() override { return "optimizeTrivialGroupByLimit"; }

    String getDescription() override { return "Optimizes trivial GROUP BY LIMIT queries by limiting the number of groups."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr /* context */) override;
};

}
