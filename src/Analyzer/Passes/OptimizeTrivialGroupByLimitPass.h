#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// When a query has GROUP BY and LIMIT without HAVING, ORDER BY, WINDOW, LIMIT BY clauses,
/// GROUP BY modifiers or aggregate functions in the projection, we can optimize it by setting
/// max_rows_to_group_by to LIMIT + OFFSET with group_by_overflow_mode = 'any'. The optimization
/// is suppressed when the user has explicitly set a non-ANY group_by_overflow_mode or a tighter
/// max_rows_to_group_by, to preserve their explicit contract.
class OptimizeTrivialGroupByLimitPass final : public IQueryTreePass
{
public:
    String getName() override { return "optimizeTrivialGroupByLimit"; }

    String getDescription() override { return "Optimizes trivial GROUP BY LIMIT queries with implicit max_rows_to_group_by setting."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr /* context */) override;
};

}
