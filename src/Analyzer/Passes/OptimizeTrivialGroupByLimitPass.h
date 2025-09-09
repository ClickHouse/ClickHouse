#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/// When a query has GROUP BY and LIMIT without HAVING, ORDER BY or WINDOW clauses,
/// we can optimize it by setting max_rows_to_group_by to the LIMIT value.
/// Only optimize when max_rows_to_group_by is zero (disabled).
class OptimizeTrivialGroupByLimitPass final : public IQueryTreePass
{
public:
    String getName() override { return "optimizeTrivialGroupByLimit"; }

    String getDescription() override { return "Optimizes trivial GROUP BY LIMIT queries with implicit max_rows_to_group_by setting."; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr /* context */) override;
};

}
