#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite `x [NOT] IN (uncorrelated subquery)` in top-level WHERE conjuncts into LEFT SEMI/ANTI JOIN,
  * so that the join planner can choose the build side and apply runtime filters, instead of always
  * materializing the whole subquery result into an in-memory set.
  *
  * Example: SELECT a FROM t1 WHERE b NOT IN (SELECT c FROM t2)
  * Result: SELECT a FROM t1 LEFT ANTI JOIN (SELECT c AS __in_join_subquery_column_1_1 FROM t2)
  *         ON b = __in_join_subquery_column_1_1
  *
  * Enabled by the `optimize_rewrite_in_subquery_to_join` setting.
  */
class RewriteInSubqueryToJoinPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteInSubqueryToJoin"; }

    String getDescription() override
    {
        return "Rewrite 'x [NOT] IN (uncorrelated subquery)' in top-level WHERE conjuncts to LEFT SEMI/ANTI JOIN";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
