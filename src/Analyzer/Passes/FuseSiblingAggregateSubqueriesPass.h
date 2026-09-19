#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Answer sibling single-row aggregate subqueries that read the same tables with one scan.
  *
  * The conjuncts every branch shares become the fused filter, and each branch's own residual conjuncts
  * become the condition of an -If combinator on its aggregates.
  *
  * Example:
  *
  * SELECT * FROM (SELECT count() AS a FROM t WHERE c AND x) AS s1,
  *               (SELECT count() AS b FROM t WHERE c AND y) AS s2;
  *
  * is rewritten into
  *
  * SELECT * FROM (SELECT countIf(x) AS a, countIf(y) AS b FROM t WHERE c AND (x OR y)) AS s1;
  */
class FuseSiblingAggregateSubqueriesPass final : public IQueryTreePass
{
public:
    String getName() override { return "FuseSiblingAggregateSubqueries"; }

    String getDescription() override
    {
        return "Answer sibling single-row aggregate subqueries over the same tables with one scan, "
               "moving each branch's own filter into an -If combinator on its aggregates";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
