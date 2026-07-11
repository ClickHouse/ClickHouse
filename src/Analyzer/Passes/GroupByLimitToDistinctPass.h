#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite aggregate-free GROUP BY with LIMIT into SELECT DISTINCT.
  *
  * SELECT a, b FROM t GROUP BY a, b LIMIT 10
  * is semantically identical to
  * SELECT DISTINCT a, b FROM t LIMIT 10
  * when the query has no aggregate functions, no HAVING / ORDER BY / QUALIFY / LIMIT BY /
  * window clauses, no GROUP BY modifiers, and the projection is exactly the set of
  * GROUP BY keys: the order of the emitted groups is unspecified, so any LIMIT distinct
  * groups are a valid result.
  *
  * Unlike aggregation, DISTINCT with LIMIT stops reading the input as soon as enough
  * distinct rows are produced, and streams the results.
  */
class GroupByLimitToDistinctPass final : public IQueryTreePass
{
public:
    String getName() override { return "GroupByLimitToDistinct"; }

    String getDescription() override
    {
        return "Rewrite aggregate-free GROUP BY ... LIMIT into SELECT DISTINCT ... LIMIT to enable early termination of the read and streaming of the results.";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
