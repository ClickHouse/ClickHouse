#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite `INTERSECT DISTINCT` and `EXCEPT DISTINCT` to `SELECT DISTINCT l.* FROM (arm_1) l SEMI|ANTI LEFT JOIN (arm_2) r
  * ON l.c_i <=> r.c_i ...`, folding more than two arms from the left, so that the set operations use the join
  * algorithms and their optimizations. The null-safe comparison matches `NULL` with `NULL` like the set operation
  * does, and an arm whose column types differ from the union's result types is wrapped in a subquery converting them,
  * like the set-operation step converts its inputs.
  *
  * A union whose columns have a `Dynamic` type (which cannot be a join key), or whose arm has duplicate column
  * names (which cannot be referenced from the join), keeps the set-operation step, as do the `ALL` modes and
  * every union when none of the enabled join algorithms can execute a semi join.
  */
class RewriteIntersectExceptToJoinPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteIntersectExceptToJoin"; }

    String getDescription() override { return "Rewrite INTERSECT DISTINCT and EXCEPT DISTINCT to a semi or anti join followed by DISTINCT"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
