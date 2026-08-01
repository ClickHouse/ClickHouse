#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Push WHERE conjuncts that constrain an arrayJoin'd column into arrayFilter on the source array.
  *
  * Example:
  *   SELECT arrayJoin(A) AS a, arrayJoin(B) AS b FROM t WHERE a = 'X' AND b = 'Y'
  * is rewritten to:
  *   SELECT arrayJoin(arrayFilter(x -> x = 'X', A)) AS a,
  *          arrayJoin(arrayFilter(x -> x = 'Y', B)) AS b
  *   FROM t
  *
  * Applicable only to top-level AND conjuncts that depend on exactly one arrayJoin'd column,
  * with a deterministic and stateless predicate. Multi-array ARRAY JOIN and LEFT ARRAY JOIN
  * are not rewritten.
  */
class RewriteArrayJoinFilterToArrayFilterPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteArrayJoinFilterToArrayFilter"; }

    String getDescription() override
    {
        return "Rewrite WHERE predicates on arrayJoin columns into arrayFilter before expansion";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
