#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite possible 'arrayExists() with its lambda function has ILIKE and the literals are constant to
    Ilike '%a%' or Ilike '%b%' to improve performance.
  *
  * Example: SELECT arrayExists(_a -> ('x' ILIKE _a), ['%A%', '%b%']);
  * Result: SELECT 'x' ILIKE '%a%' OR 'abc' ILIKE '%b%';
  *
  */
class RewriteArrayExistsLikeToOrPass final : public IQueryTreePass
{
public:
    String getName() override { return "RewriteArrayExistsLikeToOr"; }

    String getDescription() override { return "Rewrite arrayExists(lambda with ILIKE, constant) functions to multiple ILIKE OR'ed together "; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
