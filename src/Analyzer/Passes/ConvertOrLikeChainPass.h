#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Replaces all the "or"'s with {i}like to multiMatchAny
  *
  * End-anchored patterns (those not ending in an unescaped `%`, e.g. `'foo'`, `'%foo'`, `'fo_o'`) are never
  * rewritten: Vectorscan matches `$` before a final newline, RE2 does not.
  */
class ConvertOrLikeChainPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertOrLikeChain"; }

    String getDescription() override { return "Replaces all the 'or's with {i}like to multiMatchAny"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
