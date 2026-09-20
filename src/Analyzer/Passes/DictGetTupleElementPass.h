#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** If `dictGet` is called with a tuple of attribute names and the result is immediately
  * indexed with `tupleElement`, rewrite the expression to fetch only the single needed attribute.
  *
  * Example: SELECT tupleElement(dictGet('dict', ('a', 'b', 'c'), key), 2)
  * Result:  SELECT dictGet('dict', 'b', key)
  *
  * This also handles `dictGetOrDefault` with a matching rewrite for the default value argument.
  */
class DictGetTupleElementPass final : public IQueryTreePass
{
public:
    String getName() override { return "DictGetTupleElement"; }

    String getDescription() override { return "Optimize tupleElement(dictGet(..., tuple_of_attrs, ...), index) into dictGet(..., single_attr, ...)"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
