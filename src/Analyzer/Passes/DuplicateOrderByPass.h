#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove ORDER BY duplicate
  *
  * Example: SELECT * FROM (SELECT * FROM t ORDER BY a) ORDER BY a
  * Result: SELECT * FROM (SELECT * FROM t ORDER BY a)
  */
class DuplicateOrderByPass final : public IQueryTreePass
{
public:
    String getName() override { return "DeduplicateOrderBy"; }

    String getDescription() override { return "Optimize query with ORDER BY duplicates"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;
};

}
