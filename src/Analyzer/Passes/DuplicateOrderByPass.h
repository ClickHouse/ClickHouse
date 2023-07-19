#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove duplicate ORDER BY if it's possible.
  *
  * Example: SELECT c1 FROM (SELECT c1 FROM table ORDER BY c1) ORDER BY c1;
  * Result: SELECT c1 FROM (SELECT c1 FROM table) ORDER BY c1;
  *
  * Example: SELECT c1 FROM (SELECT c1 FROM table ORDER BY c1) GROUP BY c1;
  * Result: SELECT c1 FROM (SELECT c1 FROM table) ORDER BY c1;
  */
class DuplicateOrderByPass final : public IQueryTreePass
{
public:
    String getName() override { return "DuplicateOrderBy"; }

    String getDescription() override
    {
        return "Remove duplicate ORDER BY if it's possible.";
    }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
