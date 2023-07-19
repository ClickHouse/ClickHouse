#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Remove duplicate distinct if it's possible.
  *
  * Example: SELECT DISTINCT c1 FROM (SELECT DISTINCT c1 FROM table);
  * Result: SELECT c1 FROM (SELECT DISTINCT c1 FROM table);
  */
class DuplicateDistinctPass final : public IQueryTreePass
{
public:
    String getName() override { return "DuplicateDistinct"; }

    String getDescription() override
    {
        return "Remove duplicate distinct if it's possible.";
    }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
