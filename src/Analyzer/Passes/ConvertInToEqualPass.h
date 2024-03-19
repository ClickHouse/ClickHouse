#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB {

/** Optimize `in` to `equals` if possible.
  *
  * Example: SELECT * from test where x IN (1);
  * Result: SELECT * from test where x = 1;
  *
  */
class ConvertInToEqualPass final : public IQueryTreePass {
public:
    String getName() override { return "ConvertInToEqualPass"; }

    String getDescription() override { return "Convert in to equal"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};
}



