#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert equal type of comparison chain into single.
  * Replace column > A AND column > B AND column > C, where A < B < C, to column > C
  * Replace column < A AND column < B AND column < C, where A < B < C, to column < A
  *
  * Example: SELECT column FROM table WHERE column > 1 AND column > 5;
  * Result: SELECT column FROM table WHERE column > 5;
  */

class OptimizeMultipleConstantComparisonPass final : public IQueryTreePass
{
public:
    OptimizeMultipleConstantComparisonPass(int second_pass = 0) { second_pass_ = second_pass; }
    String getName() override { return "OptimizeMultipleConstantComparison"; }

    String getDescription() override { return "Optimize multiple constant comparison for one"; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

private:
    int second_pass_ = 0;
};

}
