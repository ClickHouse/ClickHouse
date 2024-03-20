#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{
/** Optimize `in` to `equals` if possible.
  * 1. convert in single value to equal
  * Example: SELECT * from test where x IN (1);
  * Result: SELECT * from test where x = 1;
  *
  * 2. convert not in single value to notEqual
  * Example: SELECT * from test where x NOT IN (1);
  * Result: SELECT * from test where x != 1;
  *
  * If value is null or tuple, do not convert.
  */
class ConvertInToEqualPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertInToEqualPass"; }

    String getDescription() override { return "Convert in to equal"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};
}
