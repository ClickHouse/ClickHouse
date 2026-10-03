#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Convert `IN` with a single element to `equals` and `NOT IN` to `notEquals`.
  *
  * Example: SELECT * FROM test WHERE x IN (1);
  * Result:  SELECT * FROM test WHERE x = 1;
  *
  * Example: SELECT * FROM test WHERE x NOT IN (1);
  * Result:  SELECT * FROM test WHERE x != 1;
  *
  * Handles `in`, `notIn`, `globalIn`, `globalNotIn`.
  * Skips conversion when the value is NULL, a Tuple, or an Array.
  * Controlled by the `optimize_in_to_equal` setting.
  */
class ConvertInToEqualPass final : public IQueryTreePass
{
public:
    String getName() override { return "ConvertInToEqualPass"; }

    String getDescription() override { return "Convert IN with single element to equals"; }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
