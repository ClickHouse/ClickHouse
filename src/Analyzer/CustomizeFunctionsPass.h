#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Customize aggregate functions and `in` functions implementations.
  *
  * Example: SELECT countDistinct();
  * Result: SELECT countDistinctImplementation();
  * Function countDistinctImplementation is taken from settings.count_distinct_implementation.
  */
class CustomizeFunctionsPass final : public IQueryTreePass
{
public:
    String getName() override { return "CustomizeFunctionsPass"; }

    String getDescription() override { return "Customize implementation of aggregate functions, and in functions."; }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}

