#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite 'any' and 'anyLast' functions pushing them inside original function.
  *
  * Example: SELECT any(f(x, y, g(z)));
  * Result: SELECT f(any(x), any(y), g(any(z)));
  */
class MoveFunctionsOutOfAnyPass final : public IQueryTreePass
{
public:
    String getName() override { return "MoveFunctionsOutOfAnyPass"; }

    String getDescription() override
    {
        return "Rewrite 'any' and 'anyLast' functions pushing them inside original function.";
    }

    void run(QueryTreeNodePtr query_tree_node, ContextPtr context) override;

};

}
