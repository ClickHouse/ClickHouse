#pragma once

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Parsers/ASTFunction.h>

namespace DB
{

using WindowFunctionList = std::vector<const ASTFunction *>;

struct WindowFunctionsUtils
{
    static bool collectWindowFunctionsFromExpression(const ASTFunction * node, WindowFunctionList & function_list)
    {
        bool window_function_added = false;
        for (auto const & child : node->children)
        {
            if (const auto * child_node = child->as<ASTFunction>())
                window_function_added = window_function_added || collectWindowFunctionsFromExpression(child_node, function_list);
        }
        if (!window_function_added && AggregateUtils::isAggregateFunction(*node))
        {
            function_list.push_back(node);
            return true;
        }
        return window_function_added;
    }
};


}
