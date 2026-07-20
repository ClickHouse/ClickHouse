#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>

#include <algorithm>

namespace DB
{

void forEachSecretArgumentConstantNode(
    const QueryTreeNodes & arguments,
    const FunctionSecretArgumentsFinder::Result & secret_arguments,
    const std::function<void(size_t, ConstantNode &)> & on_secret)
{
    for (size_t n = 0; n < arguments.size(); ++n)
    {
        auto * function_node = arguments[n]->as<FunctionNode>();

        if (function_node
            && std::find(secret_arguments.nested_maps.begin(), secret_arguments.nested_maps.end(), function_node->getFunctionName())
                != secret_arguments.nested_maps.end())
        {
            for (const auto & inner : function_node->getArguments().getNodes())
            {
                ConstantNode * value_node = nullptr;
                if (auto * inner_function = inner->as<FunctionNode>();
                    inner_function && inner_function->getFunctionName() == "equals"
                    && inner_function->getArguments().getNodes().size() == 2)
                    value_node = inner_function->getArguments().getNodes()[1]->as<ConstantNode>();
                if (!value_node)
                    value_node = inner->as<ConstantNode>();
                if (value_node)
                    on_secret(n, *value_node);
            }
            continue;
        }

        const bool in_span = secret_arguments.start <= n && n < secret_arguments.start + secret_arguments.count;
        if (!in_span && !secret_arguments.replaced_arguments.contains(n))
            continue;

        ConstantNode * constant_node = nullptr;
        if (in_span && secret_arguments.are_named && function_node && function_node->getArguments().getNodes().size() >= 2)
            constant_node = function_node->getArguments().getNodes().at(1)->as<ConstantNode>();
        if (!constant_node)
            constant_node = arguments[n]->as<ConstantNode>();
        if (constant_node)
            on_secret(n, *constant_node);
    }
}

}
