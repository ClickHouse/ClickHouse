#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>

#include <algorithm>

namespace DB
{

namespace
{
    /// The secret value of a `key = value` argument is its second child; anything else carries the
    /// secret in the node itself.
    QueryTreeNodePtr & secretValueSlot(QueryTreeNodePtr & node)
    {
        if (auto * function_node = node->as<FunctionNode>();
            function_node && function_node->getFunctionName() == "equals" && function_node->getArguments().getNodes().size() == 2)
            return function_node->getArguments().getNodes()[1];
        return node;
    }
}

void forEachSecretArgumentNode(
    QueryTreeNodes & arguments,
    const FunctionSecretArgumentsFinder::Result & secret_arguments,
    const std::function<void(size_t, QueryTreeNodePtr &)> & on_secret)
{
    for (size_t n = 0; n < arguments.size(); ++n)
    {
        if (auto * function_node = arguments[n]->as<FunctionNode>();
            function_node
            && std::find(secret_arguments.nested_maps.begin(), secret_arguments.nested_maps.end(), function_node->getFunctionName())
                != secret_arguments.nested_maps.end())
        {
            for (auto & inner : function_node->getArguments().getNodes())
                on_secret(n, secretValueSlot(inner));
            continue;
        }

        const bool in_span = secret_arguments.start <= n && n < secret_arguments.start + secret_arguments.count;
        if (in_span || secret_arguments.replaced_arguments.contains(n))
            on_secret(n, secretValueSlot(arguments[n]));
    }
}

}
