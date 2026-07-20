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

    /// Whether a nested secret map child is a `key = value` node whose value stays visible when the
    /// map is masked (the non-secret identifiers of `extra_credentials`; `headers` values are all hidden).
    bool isNonSecretMapChild(const String & map_name, const QueryTreeNodePtr & node)
    {
        if (map_name != "extra_credentials")
            return false;
        const auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "equals" || function_node->getArguments().getNodes().size() != 2)
            return false;
        const auto & key_node = function_node->getArguments().getNodes()[0];
        if (const auto * key_constant = key_node->as<ConstantNode>())
            return key_constant->getValue().getType() == Field::Types::String
                && FunctionSecretArgumentsFinder::isNonSecretExtraCredentialsKey(key_constant->getValue().safeGet<String>());
        if (const auto * key_identifier = key_node->as<IdentifierNode>())
            return FunctionSecretArgumentsFinder::isNonSecretExtraCredentialsKey(key_identifier->getIdentifier().getFullName());
        return false;
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
            {
                if (!isNonSecretMapChild(function_node->getFunctionName(), inner))
                    on_secret(n, secretValueSlot(inner));
            }
            continue;
        }

        const bool in_span = secret_arguments.start <= n && n < secret_arguments.start + secret_arguments.count;
        if (in_span || secret_arguments.replaced_arguments.contains(n))
            on_secret(n, secretValueSlot(arguments[n]));
    }
}

}
