#include <Analyzer/FunctionSecretArgumentsFinderTreeNode.h>
#include <Interpreters/SecretArgumentsRegistry.h>

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
    /// map is masked: its key is one of `visible_keys`.
    bool isNonSecretMapChild(const std::vector<std::string> & visible_keys, const QueryTreeNodePtr & node)
    {
        const auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "equals" || function_node->getArguments().getNodes().size() != 2)
            return false;
        /// Keep the value visible only when it is a plain literal or identifier; a non-literal value
        /// (e.g. `role_arn = headers('Authorization' = '...')`) can hide a nested secret, so fail closed.
        const auto & value_node = function_node->getArguments().getNodes()[1];
        if (!value_node->as<ConstantNode>() && !value_node->as<IdentifierNode>())
            return false;
        const auto & key_node = function_node->getArguments().getNodes()[0];
        if (const auto * key_constant = key_node->as<ConstantNode>())
            return key_constant->getValue().getType() == Field::Types::String
                && std::ranges::contains(visible_keys, key_constant->getValue().safeGet<String>());
        if (const auto * key_identifier = key_node->as<IdentifierNode>())
            return std::ranges::contains(visible_keys, key_identifier->getIdentifier().getFullName());
        return false;
    }
}

SecretArgumentsResult findSecretArguments(const FunctionNode & function)
{
    return SecretArgumentsRegistry::instance().find(ASTFunction::Kind::ORDINARY_FUNCTION, FunctionTreeNodeImpl<FunctionNode>(function));
}

SecretArgumentsResult findSecretArguments(const TableFunctionNode & function)
{
    return SecretArgumentsRegistry::instance().find(ASTFunction::Kind::ORDINARY_FUNCTION, FunctionTreeNodeImpl<TableFunctionNode>(function));
}

void forEachSecretArgumentNode(
    QueryTreeNodes & arguments,
    const SecretArgumentsResult & secret_arguments,
    const std::function<void(size_t, QueryTreeNodePtr &)> & on_secret)
{
    for (size_t n = 0; n < arguments.size(); ++n)
    {
        auto * function_node = arguments[n]->as<FunctionNode>();
        const auto nested_map
            = function_node ? secret_arguments.nested_maps.find(function_node->getFunctionName()) : secret_arguments.nested_maps.end();
        if (nested_map != secret_arguments.nested_maps.end())
        {
            for (auto & inner : function_node->getArguments().getNodes())
            {
                if (!isNonSecretMapChild(nested_map->second, inner))
                    on_secret(n, secretValueSlot(inner));
            }
            continue;
        }

        const bool in_span = secret_arguments.start <= n && n < secret_arguments.start + secret_arguments.count;
        if (in_span || secret_arguments.masked_arguments.contains(n) || secret_arguments.replaced_arguments.contains(n))
            on_secret(n, secretValueSlot(arguments[n]));
    }
}

}
