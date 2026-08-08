#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>

#include <queue>
#include <vector>

#include <Analyzer/FunctionNode.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace
{

struct NodeWithInfo
{
    QueryTreeNodePtr node;
    bool parents_are_only_deterministic = false;
};

bool keyArgumentTypesAreAllowed(const ColumnsWithTypeAndName & columns, bool allow_suspicious_types)
{
    if (allow_suspicious_types)
        return true;

    bool is_valid = true;
    auto check = [&](const IDataType & type)
    {
        /// Dynamic and Variant types are not allowed in GROUP BY by default.
        is_valid &= !isDynamic(type) && !isVariant(type);
    };

    for (const auto & column : columns)
    {
        check(*column.type);
        column.type->forEachChild(check);
        if (!is_valid)
            break;
    }

    return is_valid;
}

}

bool isExpressionFunctionOfKeys(const QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & keys)
{
    auto * function = node->as<FunctionNode>();
    if (!function || function->getArguments().getNodes().empty())
        return false;

    /// Aggregate and window functions are allowed as keys (for example in LIMIT BY), but they are
    /// not ordinary functions of their arguments, so such a key cannot be eliminated.
    const auto function_base = function->getFunction();
    if (!function_base)
        return false;

    std::vector<NodeWithInfo> candidates;
    auto & function_arguments = function->getArguments().getNodes();
    bool is_deterministic = function_base->isDeterministicInScopeOfQuery();
    for (auto it = function_arguments.rbegin(); it != function_arguments.rend(); ++it)
        candidates.push_back({ *it, is_deterministic });

    /// Using DFS we traverse function tree and try to find if it uses other keys as function arguments.
    bool found_at_least_one_usage = false;
    while (!candidates.empty())
    {
        auto [candidate, parents_are_only_deterministic] = candidates.back();
        candidates.pop_back();

        bool found = keys.contains(candidate);

        /// A key reached through a non-deterministic function is not determined by the keys, so the
        /// expression is not a function of them (for example rand64(g) takes the key g but is random).
        if (found && !parents_are_only_deterministic)
            return false;

        found_at_least_one_usage |= found;

        switch (candidate->getNodeType())
        {
            case QueryTreeNodeType::FUNCTION:
            {
                auto * func = candidate->as<FunctionNode>();
                auto & arguments = func->getArguments().getNodes();
                if (arguments.empty())
                    return false;

                if (!found)
                {
                    const auto func_base = func->getFunction();
                    if (!func_base)
                        return false;

                    bool is_deterministic_function = parents_are_only_deterministic &&
                        func_base->isDeterministicInScopeOfQuery();
                    for (auto it = arguments.rbegin(); it != arguments.rend(); ++it)
                        candidates.push_back({ *it, is_deterministic_function });
                }
                break;
            }
            case QueryTreeNodeType::COLUMN:
                if (!found)
                    return false;
                break;
            case QueryTreeNodeType::CONSTANT:
                if (!parents_are_only_deterministic)
                    return false;
                break;
            default:
                return false;
        }
    }

    return found_at_least_one_usage;
}

void removeKeysThatAreFunctionsOfOtherKeys(QueryTreeNodes & keys)
{
    QueryTreeNodePtrWithHashSet key_set(keys.begin(), keys.end());

    QueryTreeNodes new_keys;
    new_keys.reserve(keys.size());
    for (auto & key : keys)
    {
        if (!isExpressionFunctionOfKeys(key, key_set))
            new_keys.push_back(key);
    }

    keys = std::move(new_keys);
}

QueryTreeNodes unwrapInjectiveFunctionsInKeys(const QueryTreeNodes & keys, bool allow_suspicious_types)
{
    QueryTreeNodes new_keys;
    new_keys.reserve(keys.size());
    for (const auto & key : keys)
    {
        std::queue<QueryTreeNodePtr> nodes_to_process;
        nodes_to_process.push(key);

        while (!nodes_to_process.empty())
        {
            auto node_to_process = nodes_to_process.front();
            nodes_to_process.pop();

            const auto * function_node = node_to_process->as<FunctionNode>();
            if (!function_node)
            {
                // Constant aggregation keys are removed in PlannerExpressionAnalysis.cpp
                new_keys.push_back(node_to_process);
                continue;
            }

            /// Only ordinary functions can be injective. Aggregate and window functions, for which
            /// getFunction returns null, are allowed as keys (for example in LIMIT BY) and are kept as is.
            const auto function = function_node->getFunction();
            bool can_be_eliminated = false;
            if (function)
            {
                const auto arguments = function_node->getArgumentColumns();
                can_be_eliminated = function->isInjective(arguments) && keyArgumentTypesAreAllowed(arguments, allow_suspicious_types);
            }

            if (can_be_eliminated)
            {
                for (const auto & argument : function_node->getArguments())
                {
                    // We can skip constants here because aggregation key is already not a constant.
                    if (argument->getNodeType() != QueryTreeNodeType::CONSTANT)
                        nodes_to_process.push(argument);
                }
            }
            else
                new_keys.push_back(node_to_process);
        }
    }

    return new_keys;
}

}
