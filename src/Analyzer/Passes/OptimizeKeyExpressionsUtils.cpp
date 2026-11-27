#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>

#include <algorithm>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ValidationUtils.h>

namespace DB
{

QueryTreeNodes getGroupByKeys(const QueryNode & query_node)
{
    QueryTreeNodes keys;
    if (!query_node.hasGroupBy())
        return keys;

    for (const auto & node : query_node.getGroupBy().getNodes())
    {
        if (query_node.isGroupByWithGroupingSets())
        {
            const auto & grouping_set_keys = node->as<ListNode &>().getNodes();
            keys.insert(keys.end(), grouping_set_keys.begin(), grouping_set_keys.end());
        }
        else
        {
            keys.push_back(node);
        }
    }
    return keys;
}

bool isGroupByKeyAfterAggregation(const QueryTreeNodePtr & node, const QueryTreeNodes & group_by_keys)
{
    if (const auto * function_node = node->as<FunctionNode>(); function_node && function_node->isConvertedToNullable())
        return true;
    return std::ranges::any_of(group_by_keys, [&](const auto & key) { return compareGroupByKeys(node, key); });
}

}
