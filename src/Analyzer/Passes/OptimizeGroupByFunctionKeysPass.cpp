#include <Analyzer/Passes/OptimizeGroupByFunctionKeysPass.h>

#include <algorithm>
#include <queue>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
    extern const SettingsBool optimize_group_by_function_keys;
}

class OptimizeGroupByFunctionKeysVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByFunctionKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByFunctionKeysVisitor>;
    using Base::Base;

    static bool needChildVisit(QueryTreeNodePtr & parent, QueryTreeNodePtr & child)
    {
        if (parent->getNodeType() == QueryTreeNodeType::TABLE_FUNCTION)
            return false;

        return !child->as<FunctionNode>();
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_group_by_function_keys])
            return;

        /// When group_by_use_nulls = 1 removing keys from GROUP BY can lead
        /// to unexpected types in some functions.
        /// See example in https://github.com/ClickHouse/ClickHouse/pull/61567#issuecomment-2018007887
        if (getSettings()[Setting::group_by_use_nulls])
            return;

        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy())
            return;

        if (query->isGroupByWithCube() || query->isGroupByWithRollup())
            return;

        auto & group_by = query->getGroupBy().getNodes();
        if (query->isGroupByWithGroupingSets())
        {
            for (auto & set : group_by)
            {
                auto & grouping_set = set->as<ListNode>()->getNodes();
                optimizeGroupingSet(grouping_set);
            }
        }
        else
            optimizeGroupingSet(group_by);
    }
private:
    struct NodeWithInfo
    {
        QueryTreeNodePtr node;
        bool parents_are_only_deterministic = false;
    };

    static bool canBeEliminated(QueryTreeNodePtr & node, const QueryTreeNodePtrWithHashSet & group_by_keys)
    {
        auto * function = node->as<FunctionNode>();
        if (!function || function->getArguments().getNodes().empty())
            return false;

        std::vector<NodeWithInfo> candidates;
        auto & function_arguments = function->getArguments().getNodes();
        bool is_deterministic = function->getFunctionOrThrow()->isDeterministicInScopeOfQuery();
        for (auto it = function_arguments.rbegin(); it != function_arguments.rend(); ++it)
            candidates.push_back({ *it, is_deterministic });

        /// Using DFS we traverse function tree and try to find if it uses other keys as function arguments.
        bool found_at_least_one_usage = false;
        while (!candidates.empty())
        {
            auto [candidate, parents_are_only_deterministic] = candidates.back();
            candidates.pop_back();

            bool found = group_by_keys.contains(candidate);
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
                        bool is_deterministic_function = parents_are_only_deterministic &&
                            func->getFunctionOrThrow()->isDeterministicInScopeOfQuery();
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

    static void optimizeGroupingSet(QueryTreeNodes & grouping_set)
    {
        QueryTreeNodePtrWithHashSet group_by_keys(grouping_set.begin(), grouping_set.end());

        QueryTreeNodes new_group_by_keys;
        new_group_by_keys.reserve(grouping_set.size());
        for (auto & group_by_elem : grouping_set)
        {
            if (!canBeEliminated(group_by_elem, group_by_keys))
                new_group_by_keys.push_back(group_by_elem);
        }

        grouping_set = std::move(new_group_by_keys);
    }
};

void OptimizeGroupByFunctionKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeGroupByFunctionKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
