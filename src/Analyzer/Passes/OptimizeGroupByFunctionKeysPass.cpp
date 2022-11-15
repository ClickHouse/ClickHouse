#include <Analyzer/Passes/OptimizeGroupByFunctionKeysPass.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>

#include <algorithm>
#include <queue>

namespace DB
{

class OptimizeGroupByFunctionKeysVisitor : public InDepthQueryTreeVisitor<OptimizeGroupByFunctionKeysVisitor>
{
    static bool canBeEliminated(QueryTreeNodePtr & node, QueryTreeNodes const & group_by_nodes)
    {
        auto * function = node->as<FunctionNode>();
        if (!function || function->getArguments().getNodes().empty())
            return false;

        std::queue<QueryTreeNodePtr> candidates;
        for (auto & arg : function->getArguments().getNodes())
            candidates.emplace(arg);

        // Using BFS we traverse function tree and try to find if it uses other keys as function arguments.
        // TODO: Also process CONSTANT here. We can simplify GROUP BY x, x + 1 to GROUP BY x.
        while (!candidates.empty())
        {
            auto candidate = candidates.front();
            candidates.pop();

            auto it = std::find_if(
                group_by_nodes.begin(),
                group_by_nodes.end(),
                [candidate](QueryTreeNodePtr group_by_elem)
                {
                    return group_by_elem->isEqual(*candidate);
                });
            bool found = it != group_by_nodes.end();

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
                    for (auto & arg : arguments)
                        candidates.emplace(arg);
                }
                break;
            }
            case QueryTreeNodeType::COLUMN:
                if (!found)
                    return false;
                break;
            default:
                return false;
            }
        }
        return true;
    }

    static void optimizeGroupingSet(QueryTreeNodes & grouping_set)
    {
        QueryTreeNodes new_group_by_keys;
        new_group_by_keys.reserve(grouping_set.size());
        for (auto & group_by_elem : grouping_set)
        {
            if (!canBeEliminated(group_by_elem, grouping_set))
                new_group_by_keys.push_back(group_by_elem);
        }

        grouping_set = std::move(new_group_by_keys);
    }

public:
    static bool needChildVisit(QueryTreeNodePtr & /*parent*/, QueryTreeNodePtr & child)
    {
        return !child->as<FunctionNode>();
    }

    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasGroupBy())
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
};

void OptimizeGroupByFunctionKeysPass::run(QueryTreeNodePtr query_tree_node, ContextPtr /*context*/)
{
    OptimizeGroupByFunctionKeysVisitor().visit(query_tree_node);
}

}
