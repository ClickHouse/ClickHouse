#include <Analyzer/Passes/OptimizeGroupByInjectiveFunctionsPass.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/IQueryTreeNode.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/ExternalDictionariesLoader.h>

namespace DB
{

namespace
{

const std::unordered_set<String> possibly_injective_function_names
{
        "dictGet",
        "dictGetString",
        "dictGetUInt8",
        "dictGetUInt16",
        "dictGetUInt32",
        "dictGetUInt64",
        "dictGetInt8",
        "dictGetInt16",
        "dictGetInt32",
        "dictGetInt64",
        "dictGetFloat32",
        "dictGetFloat64",
        "dictGetDate",
        "dictGetDateTime"
};

class OptimizeGroupByInjectiveFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>
{
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeGroupByInjectiveFunctionsVisitor>;
public:
    explicit OptimizeGroupByInjectiveFunctionsVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_injective_functions_in_group_by)
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
    void optimizeGroupingSet(QueryTreeNodes & grouping_set)
    {
        auto context = getContext();

        QueryTreeNodes new_group_by_keys;
        new_group_by_keys.reserve(grouping_set.size());
        for (auto & group_by_elem : grouping_set)
        {
            std::queue<QueryTreeNodePtr> nodes_to_process;
            nodes_to_process.push(group_by_elem);

            while (!nodes_to_process.empty())
            {
                auto node_to_process = nodes_to_process.front();
                nodes_to_process.pop();

                auto const * function_node = node_to_process->as<FunctionNode>();
                if (!function_node)
                {
                    // Constant aggregation keys are removed in PlannerExpressionAnalysis.cpp
                    new_group_by_keys.push_back(node_to_process);
                    continue;
                }

                // Aggregate functions are not allowed in GROUP BY clause
                auto function = function_node->getFunctionOrThrow();
                bool can_be_eliminated = function->isInjective(function_node->getArgumentColumns());

                if (can_be_eliminated)
                {
                    for (auto const & argument : function_node->getArguments())
                    {
                        // We can skip constants here because aggregation key is already not a constant.
                        if (argument->getNodeType() != QueryTreeNodeType::CONSTANT)
                            nodes_to_process.push(argument);
                    }
                }
                else
                    new_group_by_keys.push_back(node_to_process);
            }
        }

        grouping_set = std::move(new_group_by_keys);
    }
};

}

void OptimizeGroupByInjectiveFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    OptimizeGroupByInjectiveFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
