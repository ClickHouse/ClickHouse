#include <QueryCoordination/NewOptimizer/Task/TransformGroupNode.h>
#include <QueryCoordination/NewOptimizer/Transform/Transformation.h>
#include <QueryCoordination/NewOptimizer/Group.h>
#include <QueryCoordination/NewOptimizer/GroupNode.h>


namespace DB
{

void TransformGroupNode::execute()
{
    /// frist explore children
    for (auto * child_group : group_node.getChildren())
    {
        /// TODO pushTask TransformGroup
        //        transform(*child_group, group_transformed_node);
    }

    const auto & step = group_node.getStep();

    const auto & transformations = NewOptimizer::getTransformations();

    std::vector<std::vector<SubQueryPlan>> collect_transformed_node;

    /// Apply all transformations.
    for (const auto & transformation : transformations)
    {
        if (!transformation.apply)
            continue;

        auto sub_query_plans = transformation.apply(step, query_context);

        if (!sub_query_plans.empty())
            collect_transformed_node.emplace_back(std::move(sub_query_plans));
    }

    for (auto & sub_query_plans : collect_transformed_node)
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            addPlanNodeToGroup(sub_query_plan.getRoot(), group);
        }
    }

}

}

