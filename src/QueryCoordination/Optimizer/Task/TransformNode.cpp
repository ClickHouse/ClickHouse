#include <QueryCoordination/Optimizer/Group.h>
#include <QueryCoordination/Optimizer/GroupNode.h>
#include <QueryCoordination/Optimizer/Memo.h>
#include <QueryCoordination/Optimizer/Task/TransformGroup.h>
#include <QueryCoordination/Optimizer/Task/TransformNode.h>
#include <QueryCoordination/Optimizer/Transform/Transformation.h>


namespace DB
{

void TransformNode::execute()
{
    const auto & step = group_node.getStep();

    const auto & transformations = Optimizer::getTransformations();

    std::vector<std::vector<StepTree>> collect_transformed_node;

    /// Apply all transformations.
    for (const auto & transformation : transformations)
    {
        if (!transformation.apply)
            continue;

        auto sub_query_plans = transformation.apply(step, context);

        if (!sub_query_plans.empty())
            collect_transformed_node.emplace_back(std::move(sub_query_plans));
    }

    for (auto & sub_query_plans : collect_transformed_node)
    {
        for (auto & sub_query_plan : sub_query_plans)
        {
            memo.addPlanNodeToGroup(sub_query_plan.getRoot(), group);
        }
    }

    for (auto * child_group : group_node.getChildren())
    {
        auto transform_group = std::make_unique<TransformGroup>(memo, stack, context, *child_group);
        pushTask(std::move(transform_group));
    }
}

}

