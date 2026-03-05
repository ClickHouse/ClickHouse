#include <Interpreters/HashJoin/HashJoin.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

namespace DB::QueryPlanOptimizations
{
void optimizeJoinLazyIndexing(QueryPlan::Node & node, QueryPlan::Nodes & /*nodes*/, const QueryPlanOptimizationSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(node.step.get());
    if (!limit_step)
        return;

    if (settings.max_limit_for_join_lazy_indexing != 0 && limit_step->getLimit() > settings.max_limit_for_join_lazy_indexing)
        return;

    if (node.children.size() != 1)
        return;

    // Enable lazy columns indexing on reachable join operators
    auto * child = node.children.front();
    while (child)
    {
        if (auto * join_step = typeid_cast<JoinStep *>(child->step.get()))
        {
            join_step->getJoin()->setEnableLazyColumnsIndexing(true);
            break;
        }

        if (!typeid_cast<ExpressionStep *>(child->step.get()) || child->children.size() != 1)
            break;

        child = child->children.front();
    }
}
}
