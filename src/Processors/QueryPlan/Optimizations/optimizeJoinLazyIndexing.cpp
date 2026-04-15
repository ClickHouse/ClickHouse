#include <Interpreters/HashJoin/HashJoin.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

namespace DB::QueryPlanOptimizations
{

void optimizeJoinLazyIndexing(QueryPlan::Node & node, QueryPlan::Nodes & /*nodes*/, const QueryPlanOptimizationSettings & settings)
{
    auto * limit_step = typeid_cast<LimitStep *>(node.step.get());
    auto * sorting_step = typeid_cast<SortingStep *>(node.step.get());
    auto * join_step = typeid_cast<JoinStep *>(node.step.get());
    if (!limit_step && !sorting_step && !join_step)
        return;

    if (limit_step || sorting_step)
    {
        size_t limit = limit_step ? limit_step->getLimit() : sorting_step->getLimit();
        if (settings.max_limit_for_join_lazy_indexing != 0 && (limit == 0 || limit > settings.max_limit_for_join_lazy_indexing))
            return;
    }

    static constexpr size_t MIN_PROBE_COLUMNS = 3;

    if (node.children.empty())
        return;

    /// Enable lazy columns indexing on reachable join operators. For joins, check on the probe side only.
    auto * child = node.children.front();
    while (child)
    {
        auto * child_join_step = typeid_cast<JoinStep *>(child->step.get());
        if (child_join_step)
        {
            size_t probe_columns = !child->children.empty() ? child->children.front()->step->getOutputHeader()->columns() : 0;
            if (probe_columns >= MIN_PROBE_COLUMNS)
                child_join_step->getJoin()->setEnableLazyColumnsIndexing(true);
            break;
        }

        auto * child_expr_step = typeid_cast<ExpressionStep *>(child->step.get());
        if (!child_expr_step || child->children.size() != 1)
            break;
        child = child->children.front();
    }
}
}
