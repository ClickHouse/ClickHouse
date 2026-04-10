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
    if (!limit_step && !sorting_step)
        return;

    size_t limit = limit_step ? limit_step->getLimit() : sorting_step->getLimit();
    size_t max_limit = settings.max_limit_for_join_lazy_indexing;
    if (max_limit != 0 && limit > max_limit)
        return;

    static constexpr size_t MIN_PROBE_COLUMNS = 3;

    if (node.children.size() != 1)
        return;

    /// Enable lazy columns indexing on reachable join operators.
    auto * child = node.children.front();
    while (child)
    {
        auto * join_step = typeid_cast<JoinStep *>(child->step.get());
        auto * expr_step = typeid_cast<ExpressionStep *>(child->step.get());

        if (!join_step && (!expr_step || child->children.size() != 1))
            break;

        if (join_step)
        {
            /// Only enable lazy columns indexing if there are more than 3 probe side columns.
            if (!child->children.empty() && child->children.front()->step->getOutputHeader()->columns() >= MIN_PROBE_COLUMNS)
                join_step->getJoin()->setEnableLazyColumnsIndexing(true);
        }

        if (child->children.empty())
            break;

        /// For joins, continue on the probe side.
        child = child->children.front();
    }
}
}
