#include <Interpreters/HashJoin/HashJoin.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

namespace DB::QueryPlanOptimizations
{

/// Hash join produces probe side columns by filtering or replicating the input columns.
/// Materializing these columns can be wasted if they are immediately dropped or filtered/replicated
/// by a follow up join.
///
/// This optimization tells the join to produce `ColumnReplicated` (input column + index) instead if one
/// of the following cases is detected:
/// - The join sits below a LimitStep or SortingStep with a small limit.
/// - The join sits on the probe side of an immediately following join.
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
            if (probe_columns >= settings.min_columns_for_join_lazy_indexing)
                child_join_step->getJoin()->setEnableLazyColumnsIndexing(true);
            break;
        }

        auto * child_expr_step = typeid_cast<ExpressionStep *>(child->step.get());
        auto * child_filter_step = typeid_cast<FilterStep *>(child->step.get());
        if ((!child_expr_step && !child_filter_step) || child->children.size() != 1)
            break;
        child = child->children.front();
    }
}
}
