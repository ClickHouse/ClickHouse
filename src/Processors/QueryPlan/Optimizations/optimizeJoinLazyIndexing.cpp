#include <Interpreters/HashJoin/HashJoin.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>

namespace DB::QueryPlanOptimizations
{

/// Counts probe-side input columns which could benefit from delaying the application of the join result index.
/// Mirrors `ColumnReplicated::isReplicationUseful`.
static size_t countColumnsUsefulForLazyIndexing(const QueryPlan::Node * node)
{
    const auto & header = *node->step->getOutputHeader();

    size_t count = 0;
    for (const auto & col : header)
    {
        const auto & type = col.type;
        if (type->lowCardinality())
            continue;
        if (type->isValueUnambiguouslyRepresentedInFixedSizeContiguousMemoryRegion() && type->haveMaximumSizeOfValue()
            && type->getMaximumSizeOfValueInMemory() <= 8)
            continue;
        ++count;
    }
    return count;
}

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

    static constexpr size_t MIN_PROBE_COLUMNS = 1;

    if (node.children.empty())
        return;

    /// Enable lazy columns indexing on reachable join operators. For joins, check on the probe side only.
    auto * child = node.children.front();
    while (child)
    {
        auto * child_join_step = typeid_cast<JoinStep *>(child->step.get());
        if (child_join_step)
        {
            size_t useful = !child->children.empty() ? countColumnsUsefulForLazyIndexing(child->children.front()) : 0;
            if (useful >= MIN_PROBE_COLUMNS)
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
