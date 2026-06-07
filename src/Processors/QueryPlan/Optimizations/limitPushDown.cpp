#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

/// If plan looks like Limit -> Sorting, update limit for Sorting
static bool tryUpdateLimitForSortingSteps(QueryPlan::Node * node, size_t limit)
{
    if (limit == 0)
        return false;

    QueryPlanStepPtr & step = node->step;
    QueryPlan::Node * child = nullptr;
    bool updated = false;

    if (auto * sorting = typeid_cast<SortingStep *>(step.get()))
    {
        /// TODO: remove LimitStep here.
        sorting->updateLimit(limit);
        updated = true;
        child = node->children.front();
    }

    /// In case we have several sorting steps.
    /// Try update limit for them also if possible.
    if (child)
        tryUpdateLimitForSortingSteps(child, limit);

    return updated;
}

size_t tryPushDownLimit(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & /*settings*/)
{
    if (parent_node->children.size() != 1)
        return 0;

    QueryPlan::Node * child_node = parent_node->children.front();

    auto & parent = parent_node->step;
    auto & child = child_node->step;
    auto * limit = typeid_cast<LimitStep *>(parent.get());

    if (!limit)
        return 0;

    /// Skip LIMIT WITH TIES by now.
    if (limit->withTies())
        return 0;

    /// Push LIMIT into each branch of UNION ALL.
    /// Each branch only needs to produce at most (limit + offset) rows,
    /// because the final LIMIT on top will handle the rest.
    /// Skip when `always_read_till_end` is set (e.g. `exact_rows_before_limit` or `WITH TOTALS`),
    /// because per-branch limits would terminate sources early and break `rows_before_limit_at_least`.
    if (typeid_cast<UnionStep *>(child.get()))
    {
        if (limit->alwaysReadTillEnd())
            return 0;

        const size_t limit_for_branches = limit->getLimitForSorting();
        if (limit_for_branches == 0)
            return 0;

        /// Check if all branches already have a Limit to avoid repeated application.
        bool all_have_limit = true;
        for (const auto * child_of_union : child_node->children)
        {
            if (!typeid_cast<const LimitStep *>(child_of_union->step.get()))
            {
                all_have_limit = false;
                break;
            }
        }
        if (all_have_limit)
            return 0;

        for (auto *& child_of_union : child_node->children)
        {
            auto & limit_node = nodes.emplace_back();
            limit_node.children.push_back(child_of_union);
            child_of_union = &limit_node;

            limit_node.step = std::make_unique<LimitStep>(
                limit_node.children.front()->step->getOutputHeader(),
                limit_for_branches, 0);
        }

        /// The outer Limit stays on top; we just added inner Limits to each branch.
        return 0;
    }

    const auto * transforming = dynamic_cast<const ITransformingStep *>(child.get());

    /// Skip everything which is not transform.
    if (!transforming)
        return 0;

    /// Special cases for sorting steps.
    if (tryUpdateLimitForSortingSteps(child_node, limit->getLimitForSorting()))
        return 0;

    if (auto * distinct = typeid_cast<DistinctStep *>(child.get()))
    {
        distinct->updateLimitHint(limit->getLimitForSorting());
        return 0;
    }

    if (typeid_cast<const SortingStep *>(child.get()))
        return 0;

    /// Special case for TotalsHaving. Totals may be incorrect if we push down limit.
    if (typeid_cast<const TotalsHavingStep *>(child.get()))
        return 0;

    /// Disable for WindowStep.
    /// TODO: we can push down limit in some cases if increase the limit value.
    if (typeid_cast<const WindowStep *>(child.get()))
        return 0;

    /// Now we should decide if pushing down limit possible for this step.

    const auto & transform_traits = transforming->getTransformTraits();
    // const auto & data_stream_traits = transforming->getDataStreamTraits();

    /// Cannot push down if child changes the number of rows.
    if (!transform_traits.preserves_number_of_rows)
        return 0;

    /// Input stream for Limit have changed.
    limit->updateInputHeader(transforming->getInputHeaders().front());

    parent.swap(child);
    return 2;
}

void pushLimitByIntoSort(QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        return;

    auto * limit_by = typeid_cast<LimitByStep *>(node.step.get());
    if (!limit_by)
        return;

    /// There is a ExpressionStep before LimitBy. Skip it.
    QueryPlan::Node * expr_node = node.children.front();
    if (!typeid_cast<ExpressionStep *>(expr_node->step.get()) || expr_node->children.size() != 1)
        return;

    auto * sort = typeid_cast<SortingStep *>(expr_node->children.front()->step.get());
    if (!sort)
        return;

    /// Pushing down `LIMIT BY` adds a parallel pre-cap before the final single-stream
    /// `LimitByStep`. Each stream can reduce its row count before the final merge to
    /// a single stream, so the final merge and later pipeline steps have less data to process.
    ///
    /// We try to apply `LIMIT BY` per stream when each stream is sorted and `LIMIT BY` keys are a
    /// prefix of the sort keys. This way we run the in-order variant of LIMIT BY, which is
    /// just a linear scan over the data with O(1) memory.
    ///
    /// TODO: When LIMIT BY prefix columns are a strict monotonic function of the ORDER BY key,
    ///       we can also do in-order LIMIT BY.
    ///
    /// When LIMIT BY is not a prefix of ORDER BY, we can technically still apply the generic LIMIT BY per stream
    /// by first applying an `ExpressionTransform`, and this will give quite a bit of speedup as well.
    /// However, if data is somewhat randomly distributed and high cardinality, then each stream will have a hash table
    /// whose size is the number of unique keys, and this can cause OOM.
    if (!sort->getSortDescription().hasPrefixWithoutCollation(limit_by->getColumns()))
        return;

    const UInt64 length = limit_by->getGroupLength();
    const UInt64 offset = limit_by->getGroupOffset();
    if (length == 0 || length > std::numeric_limits<UInt64>::max() - offset)
        return;

    sort->updateLimitByHint(limit_by->getColumns(), length + offset);
}

}
