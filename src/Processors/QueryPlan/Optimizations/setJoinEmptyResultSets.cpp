#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/BuildRuntimeFilterStep.h>
#include <Processors/QueryPlan/CreateSetAndFilterOnTheFlyStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IEJoinStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/PreparedSets.h>
#include <Interpreters/TableJoin.h>
#include <Storages/IStorage.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageSnapshot.h>
#include <Common/typeid_cast.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Whether an empty left / right input makes a join of this kind return no rows; a semi join needs a match on both sides.
bool leftInputEmptiesJoin(JoinKind kind, JoinStrictness strictness)
{
    return isInnerOrLeft(kind) || isCrossOrComma(kind) || (isRight(kind) && strictness == JoinStrictness::Semi);
}

bool rightInputEmptiesJoin(JoinKind kind, JoinStrictness strictness)
{
    return isInnerOrRight(kind) || isCrossOrComma(kind) || (isLeft(kind) && strictness == JoinStrictness::Semi);
}

bool emptyInputEmptiesOutput(const IQueryPlanStep * step)
{
    /// Aggregation without keys or by grouping sets, e.g. `GROUPING SETS ((k), ())`, outputs a row for an empty input.
    if (const auto * aggregating = typeid_cast<const AggregatingStep *>(step))
        return !aggregating->getParams().keys.empty() && !aggregating->isGroupingSets();
    if (const auto * merging = typeid_cast<const MergingAggregatedStep *>(step))
        return !merging->getParams().keys.empty() && !merging->isGroupingSets();

    /// `TotalsHaving` outputs its totals row on the totals port, which the short-circuit does not close.
    return typeid_cast<const ExpressionStep *>(step) || typeid_cast<const SortingStep *>(step) || typeid_cast<const DistinctStep *>(step)
        || typeid_cast<const WindowStep *>(step) || typeid_cast<const LimitByStep *>(step) || typeid_cast<const TotalsHavingStep *>(step)
        || typeid_cast<const CreateSetAndFilterOnTheFlyStep *>(step) || typeid_cast<const BuildRuntimeFilterStep *>(step);
}

void collectSets(const QueryPlan::Node & node, std::vector<FutureSetPtr> & sets);

/// For a join node: appends the sets of the inputs whose emptiness empties the join. Returns false if `node` is not a join.
bool collectJoinInputSets(const QueryPlan::Node & node, std::vector<FutureSetPtr> & sets)
{
    if (node.children.size() != 2)
        return false;

    const auto * join = typeid_cast<const JoinStep *>(node.step.get());
    const auto * ie_join = typeid_cast<const IEJoinStep *>(node.step.get());
    if (!join && !ie_join)
        return false;

    const JoinKind kind = join ? join->getJoin()->getTableJoin().kind() : ie_join->getQueryKind();
    const JoinStrictness strictness = join ? join->getJoin()->getTableJoin().strictness() : ie_join->getQueryStrictness();
    const QueryPlan::Node * left = node.children[0];
    const QueryPlan::Node * right = node.children[1];
    /// With `swap_streams` the pipelines are swapped at execution and the TableJoin is already swapped.
    if (join && join->swap_streams)
        std::swap(left, right);

    if (leftInputEmptiesJoin(kind, strictness))
        collectSets(*left, sets);
    if (rightInputEmptiesJoin(kind, strictness))
        collectSets(*right, sets);
    return true;
}

/// Appends the sets of `in` conjuncts such that `node` outputs no rows when one of them is empty.
void collectSets(const QueryPlan::Node & node, std::vector<FutureSetPtr> & sets)
{
    auto append = [&sets](std::vector<FutureSetPtr> more) { std::ranges::move(more, std::back_inserter(sets)); };
    const auto * step = node.step.get();
    if (const auto * filter = typeid_cast<const FilterStep *>(step))
        append(getSetsRequiredByFilter(filter->getExpression(), filter->getFilterColumnName()));
    else if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(step))
    {
        /// Unlike LEFT ARRAY JOIN, ARRAY JOIN outputs nothing for a row whose elements are all filtered out.
        if (array_join->hasElementFilter() && !array_join->isLeft())
            append(getSetsRequiredByFilter(*array_join->getElementFilter(), array_join->getElementFilterColumnName()));
    }
    else if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(step))
    {
        /// A remote source evaluates its filters on other servers, each with its own set.
        const auto & snapshot = source->getStorageSnapshot();
        if (!snapshot || snapshot->storage.isRemote())
            return;
        if (const auto row_level_filter = source->getRowLevelFilter())
            append(getSetsRequiredByFilter(row_level_filter->actions, row_level_filter->column_name));
        const auto prewhere = source->getPrewhereInfo();
        if (prewhere && prewhere->need_filter)
            append(getSetsRequiredByFilter(prewhere->prewhere_actions, prewhere->prewhere_column_name));
        /// `ReadFromMerge` applies the filters pushed into it to every table it reads.
        if (const auto * merge = typeid_cast<const ReadFromMerge *>(source))
            for (const auto & pushed_filter : merge->getPushedDownFilters())
                append(getSetsRequiredByFilter(pushed_filter.actions, pushed_filter.column_name));
        return;
    }
    else if (collectJoinInputSets(node, sets))
        return;
    else if (const auto * filled_join = typeid_cast<const FilledJoinStep *>(step))
    {
        /// A RIGHT or FULL filled join, unless SEMI, also emits the stored rows nobody matched.
        const auto & table_join = filled_join->getJoin()->getTableJoin();
        if (!leftInputEmptiesJoin(table_join.kind(), table_join.strictness()))
            return;
    }
    else if (!emptyInputEmptiesOutput(step))
        return;

    if (node.children.size() == 1)
        collectSets(*node.children.front(), sets);
}

}

void setJoinEmptyResultSets(QueryPlan::Node & node)
{
    std::vector<FutureSetPtr> sets;
    if (!collectJoinInputSets(node, sets))
        return;

    if (auto * join_step = typeid_cast<JoinStep *>(node.step.get()))
        join_step->setEmptyResultSets(std::move(sets));
    else if (auto * ie_join_step = typeid_cast<IEJoinStep *>(node.step.get()))
        ie_join_step->setEmptyResultSets(std::move(sets));
}

}
