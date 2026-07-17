#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilterBase *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    const auto & storage_row_level_filter = source_step_with_filter->getRowLevelFilter();

    /// A stateful function (e.g. `logTrace`, `neighbor`, `runningAccumulate`) must observe the same
    /// input blocks it would see without the optimization. When a reader-side filter (an explicit
    /// `PREWHERE`, or a row-level policy filter) contains a stateful function, both effects of this
    /// optimization would change what it sees: composing filters into the index analysis prunes
    /// granules by the deterministic conjuncts, so the stateful part runs on the reduced stream
    /// (e.g. `PREWHERE neighbor(v, 1) = 20 AND key < 5` would produce different `neighbor` values
    /// and select different rows), and the propagated outer `LIMIT` shrinks or truncates the read
    /// for sources that consume it. Keep the reader untouched: skip both filter composition and
    /// limit propagation. See the sibling fences in `optimizeTopK`, `useVectorSearch`, and
    /// `useVectorSearchWithQuantizedCodes`.
    if ((storage_row_level_filter && storage_row_level_filter->actions.hasStatefulFunctions())
        || (storage_prewhere_info && storage_prewhere_info->prewhere_actions.hasStatefulFunctions()))
    {
        source_step_with_filter->applyFilters();
        return;
    }

    if (storage_row_level_filter)
        source_step_with_filter->addFilter(storage_row_level_filter->actions.clone(), storage_row_level_filter->column_name);
    if (storage_prewhere_info)
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);

    /// Collect ExpressionStep DAGs encountered while walking up the plan.
    /// When a filter references columns produced by expressions (e.g., ALIAS
    /// columns computed in "Compute alias columns" step, or renamed in
    /// "Change column names to column identifiers" step), we compose the
    /// filter through these expression DAGs so that column references are
    /// resolved to physical columns. This is essential for correct index
    /// analysis when plan optimizations like mergeExpressions have not
    /// merged these steps into the filter.
    std::vector<const ActionsDAG *> expression_dags;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            /// Same reasoning as for reader-side filters above and for `ExpressionStep` below: a
            /// stateful function in a `FilterStep` must see the unreduced stream, but composing the
            /// filter into the index analysis would prune granules by its deterministic conjuncts,
            /// and walking further would propagate the outer `LIMIT` below the filter into the
            /// source. `arrayJoin` changes row cardinality the same way. Stop walking here.
            if (filter_step->getExpression().hasArrayJoin() || filter_step->getExpression().hasStatefulFunctions())
                break;

            auto filter_dag = filter_step->getExpression().clone();
            auto filter_column_name = filter_step->getFilterColumnName();

            /// Compose filter through accumulated expression DAGs
            /// (in bottom-to-top order). This resolves column identifiers
            /// to their underlying expressions, enabling correct index
            /// matching for ALIAS columns and renamed columns.
            for (auto it = expression_dags.rbegin(); it != expression_dags.rend(); ++it)
                filter_dag = ActionsDAG::merge((*it)->clone(), std::move(filter_dag));

            source_step_with_filter->addFilter(std::move(filter_dag), filter_column_name);
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            source_step_with_filter->setLimit(limit_step->getLimitForSorting());
            break;
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(iter->node->step.get()))
        {
            /// `arrayJoin` in an `ExpressionStep` above the source changes row cardinality.
            /// Propagating the outer `LIMIT` past such a step is unsound: the source would
            /// be told to generate at most N rows, and `arrayJoin` would then expand only
            /// those (possibly producing fewer than N output rows when arrays are empty,
            /// or wrong rows when arrays expand). Composing filters through `arrayJoin`
            /// expressions is unsound for the same reason. Stop walking here and skip both
            /// filter composition and limit propagation. See issue #82279 and the sibling
            /// guards in `liftUpFunctions`, `optimizeLazyMaterialization`, `optimizeTopK`,
            /// `topKThroughJoin`, and `pushLimitByIntoSort`.
            /// A stateful expression (e.g. `neighbor`, `logTrace`) must see the same input rows
            /// it would see without the optimization, so neither the outer `LIMIT` nor filters
            /// from above it may reduce the source rows. Stop walking here as well.
            if (expression_step->getExpression().hasArrayJoin() || expression_step->getExpression().hasStatefulFunctions())
                break;
            expression_dags.push_back(&expression_step->getExpression());
            continue;
        }
        else if (auto * object_filter_step = typeid_cast<ObjectFilterStep *>(iter->node->step.get()))
        {
            source_step_with_filter->addFilter(object_filter_step->getExpression().clone(), object_filter_step->getFilterColumnName());
        }
        else
        {
            break;
        }
    }

    source_step_with_filter->applyFilters();
}

}
