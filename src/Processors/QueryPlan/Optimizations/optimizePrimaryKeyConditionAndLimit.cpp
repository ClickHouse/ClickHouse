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
            /// `getLimitForSorting` overloads `0` with two meanings: a genuine `LIMIT 0`, and
            /// "the number of rows to read is unknown" when `limit + offset` overflows `UInt64`.
            /// A source step, in contrast, treats the value it is given as an exact upper bound on
            /// the number of rows it may produce, so propagating the overflow sentinel would turn
            /// `LIMIT 18446744073709551615 OFFSET 1` into "read no rows at all" and the query would
            /// return an empty result. Push a zero down only when the query really asks for no rows.
            const size_t limit_for_sorting = limit_step->getLimitForSorting();
            if (limit_for_sorting != 0 || (limit_step->getLimit() == 0 && limit_step->getOffset() == 0))
                source_step_with_filter->setLimit(limit_for_sorting);
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
            if (expression_step->getExpression().hasArrayJoin())
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
