#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>

#include <list>

namespace DB::QueryPlanOptimizations
{

void optimizePrimaryKeyConditionAndLimit(const Stack & stack)
{
    const auto & frame = stack.back();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilterBase *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    /// Collect ExpressionStep DAGs encountered while walking up the plan.
    /// When a filter references columns produced by expressions (e.g., ALIAS
    /// columns computed in "Compute alias columns" step, or renamed in
    /// "Change column names to column identifiers" step), we compose the
    /// filter through these expression DAGs so that column references are
    /// resolved to physical columns. This is essential for correct index
    /// analysis when plan optimizations like mergeExpressions have not
    /// merged these steps into the filter.
    std::vector<const ActionsDAG *> expression_dags;
    /// A list, `expression_dags` keeps pointers into it.
    std::list<ActionsDAG> array_join_dags;

    /// Resolve the filter's columns down to the source through the steps below it.
    auto compose = [&](ActionsDAG filter_dag)
    {
        for (auto it = expression_dags.rbegin(); it != expression_dags.rend(); ++it)
            filter_dag = ActionsDAG::merge((*it)->clone(), std::move(filter_dag));
        return filter_dag;
    };

    const auto & storage_prewhere_info = source_step_with_filter->getPrewhereInfo();
    const auto & storage_row_level_filter = source_step_with_filter->getRowLevelFilter();
    if (storage_row_level_filter)
        source_step_with_filter->addFilter(storage_row_level_filter->actions.clone(), storage_row_level_filter->column_name);
    if (storage_prewhere_info)
    {
        source_step_with_filter->addFilter(storage_prewhere_info->prewhere_actions.clone(), storage_prewhere_info->prewhere_column_name);
        /// Prewhere and filters may also compute columns that the steps above refer to.
        expression_dags.push_back(&storage_prewhere_info->prewhere_actions);
    }

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        if (auto * filter_step = typeid_cast<FilterStep *>(iter->node->step.get()))
        {
            source_step_with_filter->addFilter(compose(filter_step->getExpression().clone()), filter_step->getFilterColumnName());
            expression_dags.push_back(&filter_step->getExpression());
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(iter->node->step.get()))
        {
            /// A LIMIT above an ARRAY JOIN says nothing about the source row count.
            if (array_join_dags.empty())
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
            if (expression_step->getExpression().hasArrayJoin())
                break;
            expression_dags.push_back(&expression_step->getExpression());
            continue;
        }
        else if (auto * array_join_step = typeid_cast<ArrayJoinStep *>(iter->node->step.get()))
        {
            /// A plain ARRAY JOIN emits real elements only, so a condition on the element is one on the array too.
            /// LEFT and unaligned joins pad empty or short arrays with defaults, so theirs is not.
            if (array_join_step->isLeft() || array_join_step->isUnaligned())
                break;

            auto & dag = array_join_dags.emplace_back(array_join_step->getInputHeaders().front()->getNamesAndTypesList());
            for (const auto & name : array_join_step->getColumns())
            {
                /// The atom must be `arrayJoin(col)`, not something named like the array column; the alias just keeps the name.
                dag.addOrReplaceInOutputs(dag.addAlias(dag.addArrayJoin(dag.findInOutputs(name), {}), name));
            }
            expression_dags.push_back(&dag);

            /// The fused element filter is just a filter right above the join.
            if (const auto & element_filter = array_join_step->getElementFilter())
                source_step_with_filter->addFilter(compose(element_filter->clone()), array_join_step->getElementFilterColumnName());
        }
        else if (auto * object_filter_step = typeid_cast<ObjectFilterStep *>(iter->node->step.get()))
        {
            /// Not composed, so above an ARRAY JOIN its names would mean the elements.
            if (!array_join_dags.empty())
                break;
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
