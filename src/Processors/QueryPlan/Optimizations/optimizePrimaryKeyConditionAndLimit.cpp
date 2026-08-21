#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/JoinStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/ObjectFilterStep.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <DataTypes/DataTypeNullable.h>
#include <Core/Joins.h>

#include <optional>

namespace DB::QueryPlanOptimizations
{

namespace
{

bool isJoinThatAcceptsLeftFilter(IQueryPlanStep * step)
{
    if (const auto * logical_join = typeid_cast<const JoinStepLogical *>(step))
    {
        const auto kind = logical_join->getJoinOperator().kind;
        return isInnerOrLeft(kind) || isCrossOrComma(kind);
    }
    if (const auto * join_step = typeid_cast<const JoinStep *>(step))
    {
        const auto kind = join_step->getJoin()->getTableJoin().kind();
        return isInnerOrLeft(kind) || isCrossOrComma(kind);
    }
    return false;
}

bool typesCompatibleForSourceFilter(const DataTypePtr & header_type, const DataTypePtr & dag_type)
{
    if (header_type->equals(*dag_type))
        return true;
    return removeNullableOrLowCardinalityNullable(header_type)->equals(*removeNullableOrLowCardinalityNullable(dag_type));
}

std::optional<std::string> tryPhysicalNameInHeader(const std::string & name, const Block & header)
{
    if (header.has(name))
        return name;

    const auto pos = name.rfind('.');
    if (pos == std::string::npos || pos + 1 >= name.size())
        return {};

    std::string suffix = name.substr(pos + 1);
    if (suffix.size() >= 2 && suffix.front() == '`' && suffix.back() == '`')
        suffix = suffix.substr(1, suffix.size() - 2);

    if (header.has(suffix))
        return suffix;
    return {};
}

ActionsDAG remapFilterInputsToHeader(ActionsDAG filter_dag, const Block & header)
{
    ActionsDAG rename_dag(header.getColumnsWithTypeAndName());
    bool need_merge = false;

    for (const auto * input : filter_dag.getInputs())
    {
        if (header.has(input->result_name) && typesCompatibleForSourceFilter(header.getByName(input->result_name).type, input->result_type))
            continue;

        auto physical = tryPhysicalNameInHeader(input->result_name, header);
        if (!physical)
            continue;

        const auto & node = rename_dag.findInOutputs(*physical);
        rename_dag.addOrReplaceInOutputs(rename_dag.addAlias(node, input->result_name));
        need_merge = true;
    }

    if (!need_merge)
        return filter_dag;

    auto merged = ActionsDAG::merge(std::move(rename_dag), std::move(filter_dag));
    merged.removeUnusedActions();
    return merged;
}

bool filterInputsAreInHeader(const ActionsDAG & filter_dag, const Block & header)
{
    for (const auto * input : filter_dag.getInputs())
    {
        auto physical = tryPhysicalNameInHeader(input->result_name, header);
        if (!physical)
            return false;
        if (!typesCompatibleForSourceFilter(header.getByName(*physical).type, input->result_type))
            return false;
    }
    return true;
}

}


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
    const QueryPlan::Node * coming_from = frame.node;
    const auto & source_header = *source_step_with_filter->getOutputHeader();
    bool added_filter = false;

    for (auto iter = stack.rbegin() + 1; iter != stack.rend(); ++iter)
    {
        auto * step = iter->node->step.get();

        if (auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            auto filter_dag = filter_step->getExpression().clone();
            auto filter_column_name = filter_step->getFilterColumnName();

            /// Compose filter through accumulated expression DAGs
            /// (in bottom-to-top order). This resolves column identifiers
            /// to their underlying expressions, enabling correct index
            /// matching for ALIAS columns and renamed columns.
            for (auto it = expression_dags.rbegin(); it != expression_dags.rend(); ++it)
                filter_dag = ActionsDAG::merge((*it)->clone(), std::move(filter_dag));

            filter_dag = remapFilterInputsToHeader(std::move(filter_dag), source_header);

            /// A filter above JOIN may reference the other side. Skip those; left-only
            /// predicates still apply to this source (needed for icebergCluster listing).
            if (filterInputsAreInHeader(filter_dag, source_header))
            {
                source_step_with_filter->addFilter(std::move(filter_dag), filter_column_name);
                added_filter = true;
            }
        }
        else if (auto * limit_step = typeid_cast<LimitStep *>(step))
        {
            source_step_with_filter->setLimit(limit_step->getLimitForSorting());
            break;
        }
        else if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
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
        }
        else if (auto * object_filter_step = typeid_cast<ObjectFilterStep *>(step))
        {
            source_step_with_filter->addFilter(object_filter_step->getExpression().clone(), object_filter_step->getFilterColumnName());
            added_filter = true;
        }
        else if (
            !added_filter
            && isJoinThatAcceptsLeftFilter(step)
            && !iter->node->children.empty()
            && iter->node->children.front() == coming_from)
        {
            /// `icebergCluster` lists files during `applyFilters`, which previously
            /// stopped at JOIN. If the left-only WHERE is still above the JOIN,
            /// keep walking so file listing can prune.
        }
        else
        {
            break;
        }

        coming_from = iter->node;
    }

    source_step_with_filter->applyFilters();
}

}
