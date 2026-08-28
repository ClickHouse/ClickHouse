#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionPlannerOnlyFilter.h>
#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypeLowCardinality.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

struct FilterConjuncts
{
    /// All conjuncts in the order they appear in the filter, so that the filter can be rebuilt without reordering it.
    ActionsDAG::NodeRawConstPtrs all_conjuncts;
    ActionsDAG::NodeRawConstPtrs planner_only_conjuncts;
    ActionsDAG::NodeRawConstPtrs other_conjuncts;
};

bool isPlannerOnlyFilter(const ActionsDAG::Node & node)
{
    return node.type == ActionsDAG::ActionType::FUNCTION && node.function_base && isPlannerOnlyFilterFunction(*node.function_base);
}

void collectFilterConjuncts(const ActionsDAG::Node * node, FilterConjuncts & conjuncts)
{
    /// Unwrap aliases.
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();

    if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base && node->function_base->getName() == "and")
    {
        for (const auto * child : node->children)
            collectFilterConjuncts(child, conjuncts);
        return;
    }

    conjuncts.all_conjuncts.push_back(node);
    if (isPlannerOnlyFilter(*node))
        conjuncts.planner_only_conjuncts.push_back(node);
    else
        conjuncts.other_conjuncts.push_back(node);
}

std::optional<Float64> estimateNotNullFilterSelectivity(const QueryPlan::Node & node, const ActionsDAG::Node & predicate)
{
    /// Only filter immediately above a scan can be executed.
    const auto * reading = typeid_cast<ReadFromMergeTree *>(node.children.front()->step.get());
    if (!reading)
        return {};

    auto estimator = reading->getConditionSelectivityEstimator({predicate.children.front()->result_name});
    if (!estimator)
        return {};

    const auto total_rows = estimator->estimateRelationProfile().rows;
    if (total_rows == 0)
        return {};

    const auto surviving_rows = estimator->estimateRelationProfile(reading->getStorageMetadata(), &predicate).rows;
    return static_cast<Float64>(surviving_rows) / static_cast<Float64>(total_rows);
}

bool dependsOnColumn(const ActionsDAG::Node * node, const ActionsDAG::Node & column)
{
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack = {node};

    while (!stack.empty())
    {
        const auto * current = stack.back();
        stack.pop_back();

        if (!visited.insert(current).second)
            continue;

        if (current->type == ActionsDAG::ActionType::INPUT)
        {
            if (current->result_name == column.result_name)
                return true;
            continue;
        }

        stack.insert(stack.end(), current->children.begin(), current->children.end());
    }

    return false;
}

bool isSubsumedByOtherFilterConjunct(const ActionsDAG::Node & column, const ActionsDAG::NodeRawConstPtrs & other_filters)
{
    if (other_filters.empty())
        return false;

    ColumnPtr constant_column = column.result_type->createColumnConst(1, Field());
    auto constant_column_with_type_and_name = ColumnWithTypeAndName{constant_column, column.result_type, column.result_name};

    ActionsDAG::IntermediateExecutionResult filter_input;
    filter_input.emplace(&column, std::move(constant_column_with_type_and_name));

    for (const auto * other_filter : other_filters)
    {
        /// Skip filter conjuncts that do not depend on the column in the NOT NULL filter.
        if (!dependsOnColumn(other_filter, column))
            continue;

        try
        {
            /// If it can not be proven that `other_filter` allows null to survive, it might subsume the NOT NULL filter.
            auto result = ActionsDAG::evaluatePartialResult(filter_input, {other_filter}, /*input_rows_count=*/1, {.skip_materialize = true, .allow_unknown_function_arguments = true});
            if (getFilterResult(result.front()) != FilterResult::TRUE)
                return true;
        }
        catch (const Exception &)
        {
            /// Don't execute filters when it can not be proven that they are not subsumed by other filters.
            return true;
        }
    }

    return false;
}

/// Select planner-only markers wrapping NOT NULL filters with a low enough estimated selectivity that they are worth executing.
std::unordered_set<const ActionsDAG::Node *> collectExecutableNotNullFilters(const QueryPlan::Node & node, const FilterConjuncts & conjuncts, const QueryPlanOptimizationSettings & settings)
{
    std::unordered_set<const ActionsDAG::Node *> to_execute;
    if (!settings.allow_derived_not_null_filters_execution)
        return to_execute;

    NameSet to_execute_columns;
    for (const auto * planner_only_marker : conjuncts.planner_only_conjuncts)
    {
        const auto * predicate = planner_only_marker->children.front();
        if (predicate->type != ActionsDAG::ActionType::FUNCTION || !predicate->function_base || predicate->function_base->getName() != "isNotNull")
            continue;

        const auto * column = predicate->children.front();
        if (column->type != ActionsDAG::ActionType::INPUT)
            continue;

        if (!isNullableOrLowCardinalityNullable(column->result_type))
            continue;

        /// There could be multiple filters for the same column, execute only one.
        if (!to_execute_columns.insert(column->result_name).second)
            continue;

        /// Avoid executing filters that are subsumed by existing filters and won't add any selectivity.
        if (isSubsumedByOtherFilterConjunct(*column, conjuncts.other_conjuncts))
            continue;

        auto selectivity = estimateNotNullFilterSelectivity(node, *predicate);
        if (selectivity && *selectivity <= settings.max_selectivity_for_not_null_filters_execution)
            to_execute.insert(planner_only_marker);
    }

    return to_execute;
}

}

void resolvePlannerOnlyFilters(QueryPlan::Node & node, const QueryPlanOptimizationSettings & settings)
{
    auto * filter = typeid_cast<FilterStep *>(node.step.get());
    if (!filter || node.children.size() != 1)
        return;

    /// If the filter column stays in the output header, replacing it would change the header and break the parent step.
    if (!filter->removesFilterColumn())
        return;

    const auto & actions_dag = filter->getExpression();
    const auto * filter_output = actions_dag.tryFindInOutputs(filter->getFilterColumnName());
    if (!filter_output)
        return;

    FilterConjuncts conjuncts;
    collectFilterConjuncts(filter_output, conjuncts);
    if (conjuncts.planner_only_conjuncts.empty())
        return;

    auto to_execute = collectExecutableNotNullFilters(node, conjuncts, settings);

    const auto input_header = filter->getInputHeaders().front();

    /// Executable markers are replaced by the filter they wrap, the remaining ones are dropped.
    ActionsDAG::NodeMapping node_map;
    ActionsDAG new_actions_dag = actions_dag.clone(node_map);

    ActionsDAG::NodeRawConstPtrs new_conjuncts;
    new_conjuncts.reserve(conjuncts.all_conjuncts.size());
    for (const auto * conjunct : conjuncts.all_conjuncts)
    {
        const auto * new_conjunct = node_map.at(conjunct);
        if (!isPlannerOnlyFilter(*conjunct))
            new_conjuncts.push_back(new_conjunct);
        else if (to_execute.contains(conjunct))
            new_conjuncts.push_back(new_conjunct->children.front());
    }

    new_actions_dag.removeUnusedResult(filter->getFilterColumnName());

    /// Nothing is left to filter by, the step becomes an expression.
    if (new_conjuncts.empty())
    {
        new_actions_dag.removeUnusedActions(/*allow_remove_inputs=*/false);

        /// Ensure no planner-only filters remain in the actions of the step.
        chassert(!new_actions_dag.hasPlannerOnlyFilters());

        node.step = std::make_unique<ExpressionStep>(input_header, std::move(new_actions_dag));
        return;
    }

    const ActionsDAG::Node * new_filter = new_conjuncts.front();
    if (new_conjuncts.size() > 1)
    {
        auto func_and = FunctionFactory::instance().get("and", Context::getGlobalContextInstance());
        new_filter = &new_actions_dag.addFunction(func_and, std::move(new_conjuncts), {});
    }

    new_actions_dag.addOrReplaceInOutputs(*new_filter);
    new_actions_dag.removeUnusedActions(/*allow_remove_inputs=*/false);

    /// Ensure no planner-only filters remain in the actions of the step.
    chassert(!new_actions_dag.hasPlannerOnlyFilters());

    node.step = std::make_unique<FilterStep>(input_header, std::move(new_actions_dag), new_filter->result_name, /*remove_filter=*/true);
}

}
