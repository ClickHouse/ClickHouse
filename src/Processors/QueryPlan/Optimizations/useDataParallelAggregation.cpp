#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>

using namespace DB;

namespace
{


ReadFromMergeTree * findReadingStep(QueryPlan::Node & node)
{
    auto * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return reading;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front());

    return nullptr;
}

void appendExpression(std::optional<ActionsDAG> & dag, const ActionsDAG & expression)
{
    if (dag)
        dag->mergeInplace(expression.clone());
    else
        dag = expression.clone();
}

void buildKeyDAG(const QueryPlan::Node & node, std::optional<ActionsDAG> & dag)
{
    if (node.children.size() != 1)
        return;

    auto * step = node.step.get();
    const ActionsDAG * step_dag = nullptr;
    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
        step_dag = &expression->getExpression();
    else if (const auto * filter = typeid_cast<const FilterStep *>(step))
        step_dag = &filter->getExpression();

    if (!step_dag)
        return;

    buildKeyDAG(*node.children.front(), dag);
    appendExpression(dag, *step_dag);
}

/// 0. Partition key columns should be a subset of the key columns.
/// 1. Optimization is applicable if partition by expression is a deterministic function of col1, ..., coln and the keys are injective functions of these col1, ..., coln.
/// 2. To find col1, ..., coln we apply removeInjectiveFunctionsFromResultsRecursively to the key actions.
/// 3. We match partition key actions with the key actions to find col1', ..., coln' in partition key actions.
/// 4. We check that partition key is indeed a deterministic function of col1', ..., coln'.
bool isPartitionKeyFunctionOfKeys(const ReadFromMergeTree & reading, const ActionsDAG & key_actions, const Names & key_names)
{
    if (key_actions.hasArrayJoin() || key_actions.hasStatefulFunctions() || key_actions.hasNonDeterministic())
        return false;

    /// We are interested only in calculations required to obtain the keys (and not aggregate function arguments for example).
    auto key_nodes = key_actions.findInOutputs(key_names);
    auto key_dag = ActionsDAG::cloneSubDAG(key_nodes, /*remove_aliases=*/true);

    const auto & key_required_columns = key_dag.getRequiredColumnsNames();

    const auto & partition_actions = reading.getStorageMetadata()->getPartitionKey().expression->getActionsDAG();

    /// Check that PK columns is a subset of key columns.
    for (const auto & col : partition_actions.getRequiredColumnsNames())
        if (std::ranges::find(key_required_columns, col) == key_required_columns.end())
            return false;

    const auto irreducible_nodes = removeInjectiveFunctionsFromResultsRecursively(key_dag);

    const auto matches = matchTrees(key_dag.getOutputs(), partition_actions);

    return allOutputsDependsOnlyOnAllowedNodes(partition_actions, irreducible_nodes, matches);
}

}

namespace DB::QueryPlanOptimizations
{

void optimizeAggregationPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    if (node.children.size() != 1)
        return;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating_step)
        return;

    if (aggregating_step->isGroupingSets())
        return;

    const auto * expression_node = node.children.front();
    const auto * expression_step = typeid_cast<const ExpressionStep *>(expression_node->step.get());
    if (!expression_step)
        return;

    auto * maybe_reading_step = expression_node->children.front()->step.get();

    if (const auto * /*filter*/ _ = typeid_cast<const FilterStep *>(maybe_reading_step))
    {
        const auto * filter_node = expression_node->children.front();
        if (filter_node->children.size() != 1 || !filter_node->children.front()->step)
            return;
        maybe_reading_step = filter_node->children.front()->step.get();
    }

    auto * reading = typeid_cast<ReadFromMergeTree *>(maybe_reading_step);
    if (!reading)
        return;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeyFunctionOfKeys(*reading, expression_step->getExpression(), aggregating_step->getParams().keys))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePortForAggregation())
            aggregating_step->skipMerging();
    }
}

void optimizeLimitByPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    if (node.children.size() != 1)
        return;

    auto * limit_by_step = typeid_cast<LimitByStep *>(node.step.get());
    if (!limit_by_step)
        return;

    auto * reading = findReadingStep(*node.children.front());
    if (!reading)
        return;

    std::optional<ActionsDAG> dag;
    buildKeyDAG(*node.children.front(), dag);
    if (!dag)
        return;

    if (!reading->willOutputEachPartitionThroughSeparatePort() && isPartitionKeyFunctionOfKeys(*reading, *dag, limit_by_step->getColumns()))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePortForLimitBy())
            limit_by_step->skipStreamMerging();
    }
}
}
