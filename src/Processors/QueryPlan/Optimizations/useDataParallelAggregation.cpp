#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/IFunction.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/ExpressionActions.h>

#include <unordered_map>

using namespace DB;

namespace
{

struct Frame
{
    const ActionsDAG::Node * node = nullptr;
    size_t next_child = 0;
};

/// 0. Partition key columns should be a subset of group by key columns.
/// 1. Optimization is applicable if partition by expression is a deterministic function of col1, ..., coln and group by key is injective functions of these col1, ..., coln.
/// 2. To find col1, ..., coln we apply removeInjectiveFunctionsFromResultsRecursively to group by key actions.
/// 3. We match partition key actions with group by key actions to find col1', ..., coln' in partition key actions.
/// 4. We check that partition key is indeed a deterministic function of col1', ..., coln'.
bool isPartitionKeySuitsGroupByKey(
    const ReadFromMergeTree & reading, const ActionsDAG & group_by_actions, const AggregatingStep & aggregating)
{
    if (aggregating.isGroupingSets())
        return false;

    if (group_by_actions.hasArrayJoin() || group_by_actions.hasStatefulFunctions() || group_by_actions.hasNonDeterministic())
        return false;

    /// We are interested only in calculations required to obtain group by keys (and not aggregate function arguments for example).
    auto key_nodes = group_by_actions.findInOutputs(aggregating.getParams().keys);
    auto group_by_key_actions = ActionsDAG::cloneSubDAG(key_nodes, /*remove_aliases=*/ true);

    const auto & gb_key_required_columns = group_by_key_actions.getRequiredColumnsNames();

    const auto & partition_key = reading.getStorageMetadata()->getPartitionKey();
    const auto & partition_actions = partition_key.expression->getActionsDAG();

    /// Check that PK columns is a subset of GBK columns.
    for (const auto & col : partition_actions.getRequiredColumnsNames())
        if (std::ranges::find(gb_key_required_columns, col) == gb_key_required_columns.end())
            return false;

    const auto irreducibe_nodes = removeInjectiveFunctionsFromResultsRecursively(group_by_key_actions);

    const auto matches = matchTrees(group_by_key_actions.getOutputs(), partition_actions);

    /// `partition_actions.getOutputs()` contains both the partition key columns and source columns.
    /// For example, if `PARTITION BY toYYYYMM(date)`, then `getOutputs() = [toYYYYMM(date), date]`. The `date` column is a source
    /// column but not a key value, and should be excluded from checks. We need to find the actual partition key output
    /// nodes to check that they depend only on the allowed set of nodes (`irreducible_nodes`).
    const auto partition_key_outputs = partition_actions.findInOutputs(partition_key.column_names);

    return allOutputsDependsOnlyOnAllowedNodes(partition_key_outputs, irreducibe_nodes, matches);
}
}

namespace DB::QueryPlanOptimizations
{

size_t tryAggregatePartitionsIndependently(QueryPlan::Node * node, QueryPlan::Nodes &, const Optimization::ExtraSettings & /*settings*/)
{
    if (!node || node->children.size() != 1)
        return 0;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node->step.get());
    if (!aggregating_step)
        return 0;

    const auto * expression_node = node->children.front();
    const auto * expression_step = typeid_cast<const ExpressionStep *>(expression_node->step.get());
    if (!expression_step)
        return 0;

    auto * maybe_reading_step = expression_node->children.front()->step.get();

    if (const auto * /*filter*/ _ = typeid_cast<const FilterStep *>(maybe_reading_step))
    {
        const auto * filter_node = expression_node->children.front();
        if (filter_node->children.size() != 1 || !filter_node->children.front()->step)
            return 0;
        maybe_reading_step = filter_node->children.front()->step.get();
    }

    auto * reading = typeid_cast<ReadFromMergeTree *>(maybe_reading_step);
    if (!reading)
        return 0;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeySuitsGroupByKey(*reading, expression_step->getExpression(), *aggregating_step))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePort())
            aggregating_step->skipMerging();
    }

    return 0;
}

}
