#include <Processors/QueryPlan/Optimizations/Optimizations.h>

#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitByStep.h>

#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/Optimizations/useDataParallelAggregation.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/KeyDescription.h>

using namespace DB;

namespace
{


/// `ExpressionStep`, `FilterStep` and `ArrayJoinStep` all preserve the number of streams and keep
/// each output row within its input stream, so the partition each row belongs to does not change.
/// They are the transparent steps the optimization can look through to reach the reading.
ReadFromMergeTree * findReadingStep(QueryPlan::Node & node)
{
    auto * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        return reading;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step) || typeid_cast<ArrayJoinStep *>(step))
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
    if (!typeid_cast<const ExpressionStep *>(step) && !typeid_cast<const FilterStep *>(step)
        && !typeid_cast<const ArrayJoinStep *>(step))
        return;

    buildKeyDAG(*node.children.front(), dag);

    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
        appendExpression(dag, expression->getExpression());
    else if (const auto * filter = typeid_cast<const FilterStep *>(step))
        appendExpression(dag, filter->getExpression());
    else if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(step))
        appendExpression(dag, DB::QueryPlanOptimizations::buildArrayJoinDAG(*array_join));
}

}

namespace DB::QueryPlanOptimizations
{

/// 0. Partition key columns should be a subset of the key columns.
/// 1. Optimization is applicable if partition by expression is a deterministic function of col1, ..., coln and the keys are injective functions of these col1, ..., coln.
/// 2. To find col1, ..., coln we apply removeInjectiveFunctionsFromResultsRecursively to the key actions.
/// 3. We match partition key actions with the key actions to find col1', ..., coln' in partition key actions.
/// 4. We check that partition key is indeed a deterministic function of col1', ..., coln'.
bool isPartitionKeyFunctionOfKeys(
    const ActionsDAG & partition_actions, const Names & partition_key_columns, const ActionsDAG & key_actions, const Names & key_names)
{
    if (key_actions.hasStatefulFunctions() || key_actions.hasNonDeterministic())
        return false;

    /// We are interested only in calculations required to obtain the keys (and not aggregate function arguments for example).
    auto key_nodes = key_actions.findInOutputs(key_names);
    auto key_dag = ActionsDAG::cloneSubDAG(key_nodes, /*remove_aliases=*/true);

    const auto & key_required_columns = key_dag.getRequiredColumnsNames();

    /// Check that PK columns is a subset of key columns.
    for (const auto & col : partition_actions.getRequiredColumnsNames())
        if (std::ranges::find(key_required_columns, col) == key_required_columns.end())
            return false;

    auto irreducible_nodes = removeInjectiveFunctionsFromResultsRecursively(key_dag);

    /// An `ARRAY_JOIN` node (see `buildArrayJoinDAG`) is an exploded array element, and the partition
    /// key must not be matched through it: equal exploded values may come from unrelated source rows
    /// lying in different partitions. E.g. with `PARTITION BY length(arr)` and the key being the element
    /// of `ARRAY JOIN arr`, the arrays `[1]` and `[1, 2]` lie in different partitions but both produce
    /// the value `1`. The other key columns remain usable: with `PARTITION BY k % 8` and keys `(k, x)`
    /// where `x` is exploded, equal key tuples still imply equal `k` and hence one partition.
    std::erase_if(irreducible_nodes, [](const ActionsDAG::Node * node) { return node->type == ActionsDAG::ActionType::ARRAY_JOIN; });

    const auto matches = matchTrees(key_dag.getOutputs(), partition_actions);

    /// `partition_actions.getOutputs()` contains both the partition key columns and source columns.
    /// For example, if `PARTITION BY toYYYYMM(date)`, then `getOutputs() = [toYYYYMM(date), date]`. The `date` column is a source
    /// column but not a key value, and should be excluded from checks. We need to find the actual partition key output
    /// nodes to check that they depend only on the allowed set of nodes (`irreducible_nodes`).
    const auto partition_key_outputs = partition_actions.findInOutputs(partition_key_columns);

    return allOutputsDependsOnlyOnAllowedNodes(partition_key_outputs, irreducible_nodes, matches);
}

bool isPartitionKeyFunctionOfKeys(const KeyDescription & partition_key, const ActionsDAG & key_actions, const Names & key_names)
{
    return isPartitionKeyFunctionOfKeys(
        partition_key.expression->getActionsDAG(), partition_key.column_names, key_actions, key_names);
}

ActionsDAG buildArrayJoinDAG(const ArrayJoinStep & array_join)
{
    const auto & columns = array_join.getColumns();
    const NameSet joined(columns.begin(), columns.end());

    ActionsDAG dag(array_join.getInputHeaders().front()->getColumnsWithTypeAndName());
    for (auto & output : dag.getOutputs())
        if (joined.contains(output->result_name))
            output = &dag.addArrayJoin(*output, output->result_name);
    return dag;
}

void optimizeAggregationPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    if (node.children.size() != 1)
        return;

    auto * aggregating_step = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating_step)
        return;

    if (aggregating_step->isGroupingSets())
        return;

    /// `max_rows_to_group_by` is a global `GROUP BY` limit, enforced during the merge phase in normal
    /// aggregation. Aggregating each partition independently skips that merge, so the limit would be
    /// enforced against each partition's own hash table instead of globally, letting a query return
    /// more groups (or apply `group_by_overflow_mode`) per partition than the global limit allows.
    /// Fall back to normal aggregation, matching `make_distributed_plan`, which rejects this
    /// case for the same reason.
    if (aggregating_step->getParams().max_rows_to_group_by != 0)
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
        && isPartitionKeyFunctionOfKeys(
            reading->getStorageMetadata()->getPartitionKey(), expression_step->getExpression(), aggregating_step->getParams().keys))
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

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeyFunctionOfKeys(reading->getStorageMetadata()->getPartitionKey(), *dag, limit_by_step->getColumns()))
    {
        if (reading->requestOutputEachPartitionThroughSeparatePortForLimitBy())
            limit_by_step->skipStreamMerging();
    }
}

void optimizeDistinctPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    if (node.children.size() != 1)
        return;

    auto * distinct_step = typeid_cast<DistinctStep *>(node.step.get());
    if (!distinct_step)
        return;

    /// Trigger only on the preliminary DISTINCT, which sits close to the reading and is where we
    /// request per-partition reading. The final DISTINCT is handled separately: once the reading is set to
    /// keep each partition within a single stream, `applyStreamDisjointness` propagates that disjointness up the
    /// plan and makes the final DISTINCT skip the cross-stream merge, as long as no intermediate step breaks the
    /// disjointness property.
    if (!distinct_step->isPreliminary())
        return;

    auto * reading = findReadingStep(*node.children.front());
    if (!reading)
        return;

    std::optional<ActionsDAG> dag;
    buildKeyDAG(*node.children.front(), dag);
    if (!dag)
        return;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeyFunctionOfKeys(reading->getStorageMetadata()->getPartitionKey(), *dag, distinct_step->getColumnNames()))
    {
        reading->requestOutputEachPartitionThroughSeparatePortForDistinct();
    }
}

void optimizeWindowPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    if (node.children.size() != 1)
        return;

    auto * sorting_step = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting_step)
        return;

    /// Trigger only on a window-function sorting: a full sort that scatters the input by the hash of the
    /// window `PARTITION BY` columns. A merge-join sorting may also scatter by the sort key
    /// (`convertToScatteredFullSort`), but there both join sides must be sharded by the same hash, so it
    /// must be left alone. This pass runs after `optimizeReadInOrder`, which may have converted the
    /// sorting to `FinishSorting` (see `query_plan_reuse_storage_ordering_for_window_functions`); such a
    /// sorting merges to a single stream and is not matched here.
    if (sorting_step->getType() != SortingStep::Type::Full || !sorting_step->hasPartitions()
        || sorting_step->isSortingForMergeJoin())
        return;

    /// `max_rows_to_sort` / `max_bytes_to_sort` are enforced per stream by the checking transforms of
    /// `fullSortStreams`, so which rows land in which stream is user-visible. Skipping the scatter
    /// would regroup the streams by table partition and could fail a query that passes with the
    /// scatter, so the scatter is kept and per-partition reading has nothing to serve.
    const auto & size_limits = sorting_step->getSettings().size_limits;
    if (size_limits.max_rows != 0 || size_limits.max_bytes != 0)
        return;

    auto * reading = findReadingStep(*node.children.front());
    if (!reading)
        return;

    const Names key_names = sorting_step->getPartitionByColumnNames();

    std::optional<ActionsDAG> dag;
    buildKeyDAG(*node.children.front(), dag);
    if (!dag)
        return;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeyFunctionOfKeys(reading->getStorageMetadata()->getPartitionKey(), *dag, key_names))
    {
        reading->requestOutputEachPartitionThroughSeparatePortForWindow();
    }
}

void optimizeCreatingSetPerPartition(QueryPlan::Node & node, QueryPlan::Nodes &, const QueryPlanOptimizationSettings & /*optimization_settings*/)
{
    if (node.children.size() != 1)
        return;

    auto * creating_set_step = typeid_cast<CreatingSetStep *>(node.step.get());
    if (!creating_set_step)
        return;

    /// GLOBAL IN: the set-filling transform also copies every consumed row into an external temporary
    /// table under the `max_{rows,bytes}_to_transfer` limits. Pre-deduplication would change the table
    /// contents and what those limits count.
    if (creating_set_step->usesExternalTable())
        return;

    auto & child = *node.children.front();
    auto * reading = findReadingStep(child);
    if (!reading)
        return;

    const Names key_names = child.step->getOutputHeader()->getNames();

    std::optional<ActionsDAG> dag;
    buildKeyDAG(child, dag);
    if (!dag)
        return;

    if (!reading->willOutputEachPartitionThroughSeparatePort()
        && isPartitionKeyFunctionOfKeys(reading->getStorageMetadata()->getPartitionKey(), *dag, key_names))
    {
        reading->requestOutputEachPartitionThroughSeparatePortForCreatingSet();
    }
}
}
