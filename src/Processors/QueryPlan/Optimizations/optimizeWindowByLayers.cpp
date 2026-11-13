#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/useDataParallelAggregation.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Common/logger_useful.h>

namespace DB::QueryPlanOptimizations
{

namespace
{

/// Finds the reading below `node` through the steps that keep every row in its stream, and builds the DAG of
/// these steps, which computes the columns of the sorting input from the columns of the reading.
ReadFromMergeTree * findReading(QueryPlan::Node & node, std::optional<ActionsDAG> & dag)
{
    IQueryPlanStep * step = node.step.get();
    if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
    {
        dag.emplace(reading->getOutputHeader()->getColumnsWithTypeAndName());
        return reading;
    }

    if (node.children.size() != 1)
        return nullptr;

    const ActionsDAG * expression = nullptr;
    std::optional<ActionsDAG> array_join_dag;
    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
        expression = &expression_step->getExpression();
    else if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        expression = &filter_step->getExpression();
    else if (const auto * array_join_step = typeid_cast<const ArrayJoinStep *>(step))
        expression = &array_join_dag.emplace(buildArrayJoinDAG(*array_join_step));
    else
        return nullptr;

    auto * reading = findReading(*node.children.front(), dag);
    if (reading)
        dag->mergeInplace(expression->clone());
    return reading;
}

/// The length of the longest prefix of the primary key which is a deterministic function of `key_names`.
/// Rows with equal values of `key_names` have equal values of this prefix.
size_t primaryKeyPrefixDeterminedByKeys(const ReadFromMergeTree & reading, const ActionsDAG & key_actions, const Names & key_names)
{
    const auto & primary_key = reading.getStorageMetadata()->getPrimaryKey();
    if (!primary_key.expression)
        return 0;

    const auto & primary_key_actions = primary_key.expression->getActionsDAG();

    size_t prefix_size = 0;
    while (prefix_size < primary_key.column_names.size())
    {
        /// Only the columns of the prefix may be required: the rest of the key is not determined by `key_names`.
        Names prefix_names(primary_key.column_names.begin(), primary_key.column_names.begin() + prefix_size + 1);
        auto prefix_actions = ActionsDAG::cloneSubDAG(primary_key_actions.findInOutputs(prefix_names), /*remove_aliases=*/false);
        if (!isPartitionKeyFunctionOfKeys(prefix_actions, prefix_actions.getOutputs(), key_actions, key_names))
            break;
        ++prefix_size;
    }

    return prefix_size;
}

void tryReadWindowByLayers(QueryPlan::Node & node)
{
    if (node.children.size() != 1)
        return;

    auto * sorting = typeid_cast<SortingStep *>(node.step.get());
    if (!sorting || sorting->getType() != SortingStep::Type::FinishSorting || !sorting->hasPartitions()
        || sorting->isSortingForMergeJoin() || sorting->getLimit() != 0 || sorting->hasLimitByHint())
        return;

    std::optional<ActionsDAG> dag;
    auto * reading = findReading(*node.children.front(), dag);
    if (!reading || !reading->getInputOrder())
        return;

    /// The same restrictions as for the join by primary key ranges (`optimizeJoinByShards`).
    if (reading->isQueryWithFinal() || reading->isParallelReadingEnabled() || reading->isParallelReadingFromReplicas()
        || reading->getDistributedReadBucketCount() > 0 || reading->willOutputEachPartitionThroughSeparatePort())
        return;

    const size_t num_layers = reading->getNumStreams();
    if (num_layers <= 1)
        return;

    if (dag->hasStatefulFunctions() || dag->hasNonDeterministic())
        return;

    const size_t prefix_size = primaryKeyPrefixDeterminedByKeys(*reading, *dag, sorting->getPartitionByColumnNames());
    if (prefix_size == 0)
        return;

    auto analysis_result = reading->getOrCreateAnalyzedResult();
    if (!analysis_result->split_parts.layers.empty())
        return;

    const bool in_reverse_order = reading->getInputOrder()->direction < 0;
    analysis_result->split_parts = splitIntersectingPartsRangesIntoLayers(
        analysis_result->parts_with_ranges, num_layers, prefix_size, in_reverse_order, getLogger("optimizeWindowByLayers"));
    analysis_result->split_parts.in_reverse_order = in_reverse_order;

    /// Every layer is merged by itself, without the virtual rows of a merge across all the parts.
    reading->resetVirtualRowConversions();
    sorting->convertToPartitionedFinishSorting();
}

}

void optimizeWindowByLayers(QueryPlan::Node & root)
{
    Stack stack;
    traverseQueryPlan(stack, root, [](QueryPlan::Node & node) { tryReadWindowByLayers(node); });
}

}
