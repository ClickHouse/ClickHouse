#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/SelectQueryInfo.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace QueryPlanOptimizations
{

namespace
{

/// Walk down from the aggregation through `Expression`, `Filter` and `CreatingSets` steps to a
/// `ReadFromMergeTree`, merging the actions on the way into one DAG that maps the read columns to the
/// aggregation input.
ReadFromMergeTree * findReadingStep(QueryPlan::Node & node, ActionsDAG & dag)
{
    std::vector<const IQueryPlanStep *> chain;
    QueryPlan::Node * current = &node;

    while (true)
    {
        if (current->children.empty())
            return nullptr;

        current = current->children.front();
        IQueryPlanStep * step = current->step.get();

        if (typeid_cast<CreatingSetsStep *>(step) || typeid_cast<DelayedCreatingSetsStep *>(step))
            continue;

        if (current->children.size() > 1)
            return nullptr;

        if (auto * reading = typeid_cast<ReadFromMergeTree *>(step))
        {
            if (reading->isQueryWithFinal() || reading->isParallelReadingEnabled()
                || reading->willOutputEachPartitionThroughSeparatePort())
                return nullptr;

            if (const auto & prewhere_info = reading->getPrewhereInfo())
                dag = prewhere_info->prewhere_actions.clone();
            else
                dag = ActionsDAG(reading->getOutputHeader()->getColumnsWithTypeAndName());

            for (auto it = chain.rbegin(); it != chain.rend(); ++it)
            {
                if (const auto * expression = typeid_cast<const ExpressionStep *>(*it))
                    dag.mergeInplace(expression->getExpression().clone());
                else if (const auto * filter = typeid_cast<const FilterStep *>(*it))
                    dag.mergeInplace(filter->getExpression().clone());
            }

            return reading;
        }

        if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        {
            chain.push_back(step);
            continue;
        }

        return nullptr;
    }
}

/// The number of leading primary key columns that the aggregation keys contain as plain columns.
/// A key wrapped into a function, even a monotonic one, ends the prefix: the layers split the raw
/// key values, and one function value could span two layers.
size_t findAggregatedPrimaryKeyPrefix(const AggregatingStep & aggregating, const ReadFromMergeTree & reading, const ActionsDAG & dag)
{
    const auto & primary_key = reading.getStorageMetadata()->getPrimaryKey();
    if (primary_key.column_names.empty())
        return 0;

    std::unordered_map<std::string_view, const ActionsDAG::Node *> outputs;
    for (const auto & output : dag.getOutputs())
        outputs.emplace(output->result_name, output);

    const auto & pk_dag = primary_key.expression->getActionsDAG();
    auto matches = matchTrees(pk_dag.getOutputs(), dag, false);

    const auto & keys = aggregating.getParams().keys;
    size_t prefix = 0;

    for (; prefix < primary_key.column_names.size(); ++prefix)
    {
        if (!isSafePrimaryDataKeyType(*primary_key.data_types[prefix]))
            break;

        if (prefix < primary_key.reverse_flags.size() && primary_key.reverse_flags[prefix])
            break;

        const auto * pk_output = pk_dag.tryFindInOutputs(primary_key.column_names[prefix]);
        if (!pk_output)
            break;

        bool found = false;
        for (const auto & key : keys)
        {
            auto it = outputs.find(key);
            if (it == outputs.end())
                continue;

            auto match = matches.find(it->second);
            if (match == matches.end() || match->second.monotonicity)
                continue;

            if (match->second.node == pk_output)
            {
                found = true;
                break;
            }
        }

        if (!found)
            break;
    }

    return prefix;
}

void tryApply(QueryPlan::Node & node)
{
    auto * aggregating = typeid_cast<AggregatingStep *>(node.step.get());
    if (!aggregating)
        return;

    const auto & params = aggregating->getParams();
    if (!aggregating->inOrder() || aggregating->isMergingSkipped() || aggregating->explicitSortingRequired()
        || aggregating->isGroupingSets() || aggregating->memoryBoundMergingWillBeUsed()
        || params.overflow_row)
        return;

    ActionsDAG dag;
    auto * reading = findReadingStep(node, dag);
    if (!reading || reading->getNumStreams() < 2)
        return;

    const auto & input_order_info = reading->getInputOrder();
    if (!input_order_info || input_order_info->direction != 1)
        return;

    auto analysis_result = reading->getAnalyzedResult();
    if (!analysis_result)
        analysis_result = reading->selectRangesToRead();

    if (!analysis_result || !analysis_result->split_parts.layers.empty())
        return;

    size_t prefix = std::min(
        findAggregatedPrimaryKeyPrefix(*aggregating, *reading, dag), input_order_info->used_prefix_of_sorting_key_size);
    if (prefix == 0)
        return;

    auto logger = getLogger("optimizeAggregationInOrderByLayers");
    auto split = splitIntersectingPartsRangesIntoLayers(
        analysis_result->parts_with_ranges, reading->getNumStreams(), prefix, /*in_reverse_order=*/ false, logger);

    if (split.layers.size() < 2)
        return;

    LOG_TRACE(
        logger,
        "Aggregating in order over {} primary key ranges by a prefix of {} column(s)",
        split.layers.size(),
        prefix);

    analysis_result->split_parts = std::move(split);
    reading->setAnalyzedResult(std::move(analysis_result));
    aggregating->skipMerging();
}

}

void optimizeAggregationInOrderByLayers(QueryPlan::Node & root)
{
    std::vector<QueryPlan::Node *> stack{&root};

    while (!stack.empty())
    {
        auto * node = stack.back();
        stack.pop_back();

        tryApply(*node);

        for (auto * child : node->children)
            stack.push_back(child);
    }
}

}
}
