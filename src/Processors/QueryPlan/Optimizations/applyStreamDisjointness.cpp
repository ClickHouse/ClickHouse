#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/useDataParallelAggregation.h>

#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB::QueryPlanOptimizations
{

struct StreamDisjointnessProperty
{
    const KeyDescription * partition_key = nullptr;
    /// This is the composition of the pass-through expressions between the storage and the current node.
    std::optional<ActionsDAG> column_actions;
};

static void appendExpression(std::optional<ActionsDAG> & dag, const ActionsDAG & expression)
{
    if (dag)
        dag->mergeInplace(expression.clone());
    else
        dag = expression.clone();
}

static bool partitionDeterminedByKeys(const StreamDisjointnessProperty & property, const Names & keys)
{
    return property.partition_key && property.column_actions
        && isPartitionKeyFunctionOfKeys(*property.partition_key, *property.column_actions, keys);
}

static StreamDisjointnessProperty applyStreamDisjointness(
    QueryPlan::Node * node, StreamDisjointnessProperty * children_properties, const QueryPlanOptimizationSettings & settings)
{
    auto * step = node->step.get();

    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        if (reading->willOutputEachPartitionThroughSeparatePort())
            return {&reading->getStorageMetadata()->getPartitionKey(), std::nullopt};
        return {};
    }

    /// Skip multi-child steps (joins, unions, ...) as they do not pass the disjointness property
    /// through.
    if (node->children.size() != 1)
        return {};

    auto property = std::move(children_properties[0]);

    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
    {
        if (property.partition_key)
            appendExpression(property.column_actions, expression->getExpression());
        return property;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(step))
    {
        if (property.partition_key)
            appendExpression(property.column_actions, filter->getExpression());
        return property;
    }

    if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(step))
    {
        /// ARRAY JOIN keeps every output row in its input stream and does not change the partition
        /// columns, so disjointness survives.
        if (property.partition_key && property.column_actions)
        {
            const auto & cols = array_join->getColumns();
            property.column_actions->removeFromOutputs(NameSet(cols.begin(), cols.end()));
        }
        return property;
    }

    if (auto * distinct = typeid_cast<DistinctStep *>(step))
    {
        /// Preliminary DISTINCT is already parallel and there is no merge of streams.
        if (distinct->isPreliminary())
            return property;

        if (settings.distinct_partitions_independently && partitionDeterminedByKeys(property, distinct->getColumnNames()))
        {
            distinct->skipStreamMerging();
            return property;
        }

        /// Otherwise the final DISTINCT merges to a single stream and is a barrier.
        return {};
    }

    if (auto * limit_by = typeid_cast<LimitByStep *>(step))
    {
        if (settings.limit_by_partitions_independently && partitionDeterminedByKeys(property, limit_by->getColumns()))
        {
            limit_by->skipStreamMerging();
            return property;
        }

        /// Otherwise the LIMIT merges to a single stream and is a barrier.
        return {};
    }

    if (auto * aggregating = typeid_cast<AggregatingStep *>(step))
    {
        if (settings.aggregate_partitions_independently && !aggregating->isGroupingSets()
            && partitionDeterminedByKeys(property, aggregating->getParams().keys))
        {
            aggregating->skipMerging();
        }

        /// TODO (nihalzp): For Sharded Aggregation, we can consider this a source and propagate here.
        return {};
    }

    /// TODO (nihalzp): A SortingStep with a window-function PARTITION BY scatters rows by those columns
    /// (ScatterByPartitionTransform / PartitionedFinishSorting), so its output streams are disjoint by the
    /// PARTITION BY columns. We can consider it a source and propagate here.
    return {};
}

void applyStreamDisjointness(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    Stack stack;
    stack.push_back({.node = &root});

    std::vector<StreamDisjointnessProperty> properties;

    while (!stack.empty())
    {
        auto & frame = stack.back();

        /// Traverse all children first.
        if (frame.next_child < frame.node->children.size())
        {
            auto next_frame = Frame{.node = frame.node->children[frame.next_child]};
            ++frame.next_child;
            stack.push_back(next_frame);
            continue;
        }

        auto * node = frame.node;
        stack.pop_back();

        auto it = properties.begin() + (properties.size() - node->children.size());
        auto * children_properties = (it == properties.end()) ? nullptr : &*it;
        auto property = applyStreamDisjointness(node, children_properties, optimization_settings);
        properties.erase(it, properties.end());
        properties.push_back(std::move(property));
    }
}

}
