#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/useDataParallelAggregation.h>

#include <Interpreters/ActionsDAG.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Storages/KeyDescription.h>
#include <Storages/StorageInMemoryMetadata.h>

namespace DB::QueryPlanOptimizations
{

struct StreamDisjointnessProperty
{
    /// The expression whose value determines the stream a row belongs to: the table partition key for
    /// per-partition reading, or the window `PARTITION BY` columns (as an identity expression) for the
    /// hash scatter of a window-function sorting. `std::nullopt` when the streams are not known to be
    /// disjoint.
    std::optional<ActionsDAG> partition_key_actions;
    Names partition_key_columns;

    /// This is the composition of the pass-through expressions between the source and the current node.
    std::optional<ActionsDAG> column_actions;

    /// The per-partition reading that sourced the disjointness; null when the source is a scatter,
    /// whose stream count is already the full pipeline width.
    const ReadFromMergeTree * reading = nullptr;

    bool isDisjoint() const { return partition_key_actions.has_value(); }
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
    return property.isDisjoint() && property.column_actions
        && isPartitionKeyFunctionOfKeys(*property.partition_key_actions, property.partition_key_columns, *property.column_actions, keys);
}

static StreamDisjointnessProperty applyStreamDisjointness(
    QueryPlan::Node * node, StreamDisjointnessProperty * children_properties, const QueryPlanOptimizationSettings & settings)
{
    auto * step = node->step.get();

    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        if (reading->willOutputEachPartitionThroughSeparatePort())
        {
            const auto & partition_key = reading->getStorageMetadata()->getPartitionKey();
            return {partition_key.expression->getActionsDAG().clone(), partition_key.column_names, std::nullopt, reading};
        }
        return {};
    }

    /// Skip multi-child steps (joins, unions, ...) as they do not pass the disjointness property
    /// through.
    if (node->children.size() != 1)
        return {};

    auto property = std::move(children_properties[0]);

    if (const auto * expression = typeid_cast<const ExpressionStep *>(step))
    {
        if (property.isDisjoint())
            appendExpression(property.column_actions, expression->getExpression());
        return property;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(step))
    {
        if (property.isDisjoint())
            appendExpression(property.column_actions, filter->getExpression());
        return property;
    }

    if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(step))
    {
        /// ARRAY JOIN keeps every output row in its input stream and does not change the partition
        /// columns, so disjointness survives. The exploded columns enter the pass-through expressions as
        /// `ARRAY_JOIN` nodes (see `buildArrayJoinDAG`), so a key tracing to one is rejected instead of
        /// being confused with the source array column of the same name.
        if (property.isDisjoint())
            appendExpression(property.column_actions, buildArrayJoinDAG(*array_join));
        return property;
    }

    if (auto * distinct = typeid_cast<DistinctStep *>(step))
    {
        /// Preliminary DISTINCT is already parallel and there is no merge of streams.
        if (distinct->isPreliminary())
            return property;

        /// `max_rows_in_distinct` / `max_bytes_in_distinct` are enforced by the single final transform
        /// that sees the whole merged input. Skipping the merge would turn them into independent
        /// per-stream limits, so in that case we keep the merge and the limits stay global.
        const auto & size_limits = distinct->getSetSizeLimits();
        const bool has_size_limits = size_limits.max_rows != 0 || size_limits.max_bytes != 0;

        if (settings.distinct_partitions_independently && !has_size_limits
            && partitionDeterminedByKeys(property, distinct->getColumnNames()))
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

    if (auto * creating_set = typeid_cast<CreatingSetStep *>(step))
    {
        /// The set is keyed on all columns of its input header. With disjoint input streams, deduplicating
        /// each stream independently is complete deduplication, so the single set-filling transform only
        /// hashes unique rows.
        if (settings.creating_set_partitions_independently && !creating_set->usesExternalTable()
            && partitionDeterminedByKeys(property, step->getInputHeaders().front()->getNames()))
        {
            creating_set->enablePreliminaryDistinct();
        }

        return {};
    }

    if (auto * aggregating = typeid_cast<AggregatingStep *>(step))
    {
        /// `max_rows_to_group_by` is a global `GROUP BY` limit, enforced during the merge phase in
        /// normal aggregation. Skipping the merge would enforce it against each stream's own hash
        /// table instead of globally, so in that case we keep the merge and the limit stays global.
        if (settings.aggregate_partitions_independently && !aggregating->isGroupingSets()
            && aggregating->getParams().max_rows_to_group_by == 0
            && partitionDeterminedByKeys(property, aggregating->getParams().keys))
        {
            aggregating->skipMerging();
        }

        return {};
    }

    if (auto * sorting = typeid_cast<SortingStep *>(step))
    {
        /// A window-function sorting scatters the input by the hash of the window `PARTITION BY` columns
        /// so that whole window partitions land in one stream. With disjoint input streams the scatter is
        /// redundant: every stream already carries whole partitions, and the partitioned sort sorts each
        /// stream independently without merging them back, keeping each partition contiguous and sorted.
        /// Sorting reorders rows only within their stream, so the disjointness survives. A merge-join
        /// sorting also scatters by the sort key (`convertToScatteredFullSort`), but there both join
        /// sides must be sharded by the same hash, so it is left alone.
        if (sorting->getType() == SortingStep::Type::Full && sorting->hasPartitions()
            && !sorting->isSortingForMergeJoin())
        {
            Names partition_by_names = sorting->getPartitionByColumnNames();

            /// `max_rows_to_sort` / `max_bytes_to_sort` are enforced per stream by the checking
            /// transforms of `fullSortStreams`, so which rows land in which stream is user-visible.
            /// Skipping the scatter would regroup the streams by table partition and could fail a query
            /// that passes with the scatter, so in that case the scatter is kept.
            const auto & size_limits = sorting->getSettings().size_limits;
            const bool has_size_limits = size_limits.max_rows != 0 || size_limits.max_bytes != 0;

            /// Unlike the single-stream reductions (final DISTINCT, LIMIT BY, set fill), the scatter's
            /// baseline is parallel: it redistributes the streams across all threads. Skipping it caps
            /// the window processing at the stream count, so when the streams come from per-partition
            /// reading requested by another feature, the skip must pass the same cost heuristic as a
            /// window's own per-partition request. A property sourced by a lower window's scatter has
            /// no reading and needs no check: its stream count is already the full width.
            const bool profitable = !property.reading || settings.force_window_partitions_independently
                || property.reading->isPartitionIndependentProcessingProfitable(ReadFromMergeTree::ProcessorKind::Window);

            if (settings.window_partitions_independently && !has_size_limits && profitable
                && partitionDeterminedByKeys(property, partition_by_names))
            {
                sorting->skipScatterByPartition();
                return property;
            }

            /// The scatter runs: rows are distributed by the hash of the `PARTITION BY` columns, so
            /// every value of that column tuple lands in exactly one output stream. The sorting is
            /// therefore itself a disjointness source, with the identity over the `PARTITION BY`
            /// columns as the partitioning expression.
            ColumnsWithTypeAndName partition_columns;
            partition_columns.reserve(partition_by_names.size());
            for (const auto & name : partition_by_names)
                partition_columns.push_back(step->getInputHeaders().front()->getByName(name));

            return {ActionsDAG(partition_columns), std::move(partition_by_names), std::nullopt};
        }

        /// Any other sorting merges or reshuffles the streams and is a barrier.
        return {};
    }

    if (const auto * window = typeid_cast<const WindowStep *>(step))
    {
        /// The window transform keeps every row within its input stream and only appends the window
        /// function result columns, so disjointness survives. The appended columns are recorded as
        /// pass-through inputs so that a consumer above can resolve keys that reference them. After the
        /// last window the pipeline may be resized back to `max_threads`
        /// (`query_plan_enable_multithreading_after_window_functions`), which mixes the streams.
        if (window->hasStreamsFanOut())
            return {};

        /// A window without a full sort description (`OVER ()`) merges the pipeline to a single stream.
        if (window->getWindowDescription().full_sort_description.empty())
            return {};

        if (property.isDisjoint())
            appendExpression(property.column_actions, ActionsDAG(window->getOutputHeader()->getColumnsWithTypeAndName()));
        return property;
    }

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
