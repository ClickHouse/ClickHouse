#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>
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
    /// Equal values of the partition key outputs belong to the same stream. These outputs can be
    /// the table partition key or the identity over hash scatter keys, such as window `PARTITION BY` columns.
    ActionsDAG partition_key_actions;

    /// The composition of expressions between the disjointness source and the current step tracks
    /// column lineage, allowing downstream keys to be related to the original partitioning expression.
    ActionsDAG column_actions;

    /// The per-partition reading that sourced this disjointness. Its partition count and skew affect
    /// the profitability of reusing the streams for a window. Hash partitioning has no associated reading
    /// and chooses its stream count independently of the table partitions.
    const ReadFromMergeTree * reading = nullptr;
};

static StreamDisjointnessProperty makeHashPartitionedProperty(const Block & header, const Names & keys)
{
    ActionsDAG column_actions(header.getColumnsWithTypeAndName());
    auto partition_key_actions = ActionsDAG::cloneSubDAG(column_actions.findInOutputs(keys), /*remove_aliases=*/false);
    return {std::move(partition_key_actions), std::move(column_actions)};
}

static bool partitionDeterminedByKeys(const StreamDisjointnessProperty & property, const Names & keys)
{
    return isPartitionKeyFunctionOfKeys(
        property.partition_key_actions, property.partition_key_actions.getOutputs(), property.column_actions, keys);
}

static std::optional<StreamDisjointnessProperty> applyStreamDisjointness(
    IQueryPlanStep & step, std::optional<StreamDisjointnessProperty> property, const QueryPlanOptimizationSettings & settings)
{
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(&step))
    {
        if (!reading->willOutputEachPartitionThroughSeparatePort())
            return {};

        const auto & partition_key = reading->getStorageMetadata()->getPartitionKey();
        return StreamDisjointnessProperty
        {
            ActionsDAG::cloneSubDAG(
                partition_key.expression->getActionsDAG().findInOutputs(partition_key.column_names), /*remove_aliases=*/false),
            ActionsDAG(reading->getOutputHeader()->getColumnsWithTypeAndName()),
            reading,
        };
    }

    if (auto * distinct = typeid_cast<DistinctStep *>(&step))
    {
        /// Preliminary `DISTINCT` processes each stream independently and does not merge streams.
        if (distinct->isPreliminary())
            return property;

        /// An input-order requirement keeps the final `DISTINCT` on one merged stream: neither a skipped merge
        /// nor hash partitioning may reorder its rows.
        if (distinct->preservesInputOrder())
            return {};

        /// Disjoint inputs can be deduplicated independently. `DistinctStep` enforces size limits on
        /// their combined set while keeping stream assignments intact for downstream consumers.
        if (property && settings.distinct_partitions_independently
            && partitionDeterminedByKeys(*property, distinct->getColumnNames()))
        {
            distinct->skipStreamMerging();
            return property;
        }

        /// Without parallel hash deduplication, the final step merges its inputs into a single stream.
        if (!settings.parallel_distinct || !distinct->getSortDescription().empty())
            return {};

        distinct->enableParallelDistinct();
        const auto & header = *distinct->getInputHeaders().front();
        const auto & keys = distinct->getColumnNames();
        /// Hash partitioning keeps equal keys in one stream. A single-stream result without scattering
        /// satisfies the same property.
        return makeHashPartitionedProperty(header, keys.empty() ? header.getNames() : keys);
    }

    if (auto * sorting = typeid_cast<SortingStep *>(&step))
    {
        /// Window sorting scatters by the `PARTITION BY` columns so each window partition belongs to one
        /// stream. If the window key determines the input partition, each stream already contains whole
        /// window partitions and can be sorted independently without scattering or merging. Sorting within
        /// streams preserves disjointness.
        /// A merge-join sorting also scatters by its sort key (`convertToScatteredFullSort`), but both join
        /// sides must use the same hash partitioning, so their scatters cannot be skipped independently.
        if (sorting->getType() != SortingStep::Type::Full || !sorting->hasPartitions() || sorting->isSortingForMergeJoin())
            return {};

        const auto partition_by_names = sorting->getPartitionByColumnNames();

        /// `max_rows_to_sort` and `max_bytes_to_sort` are enforced per stream by `fullSortStreams`.
        /// Reusing table partitions changes which rows share a stream and can make a query exceed a limit
        /// that it would satisfy after scattering, so size limits prevent this reuse.
        ///
        /// The window's scatter already provides parallelism. Reusing table partitions can cap processing
        /// at a smaller stream count or retain their skew, so it must pass the window's cost heuristic even
        /// when another step requested per-partition reading. Hash scatters choose their stream counts
        /// independently of the table partitions and do not need this reading-specific check.
        if (property && settings.window_partitions_independently && !sorting->getSettings().size_limits.hasLimits()
            && partitionDeterminedByKeys(*property, partition_by_names)
            && (!property->reading || settings.force_window_partitions_independently
                || property->reading->isPartitionIndependentProcessingProfitable(ReadFromMergeTree::ProcessorKind::Window)))
        {
            sorting->skipScatterByPartition();
            return property;
        }

        /// Scattering puts every value of the `PARTITION BY` tuple in one output stream, making this
        /// sorting a new disjointness source with the identity over those columns as its partition key.
        return makeHashPartitionedProperty(*sorting->getInputHeaders().front(), partition_by_names);
    }

    if (!property)
        return {};

    if (const auto * expression = typeid_cast<const ExpressionStep *>(&step))
    {
        property->column_actions.mergeInplace(expression->getExpression().clone());
        return property;
    }

    if (const auto * filter = typeid_cast<const FilterStep *>(&step))
    {
        property->column_actions.mergeInplace(filter->getExpression().clone());
        return property;
    }

    if (const auto * array_join = typeid_cast<const ArrayJoinStep *>(&step))
    {
        /// `ARRAY JOIN` keeps every emitted row in its source row's stream, so disjointness survives.
        /// `buildArrayJoinDAG` records exploded columns as `ARRAY_JOIN` nodes so the key check can reject
        /// dependencies through them, rather than matching an element to its source array of the same name.
        /// Other key columns remain traceable to the partitioning expression.
        property->column_actions.mergeInplace(buildArrayJoinDAG(*array_join));
        return property;
    }

    if (const auto * window = typeid_cast<const WindowStep *>(&step))
    {
        /// Window computation keeps each row within its input stream. After the last window,
        /// `query_plan_enable_multithreading_after_window_functions` can resize to `max_threads`, mixing
        /// streams. A window without a full sort description (`OVER ()`) merges them into a single stream
        /// and prevents downstream steps from reusing their parallelism.
        if (window->hasStreamsFanOut() || window->getWindowDescription().full_sort_description.empty())
            return {};

        /// Window results are new inputs to subsequent expressions, independent of earlier column lineage.
        /// Recording them lets downstream consumers resolve keys that include window result columns.
        for (const auto & function : window->getWindowFunctions())
        {
            const auto & column = window->getOutputHeader()->getByName(function.column_name);
            property->column_actions.addOrReplaceInOutputs(property->column_actions.addInput(column));
        }
        return property;
    }

    if (auto * limit_by = typeid_cast<LimitByStep *>(&step))
    {
        if (settings.limit_by_partitions_independently && partitionDeterminedByKeys(*property, limit_by->getColumns()))
        {
            limit_by->skipStreamMerging();
            return property;
        }

        /// Otherwise `LIMIT BY` merges its inputs into a single stream and ends disjointness propagation.
        return {};
    }

    if (auto * creating_set = typeid_cast<CreatingSetStep *>(&step))
    {
        /// The set is keyed on all columns of its input header. When these keys determine disjoint
        /// streams, independent per-stream deduplication can remove all duplicates before the single set
        /// fill, leaving it only unique rows to hash. This preliminary pass is best-effort: it can abandon
        /// deduplication on mostly-unique input because the set fill always deduplicates its input.
        if (settings.creating_set_partitions_independently && !creating_set->usesExternalTable()
            && partitionDeterminedByKeys(*property, creating_set->getInputHeaders().front()->getNames()))
        {
            creating_set->enablePreliminaryDistinct();
        }

        return {};
    }

    if (auto * aggregating = typeid_cast<AggregatingStep *>(&step))
    {
        /// `max_rows_to_group_by` is a global `GROUP BY` limit, enforced during the merge phase in normal
        /// aggregation. Skipping the merge would enforce it against each stream's own hash table instead
        /// of the combined groups, so the merge must remain when this limit is set.
        if (settings.aggregate_partitions_independently && !aggregating->isGroupingSets()
            && aggregating->getParams().max_rows_to_group_by == 0 && partitionDeterminedByKeys(*property, aggregating->getParams().keys))
        {
            aggregating->skipMerging();
        }
    }

    return {};
}

void applyStreamDisjointness(const QueryPlanOptimizationSettings & optimization_settings, QueryPlan::Node & root)
{
    /// An absent property means that the streams are not known to be disjoint.
    std::optional<StreamDisjointnessProperty> property;
    auto propagate = [&](QueryPlan::Node & node)
    {
        /// Joins and unions do not propagate their inputs' disjointness. Only unary steps inherit it,
        /// so the last visited child's property is sufficient.
        if (node.children.size() != 1)
            property.reset();

        property = applyStreamDisjointness(*node.step, std::move(property), optimization_settings);
    };

    Stack stack;
    /// Visit children first so a unary step receives the property computed for its input.
    traverseQueryPlan(stack, root, NoOp{}, propagate);
}

}
