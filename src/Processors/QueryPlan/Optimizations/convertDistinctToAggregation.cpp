#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/Utils.h>

#include <Columns/ColumnConst.h>
#include <Common/assert_cast.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/ExtremesStep.h>
#include <Processors/QueryPlan/FractionalLimitStep.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/ReadFromRecursiveCTEStep.h>
#include <Processors/QueryPlan/ReadFromRemote.h>
#include <Processors/QueryPlan/ReadFromStreamLikeEngine.h>
#include <Processors/QueryPlan/ReadFromSystemNumbersStep.h>
#include <Processors/QueryPlan/ReadFromSystemPrimesStep.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Processors/QueryPlan/TotalsHavingStep.h>
#include <Storages/IStorage.h>

namespace DB::ErrorCodes
{
    extern const int SET_SIZE_LIMIT_EXCEEDED;
}

namespace DB::QueryPlanOptimizations
{

static bool consumesRowsByPosition(const IQueryPlanStep & step)
{
    return typeid_cast<const LimitStep *>(&step) || typeid_cast<const OffsetStep *>(&step)
        || typeid_cast<const LimitByStep *>(&step) || typeid_cast<const NegativeLimitByStep *>(&step)
        || typeid_cast<const NegativeLimitStep *>(&step) || typeid_cast<const NegativeOffsetStep *>(&step)
        || typeid_cast<const FractionalLimitStep *>(&step) || typeid_cast<const FractionalOffsetStep *>(&step)
        || typeid_cast<const LimitRangeStep *>(&step);
}

static bool requiresDistinctPreservation(const IQueryPlanStep & step)
{
    if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(&step))
    {
        const auto & storage = source->getStorageSnapshot()->storage;
        if (source->getQueryInfo().isStream() || storage.isStreamingStorage())
            return true;

        if (const auto * numbers = typeid_cast<const ReadFromSystemNumbersStep *>(&step))
            return !numbers->hasBoundedRead();

        if (const auto * primes = typeid_cast<const ReadFromSystemPrimesStep *>(&step))
            return !primes->hasBoundedRead();

        return !storage.hasBoundedRead();
    }

    /// `DISTINCT` forwards totals and extremes, whereas aggregation discards them.
    if (typeid_cast<const TotalsHavingStep *>(&step) || typeid_cast<const ExtremesStep *>(&step))
        return true;

    if (const auto * source = dynamic_cast<const ReadFromPreparedSource *>(&step))
        return !source->hasBoundedRead() || source->hasTotals() || source->hasExtremes();

    if (const auto * source = typeid_cast<const ReadFromRemote *>(&step))
        return !source->hasBoundedRead() || source->hasTotals() || source->hasExtremes();

    /// Parallel replica reads expose only the main stream.
    if (const auto * source = typeid_cast<const ReadFromParallelRemoteReplicasStep *>(&step))
        return !source->hasBoundedRead();

    return typeid_cast<const ReadFromRecursiveCTEStep *>(&step) || dynamic_cast<const ReadFromStreamLikeEngine *>(&step);
}

std::unordered_set<const QueryPlan::Node *> collectDistinctToAggregationCandidates(const QueryPlan::Node & root)
{
    /// Positional limits constrain their inputs even when no limit hint reaches `DISTINCT`.
    /// Source and auxiliary stream restrictions constrain consumers, so those propagate upwards.
    struct Frame
    {
        const QueryPlan::Node * node;
        size_t next_child = 0;
        bool has_positional_limit = false;
        bool requires_distinct_preservation = false;
    };

    std::unordered_set<const QueryPlan::Node *> candidates;
    std::vector<Frame> stack{{.node = &root}};
    while (!stack.empty())
    {
        auto & frame = stack.back();
        if (frame.next_child == 0)
        {
            frame.has_positional_limit |= consumesRowsByPosition(*frame.node->step);
            frame.requires_distinct_preservation = requiresDistinctPreservation(*frame.node->step);
        }

        if (frame.next_child < frame.node->children.size())
        {
            const auto * child = frame.node->children[frame.next_child++];
            stack.push_back({.node = child, .has_positional_limit = frame.has_positional_limit});
            continue;
        }

        if (!frame.has_positional_limit && !frame.requires_distinct_preservation
            && typeid_cast<const DistinctStep *>(frame.node->step.get()))
            candidates.insert(frame.node);

        const bool requires_distinct_preservation = frame.requires_distinct_preservation;
        stack.pop_back();
        if (!stack.empty())
            stack.back().requires_distinct_preservation |= requires_distinct_preservation;
    }
    return candidates;
}

static bool hasIdenticalDuplicateColumns(const QueryPlan::Node & input)
{
    const auto * node = &input;
    while (typeid_cast<const DistinctStep *>(node->step.get()))
        node = node->children.front();

    const auto * expression = typeid_cast<const ExpressionStep *>(node->step.get());
    return expression && expression->hasIdenticalDuplicateOutputColumns();
}

bool tryConvertDistinctToAggregation(
    QueryPlan::Node & node, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & settings)
{
    const auto & distinct = assert_cast<const DistinctStep &>(*node.step);

    /// A final `DISTINCT` over partition-disjoint streams deduplicates each stream on its own and never
    /// merges them, so it is already parallel and has no single-threaded merge to replace.
    if (distinct.isPreliminary() || distinct.getLimitHint() != 0 || distinct.skipsStreamMerging())
        return false;

    if (!distinct.getSortDescription().empty())
        return false;

    const auto & limits = distinct.getSetSizeLimits();

    /// Aggregation's producers decide where to stop in `break` mode, whereas the final `DISTINCT`
    /// stops against a single global set. Retain that behavior when a stopping bound is configured.
    if (limits.overflow_mode == OverflowMode::BREAK && limits.hasLimits())
        return false;

    const auto header = distinct.getInputHeaders().front();
    const Names & distinct_columns = distinct.getColumnNames();
    const NameSet distinct_keys(distinct_columns.begin(), distinct_columns.end());

    /// Aggregation resolves keys by name and reconstructs their values. Duplicate names are safe
    /// only when the corresponding input columns are known to contain identical values.
    if (header->getNameSet().size() != header->columns() && !hasIdenticalDuplicateColumns(*node.children.front()))
        return false;

    Names keys;
    for (const auto & column : *header)
    {
        if (isColumnConst(*column.column))
            continue;
        if (!distinct_columns.empty() && !distinct_keys.contains(column.name))
            return false;
        keys.push_back(column.name);
    }

    /// With only constant keys, `DISTINCT` stops reading after the first nonempty chunk.
    if (keys.empty())
        return false;

    const bool has_constants = keys.size() != header->columns();

    const auto & aggregation_settings = settings.distinct_aggregation_settings;
    Aggregator::Params params(
        keys,
        AggregateDescriptions{},
        /*overflow_row=*/false,
        limits.max_rows,
        limits.overflow_mode,
        limits.max_bytes,
        {"DISTINCT", ErrorCodes::SET_SIZE_LIMIT_EXCEEDED, ErrorCodes::SET_SIZE_LIMIT_EXCEEDED},
        aggregation_settings.group_by_two_level_threshold,
        aggregation_settings.group_by_two_level_threshold_bytes,
        /*max_bytes_before_external_group_by=*/0,
        /*empty_result_for_aggregation_by_empty_set=*/true,
        /*tmp_data_scope=*/nullptr,
        /*max_threads=*/0,
        /*min_free_disk_space=*/0,
        /*compile_aggregate_expressions=*/false,
        /*min_count_to_compile_aggregate_expression=*/0,
        settings.max_block_size,
        aggregation_settings.enable_prefetch,
        /*only_merge=*/false,
        /*optimize_group_by_constant_keys=*/false,
        aggregation_settings.min_hit_rate_to_use_consecutive_keys_optimization,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation=*/true,
        aggregation_settings.serialize_string_with_zero_byte,
        aggregation_settings.enable_parallel_single_level_merge,
        aggregation_settings.enable_packed_string_keys,
        aggregation_settings.enable_adaptive_aggregator,
        aggregation_settings.adaptive_aggregator_freeze_threshold,
        aggregation_settings.adaptive_aggregator_freeze_threshold_bytes);

    auto aggregation = std::make_unique<AggregatingStep>(
        header,
        std::move(params),
        GroupingSetsParamsList{},
        /*final=*/true,
        settings.max_block_size,
        /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/0,
        /*temporary_data_merge_threads=*/0,
        /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false,
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false);
    aggregation->setStepDescription("DISTINCT");
    node.step = std::move(aggregation);

    /// Constants do not participate in deduplication. Restore their values and positions after
    /// aggregation to preserve the output header, including its constant column representation.
    if (has_constants)
    {
        auto actions = ActionsDAG::makeConvertingActions(
            node.step->getOutputHeader()->getColumnsWithTypeAndName(),
            header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            /*context=*/nullptr,
            /*ignore_constant_values=*/true);
        makeExpressionNodeOnTopOf(node, std::move(actions), nodes);
    }

    chassert(blocksHaveEqualStructure(*node.step->getOutputHeader(), *header));
    return true;
}

}
