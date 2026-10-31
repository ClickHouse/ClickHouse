#include <Interpreters/Cache/PartialAggregateCacheQueryHash.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <Interpreters/Cache/PartialAggregateCache.h>
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Common/FieldVisitorHash.h>
#include <Common/SipHash.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <algorithm>

namespace DB
{

namespace Setting
{
extern const SettingsBool use_partial_aggregate_cache;
}

namespace
{

template <typename Predicate>
bool nodeHasStep(const QueryPlan::Node * node, const Predicate & predicate)
{
    if (!node)
        return false;

    if (predicate(*node->step))
        return true;

    for (const QueryPlan::Node * child : node->children)
    {
        if (nodeHasStep(child, predicate))
            return true;
    }

    return false;
}

/// `SipHash::update(String)` appends raw bytes without a boundary, so `sum(a), count()` would hash the same as
/// `sum(acount)`. Prefix every string with its length.
void updateHashWithString(SipHash & hash, const String & str)
{
    hash.update(str.size());
    hash.update(str);
}

}

bool planHasArrayJoinStep(const QueryPlan & query_plan)
{
    return nodeHasStep(query_plan.getRootNode(), [](const IQueryPlanStep & step) { return typeid_cast<const ArrayJoinStep *>(&step) != nullptr; });
}

bool planHasGroupByTopK(const QueryPlan & query_plan)
{
    return nodeHasStep(
        query_plan.getRootNode(),
        [](const IQueryPlanStep & step)
        {
            const auto * aggregating_step = typeid_cast<const AggregatingStep *>(&step);
            return aggregating_step && aggregating_step->getParams().top_k.has_value();
        });
}

bool planHasAggregationInOrder(const QueryPlan & query_plan)
{
    return nodeHasStep(
        query_plan.getRootNode(),
        [](const IQueryPlanStep & step)
        {
            const auto * aggregating_step = typeid_cast<const AggregatingStep *>(&step);
            return aggregating_step && aggregating_step->inOrder();
        });
}

std::optional<IASTHash> computePartialAggregateCacheQueryHash(
    const PartialAggregateCachePtr & cache,
    const Aggregator::Params & params,
    bool group_by_use_nulls,
    bool has_sort_description_for_merging,
    const Names * grouping_set_missing_keys,
    std::optional<size_t> grouping_set_index)
{
    if (has_sort_description_for_merging || !cache)
        return std::nullopt;

    /// The GROUP BY top-K heap (`enable_group_by_top_k_optimization`) skips the keys of a part that are beyond
    /// the boundary derived from the rows of other parts, so the state of a part depends on the whole part set
    /// and cannot be keyed by the part identity alone. See `planHasGroupByTopK`.
    if (params.top_k)
        return std::nullopt;

    const bool partial_cache_is_compatible_with_group_by_limits
        = params.max_rows_to_group_by == 0 || params.group_by_overflow_mode == OverflowMode::THROW;
    if (!partial_cache_is_compatible_with_group_by_limits)
        return std::nullopt;

    const UInt64 semantic_query_key = params.query_semantic_hash_for_partial_cache;
    if (semantic_query_key == 0)
        return std::nullopt;

    const bool grouping_set_tail = grouping_set_missing_keys != nullptr || grouping_set_index.has_value();
    if (grouping_set_tail && (grouping_set_missing_keys == nullptr || !grouping_set_index.has_value()))
        return std::nullopt;

    SipHash hash;
    hash.update(semantic_query_key);
    hash.update(static_cast<UInt8>(group_by_use_nulls));
    hash.update(static_cast<UInt8>(params.serialize_string_with_zero_byte));
    hash.update(static_cast<UInt8>(params.overflow_row));
    hash.update(static_cast<UInt8>(params.group_by_overflow_mode));
    hash.update(params.max_rows_to_group_by);
    hash.update(params.keys.size());
    for (const auto & key : params.keys)
        updateHashWithString(hash, key);
    hash.update(params.aggregates.size());
    for (const auto & aggregate : params.aggregates)
    {
        updateHashWithString(hash, aggregate.function->getName());
        hash.update(aggregate.parameters.size());
        for (const auto & parameter : aggregate.parameters)
            applyVisitor(FieldVisitorHash(hash), parameter);
        hash.update(aggregate.argument_names.size());
        for (const auto & arg : aggregate.argument_names)
            updateHashWithString(hash, arg);
    }

    if (grouping_set_missing_keys)
    {
        Names missing_sorted = *grouping_set_missing_keys;
        std::sort(missing_sorted.begin(), missing_sorted.end());
        hash.update(missing_sorted.size());
        for (const auto & k : missing_sorted)
            updateHashWithString(hash, k);
        hash.update(static_cast<UInt64>(*grouping_set_index));
    }

    return getSipHash128AsPair(hash);
}

std::optional<IASTHash> tryComputePartialAggregateCacheQueryHash(
    const Settings & settings,
    const PartialAggregateCachePtr & cache,
    const Aggregator::Params & params,
    bool group_by_use_nulls,
    bool has_sort_description_for_merging)
{
    if (!settings[Setting::use_partial_aggregate_cache])
        return std::nullopt;
    return computePartialAggregateCacheQueryHash(
        cache,
        params,
        group_by_use_nulls,
        has_sort_description_for_merging,
        nullptr,
        std::nullopt);
}

}
