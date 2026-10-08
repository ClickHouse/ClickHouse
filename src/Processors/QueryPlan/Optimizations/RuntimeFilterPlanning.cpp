#include <Processors/QueryPlan/Optimizations/RuntimeFilterPlanning.h>

#include <algorithm>

#include <Processors/QueryPlan/RuntimeFilterLookup.h>

namespace DB::QueryPlanOptimizations
{

RuntimeFilterKeyEstimate
makeRuntimeFilterKeyEstimate(std::optional<UInt64> estimated_rows, std::optional<UInt64> key_ndv, bool build_subtree_contains_filter_step)
{
    const bool has_key_ndv_statistics = key_ndv && *key_ndv > 0;
    if (has_key_ndv_statistics)
        estimated_rows = std::min(estimated_rows.value_or(*key_ndv), *key_ndv);

    return RuntimeFilterKeyEstimate{estimated_rows, has_key_ndv_statistics, build_subtree_contains_filter_step};
}

RuntimeFilterPlan chooseRuntimeFilterPlan(
    const DataTypePtr & common_key_type,
    RuntimeFilterPolarity polarity,
    const RuntimeFilterKeyEstimate & estimate,
    const RuntimeFilterPlanningPolicy & policy)
{
    const bool positive = polarity == RuntimeFilterPolarity::Contains;
    const bool can_use_minmax = positive && policy.use_minmax && supportsNumericMinMaxRuntimeFilter(common_key_type);
    const auto membership_kind = can_use_minmax ? RuntimeFilterPlanKind::MembershipWithMinMax : RuntimeFilterPlanKind::Membership;

    if (!positive || !AdaptiveSetRuntimeFilter::isDataTypeSupported(common_key_type))
        return {membership_kind, std::nullopt, RuntimeFilterPlanReason::MembershipRequired};

    if (estimate.estimated_distinct_keys && *estimate.estimated_distinct_keys <= policy.exact_values_limit)
        return {membership_kind, std::nullopt, RuntimeFilterPlanReason::ExactPathExpected};

    if (policy.max_estimated_set_bits_ratio >= 1.0)
        return {membership_kind, std::nullopt, RuntimeFilterPlanReason::SaturationCheckDisabled};

    if (!estimate.has_key_ndv_statistics || !estimate.estimated_distinct_keys)
        return {membership_kind, std::nullopt, RuntimeFilterPlanReason::MissingKeyStatistics};

    const Float64 estimated_ratio
        = estimateRuntimeBloomFilterSetBitsRatio(static_cast<Float64>(*estimate.estimated_distinct_keys), policy.bloom);

    if (estimate.build_subtree_contains_filter_step)
        return {membership_kind, estimated_ratio, RuntimeFilterPlanReason::UnreliableKeyStatistics};

    if (estimated_ratio <= policy.max_estimated_set_bits_ratio)
        return {membership_kind, estimated_ratio, RuntimeFilterPlanReason::EstimatedBloomFilterUsable};

    return {
        can_use_minmax ? RuntimeFilterPlanKind::MinMaxOnly : RuntimeFilterPlanKind::Skip,
        estimated_ratio,
        RuntimeFilterPlanReason::EstimatedBloomFilterSaturated};
}

}
