#pragma once

#include <optional>

#include <DataTypes/IDataType_fwd.h>
#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Processors/QueryPlan/RuntimeFilterTypes.h>
#include <base/types.h>

namespace DB::QueryPlanOptimizations
{

enum class RuntimeFilterPlanKind : UInt8
{
    Skip,
    Membership,
    MembershipWithMinMax,
    MinMaxOnly,
};

enum class RuntimeFilterPlanReason : UInt8
{
    MembershipRequired,
    ExactPathExpected,
    MissingKeyStatistics,
    UnreliableKeyStatistics,
    SaturationCheckDisabled,
    EstimatedBloomFilterUsable,
    EstimatedBloomFilterSaturated,
};

struct RuntimeFilterKeyEstimate
{
    std::optional<UInt64> estimated_distinct_keys;
    bool has_key_ndv_statistics = false;
    bool build_subtree_contains_filter_step = false;
};

RuntimeFilterKeyEstimate
makeRuntimeFilterKeyEstimate(std::optional<UInt64> estimated_rows, std::optional<UInt64> key_ndv, bool build_subtree_contains_filter_step);

struct RuntimeFilterPlanningPolicy
{
    UInt64 exact_values_limit;
    RuntimeBloomFilterParameters bloom;
    Float64 max_estimated_set_bits_ratio;
    bool use_minmax;
};

struct RuntimeFilterPlan
{
    RuntimeFilterPlanKind kind;
    std::optional<Float64> estimated_bloom_set_bits_ratio;
    RuntimeFilterPlanReason reason;
};

RuntimeFilterPlan chooseRuntimeFilterPlan(
    const DataTypePtr & common_key_type,
    RuntimeFilterPolarity polarity,
    const RuntimeFilterKeyEstimate & estimate,
    const RuntimeFilterPlanningPolicy & policy);

}
