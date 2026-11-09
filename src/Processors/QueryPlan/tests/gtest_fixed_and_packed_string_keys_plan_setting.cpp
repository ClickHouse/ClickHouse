#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/RollupStep.h>

using namespace DB;

/// `enable_fixed_and_packed_string_keys_in_aggregation` defaults to `false` in
/// `QueryPlanSerializationSettings`: the serialized method, which an initiator that predates the setting
/// uses for fixed-width keys and one `String` key. So a peer that knows the name must receive `true`
/// whenever an aggregation step can choose one of the `keys*_packed_string` methods. Otherwise it would
/// aggregate with the serialized method and split keys over different two-level buckets.
///
/// A peer that predates the name rejects any plan carrying it. A peer below the setting's version throws on
/// any unknown name, and a peer at that version that does not know the name yet throws because the setting is
/// declared `IMPORTANT`. A peer below the version gets the name only where its serialized method could
/// corrupt the distributed merge, so it fails closed there and nowhere else.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_FIXED_AND_PACKED_STRING_KEYS_SETTING - 1;

Aggregator::Params makeParams(const Names & keys, bool enable_fixed_and_packed_string_keys, size_t group_by_two_level_threshold = 100000)
{
    Aggregator::Params params(
        keys,
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/false,
        /*enable_packed_string_keys=*/true,
        enable_fixed_and_packed_string_keys);
    params.group_by_two_level_threshold = group_by_two_level_threshold;
    return params;
}

/// Whether the name appears in the settings stream written by `QueryPlanSerializationSettings::write`.
bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.write(out);
    return out.str().contains("enable_fixed_and_packed_string_keys_in_aggregation");
}

bool aggregatingStepCarriesSetting(
    const Block & header,
    const Names & keys,
    bool enable_fixed_and_packed_string_keys,
    UInt64 version,
    size_t group_by_two_level_threshold = 100000)
{
    AggregatingStep step(
        std::make_shared<const Block>(header),
        makeParams(keys, enable_fixed_and_packed_string_keys, group_by_two_level_threshold),
        GroupingSetsParamsList{},
        /*final=*/true,
        /*max_block_size=*/65536,
        /*aggregation_in_order_max_block_bytes=*/0,
        /*merge_threads=*/1,
        /*temporary_data_merge_threads=*/1,
        /*storage_has_evenly_distributed_read=*/false,
        /*group_by_use_nulls=*/false,
        /*sort_description_for_merging=*/SortDescription{},
        /*group_by_sort_description=*/SortDescription{},
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false,
        /*explicit_sorting_required_for_aggregation_in_order=*/false);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return wireCarriesSetting(settings);
}

bool mergingAggregatedStepCarriesSetting(const Block & header, const Names & keys, bool enable_fixed_and_packed_string_keys, UInt64 version)
{
    MergingAggregatedStep step(
        std::make_shared<const Block>(header),
        /// The planner builds merge params with the short `Params` constructor, which leaves both two-level
        /// thresholds at `0`: they say nothing about the bucket layout of this step's input.
        makeParams(keys, enable_fixed_and_packed_string_keys, /*group_by_two_level_threshold=*/0),
        GroupingSetsParamsList{},
        /*final=*/true,
        /*memory_efficient_aggregation=*/false,
        /*memory_efficient_merge_threads=*/1,
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*max_block_size=*/65536,
        /*memory_bound_merging_max_block_bytes=*/0,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return wireCarriesSetting(settings);
}

template <typename Step>
bool rollupLikeStepCarriesSetting(const Block & header, const Names & keys, bool enable_fixed_and_packed_string_keys, UInt64 version)
{
    Step step(
        std::make_shared<const Block>(header),
        makeParams(keys, enable_fixed_and_packed_string_keys, /*group_by_two_level_threshold=*/0),
        /*final=*/true,
        /*use_nulls=*/false);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return wireCarriesSetting(settings);
}

Block makeHeader(const std::vector<std::pair<String, DataTypePtr>> & columns)
{
    ColumnsWithTypeAndName result;
    for (const auto & [name, type] : columns)
        result.emplace_back(type->createColumn(), type, name);
    return Block(std::move(result));
}

DataTypePtr stringType()
{
    return std::make_shared<DataTypeString>();
}

DataTypePtr uint64Type()
{
    return std::make_shared<DataTypeUInt64>();
}

/// `UInt64` and `String`: the `keys64_packed_string` method.
Block numberAndString()
{
    return makeHeader({{"k", uint64Type()}, {"s", stringType()}});
}

}

TEST(FixedAndPackedStringKeysPlanSetting, EmittedTowardsCurrentPeersWhenTheMethodCanBeChosen)
{
    const Block header = numberAndString();
    EXPECT_TRUE(aggregatingStepCarriesSetting(header, {"k", "s"}, true, current_version));
    EXPECT_TRUE(mergingAggregatedStepCarriesSetting(header, {"k", "s"}, true, current_version));

    /// Also with both two-level thresholds at `0`, where an old peer would not receive it.
    EXPECT_TRUE(aggregatingStepCarriesSetting(header, {"k", "s"}, true, current_version, /*group_by_two_level_threshold=*/0));

    /// The `keys128_packed_string` method: 12 bytes of fixed-width values.
    const Block wider = makeHeader({{"k", uint64Type()}, {"n", std::make_shared<DataTypeUInt32>()}, {"s", stringType()}});
    EXPECT_TRUE(aggregatingStepCarriesSetting(wider, {"k", "n", "s"}, true, current_version));

    /// The `keys32_packed_string` method: 4 bytes of fixed-width values.
    const Block narrower = makeHeader({{"n", std::make_shared<DataTypeUInt32>()}, {"s", stringType()}});
    EXPECT_TRUE(aggregatingStepCarriesSetting(narrower, {"n", "s"}, true, current_version));
}

TEST(FixedAndPackedStringKeysPlanSetting, NotEmittedWhenDisabled)
{
    /// The receiver's default is the serialized method already.
    const Block header = numberAndString();
    for (const UInt64 version : {current_version, pre_setting_version})
    {
        EXPECT_FALSE(aggregatingStepCarriesSetting(header, {"k", "s"}, false, version));
        EXPECT_FALSE(mergingAggregatedStepCarriesSetting(header, {"k", "s"}, false, version));
    }
}

TEST(FixedAndPackedStringKeysPlanSetting, NotEmittedWhenTheMethodIsUnreachable)
{
    const Block header = numberAndString();
    for (const UInt64 version : {current_version, pre_setting_version})
    {
        /// `count()`: no keys at all.
        EXPECT_FALSE(aggregatingStepCarriesSetting(header, {}, true, version));

        /// A lone `String` key, which has the packed method.
        EXPECT_FALSE(aggregatingStepCarriesSetting(header, {"s"}, true, version));
        EXPECT_FALSE(mergingAggregatedStepCarriesSetting(header, {"s"}, true, version));
    }
}

TEST(FixedAndPackedStringKeysPlanSetting, EmittedTowardsOldPeersOnlyWhereTheMergeCouldMixMethods)
{
    const Block header = numberAndString();

    /// The old peer rejects the plan: its serialized method could go two-level and number the buckets
    /// differently.
    EXPECT_TRUE(aggregatingStepCarriesSetting(header, {"k", "s"}, true, pre_setting_version));

    /// With both serialized two-level thresholds at `0` the receiver never produces a two-level state, so its
    /// single-level blocks merge correctly and it may run the plan with its serialized method.
    EXPECT_FALSE(aggregatingStepCarriesSetting(header, {"k", "s"}, true, pre_setting_version, /*group_by_two_level_threshold=*/0));

    /// `MergingAggregatedStep` has no such narrowing: whether its input is bucketed depends on the
    /// thresholds of the producing steps, which its own merge params do not carry (see
    /// `MergingAggregatedStep::serializeSettings`).
    EXPECT_TRUE(mergingAggregatedStepCarriesSetting(header, {"k", "s"}, true, pre_setting_version));
}

TEST(FixedAndPackedStringKeysPlanSetting, RollupAndCubeCarryTheValueTowardsPeersThatKnowIt)
{
    const Block header = numberAndString();
    for (const bool enabled : {true, false})
    {
        EXPECT_TRUE(rollupLikeStepCarriesSetting<RollupStep>(header, {"k", "s"}, enabled, current_version));
        EXPECT_TRUE(rollupLikeStepCarriesSetting<CubeStep>(header, {"k", "s"}, enabled, current_version));

        /// An older peer merges with its serialized method, as does the aggregation that feeds the step
        /// there.
        EXPECT_FALSE(rollupLikeStepCarriesSetting<RollupStep>(header, {"k", "s"}, enabled, pre_setting_version));
        EXPECT_FALSE(rollupLikeStepCarriesSetting<CubeStep>(header, {"k", "s"}, enabled, pre_setting_version));
    }
}
