#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

using namespace DB;

/// `enable_packed_string_keys_in_aggregation` must reach the wire whenever it can take effect on a peer that knows
/// it, and only when correctness demands it on a peer that does not.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `readBinary` throws on a name it does not know, so a
/// peer that predates the setting rejects any plan carrying it. Towards such a peer (a query-plan serialization
/// version below `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PACKED_STRING_KEYS_SETTING`), emitting it for a
/// `count()` or `GROUP BY UInt64` aggregation - where the single-`String` method is unreachable - would break the
/// peer for nothing. A peer at that version or above always receives the value when the legacy method is requested,
/// so the setting cannot silently stop taking effect on remote aggregation.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PACKED_STRING_KEYS_SETTING - 1;

Aggregator::Params makeParams(const Names & keys, bool enable_packed_string_keys, size_t group_by_two_level_threshold = 100000)
{
    Aggregator::Params params(
        keys,
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/false,
        enable_packed_string_keys);
    params.group_by_two_level_threshold = group_by_two_level_threshold;
    return params;
}

/// The name as it appears in the binary settings stream written by `writeChangedBinary`.
bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains("enable_packed_string_keys_in_aggregation");
}

bool aggregatingStepCarriesSetting(
    const Block & header,
    const Names & keys,
    bool enable_packed_string_keys,
    UInt64 version,
    size_t group_by_two_level_threshold = 100000)
{
    AggregatingStep step(
        std::make_shared<const Block>(header),
        makeParams(keys, enable_packed_string_keys, group_by_two_level_threshold),
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

bool mergingAggregatedStepCarriesSetting(const Block & header, const Names & keys, bool enable_packed_string_keys, UInt64 version)
{
    MergingAggregatedStep step(
        std::make_shared<const Block>(header),
        /// The planner builds merge params with the short `Params` constructor, which leaves both two-level
        /// thresholds at `0` - the bucket layout of this step's input is not knowable from them.
        makeParams(keys, enable_packed_string_keys, /*group_by_two_level_threshold=*/0),
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

Block headerWithKey(const DataTypePtr & type)
{
    return Block({ColumnWithTypeAndName(type->createColumn(), type, "k")});
}

}

TEST(PackedStringKeysPlanSetting, EmittedOnlyForSingleStringKeyTowardsOldPeers)
{
    const Block string_key = headerWithKey(std::make_shared<DataTypeString>());

    /// The only case that has to be communicated to a peer that does not know the name: the initiator asks for the
    /// legacy method for a key the packed method would otherwise be chosen for, and the plan can go two-level.
    EXPECT_TRUE(aggregatingStepCarriesSetting(string_key, {"k"}, false, pre_setting_version));
    EXPECT_TRUE(mergingAggregatedStepCarriesSetting(string_key, {"k"}, false, pre_setting_version));

    /// The default needs nothing on the wire - the receiver's default is the packed method already.
    EXPECT_FALSE(aggregatingStepCarriesSetting(string_key, {"k"}, true, pre_setting_version));
    EXPECT_FALSE(mergingAggregatedStepCarriesSetting(string_key, {"k"}, true, pre_setting_version));
    EXPECT_FALSE(aggregatingStepCarriesSetting(string_key, {"k"}, true, current_version));
    EXPECT_FALSE(mergingAggregatedStepCarriesSetting(string_key, {"k"}, true, current_version));
}

TEST(PackedStringKeysPlanSetting, AlwaysEmittedWhenDisabledTowardsCurrentPeers)
{
    const Block string_key = headerWithKey(std::make_shared<DataTypeString>());

    /// A peer at the current version knows the name, so the value is written whenever the legacy method is
    /// requested - even for plans where it changes nothing - and the setting always takes effect remotely.
    EXPECT_TRUE(aggregatingStepCarriesSetting(string_key, {"k"}, false, current_version));
    EXPECT_TRUE(mergingAggregatedStepCarriesSetting(string_key, {"k"}, false, current_version));

    /// In particular with both two-level thresholds at `0`, where an old peer would not receive it.
    EXPECT_TRUE(aggregatingStepCarriesSetting(string_key, {"k"}, false, current_version, /*group_by_two_level_threshold=*/0));
    EXPECT_TRUE(aggregatingStepCarriesSetting(
        headerWithKey(std::make_shared<DataTypeUInt64>()), {"k"}, false, current_version));
}

TEST(PackedStringKeysPlanSetting, NotEmittedTowardsOldPeersWhenTwoLevelAggregationIsImpossible)
{
    const Block string_key = headerWithKey(std::make_shared<DataTypeString>());

    /// With both serialized two-level thresholds at `0` the receiver can never produce a two-level state, so a method
    /// mismatch is unobservable and an old peer may safely run the plan with its default method.
    EXPECT_FALSE(aggregatingStepCarriesSetting(string_key, {"k"}, false, pre_setting_version, /*group_by_two_level_threshold=*/0));

    /// `MergingAggregatedStep` has no such narrowing: whether its *input* is bucketed is a property of the producing
    /// steps' thresholds, which its own merge params do not carry (see `MergingAggregatedStep::serializeSettings`).
    EXPECT_TRUE(mergingAggregatedStepCarriesSetting(string_key, {"k"}, false, pre_setting_version));
}

TEST(PackedStringKeysPlanSetting, NotEmittedTowardsOldPeersWhenTheMethodIsUnreachable)
{
    /// `count()`: no keys at all.
    EXPECT_FALSE(aggregatingStepCarriesSetting(headerWithKey(std::make_shared<DataTypeUInt64>()), {}, false, pre_setting_version));

    /// A single non-`String` key.
    EXPECT_FALSE(aggregatingStepCarriesSetting(headerWithKey(std::make_shared<DataTypeUInt64>()), {"k"}, false, pre_setting_version));
    EXPECT_FALSE(mergingAggregatedStepCarriesSetting(headerWithKey(std::make_shared<DataTypeUInt64>()), {"k"}, false, pre_setting_version));

    /// `Nullable(String)` and `LowCardinality(String)` get their own methods.
    const auto nullable_string = makeNullable(std::make_shared<DataTypeString>());
    EXPECT_FALSE(aggregatingStepCarriesSetting(headerWithKey(nullable_string), {"k"}, false, pre_setting_version));

    const auto low_cardinality_string = std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>());
    EXPECT_FALSE(aggregatingStepCarriesSetting(headerWithKey(low_cardinality_string), {"k"}, false, pre_setting_version));

    /// More than one key - the packed method is single-key only.
    auto string_type = std::make_shared<DataTypeString>();
    Block two_keys({
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "k"),
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "k2")});
    EXPECT_FALSE(aggregatingStepCarriesSetting(two_keys, {"k", "k2"}, false, pre_setting_version));
}

TEST(PackedStringKeysPlanSetting, EmittedWhenAnyGroupingSetUsesASingleStringKey)
{
    auto string_type = std::make_shared<DataTypeString>();
    auto number_type = std::make_shared<DataTypeUInt64>();
    Block header({
        ColumnWithTypeAndName(string_type->createColumn(), string_type, "s"),
        ColumnWithTypeAndName(number_type->createColumn(), number_type, "n")});

    /// `GROUPING SETS ((s), (n))`: the first set aggregates by a single `String` key.
    const GroupingSetsParamsList with_string_set = {GroupingSetsParams({"s"}, {"n"}), GroupingSetsParams({"n"}, {"s"})};
    EXPECT_TRUE(aggregationCanUsePackedStringKeys(header, {"s", "n"}, with_string_set));

    /// `GROUPING SETS ((n), (s, n))`: no set is a single `String` key.
    const GroupingSetsParamsList without_string_set = {GroupingSetsParams({"n"}, {"s"}), GroupingSetsParams({"s", "n"}, {})};
    EXPECT_FALSE(aggregationCanUsePackedStringKeys(header, {"s", "n"}, without_string_set));
}

TEST(PackedStringKeysPlanSetting, EmittedWhenAKeyIsNotInTheHeader)
{
    /// Fail closed: an unresolvable key must not silently leave the receiver on the other method.
    EXPECT_TRUE(aggregationCanUsePackedStringKeys(headerWithKey(std::make_shared<DataTypeUInt64>()), {"absent"}, {}));
}
