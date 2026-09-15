#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsBool enable_adaptive_aggregator;
    extern const QueryPlanSerializationSettingsUInt64 adaptive_aggregator_freeze_threshold;
    extern const QueryPlanSerializationSettingsUInt64 adaptive_aggregator_freeze_threshold_bytes;
}
}

using namespace DB;

/// `enable_adaptive_aggregator` and `adaptive_aggregator_freeze_threshold` may go on the wire only towards a peer
/// whose query-plan serialization version knows the names.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `writeChangedBinary` writes every touched entry by
/// name and `readBinary` throws on a name it does not know. Writing either name towards a peer that predates it
/// would make every serialized aggregation plan unreadable there, so under `serialize_query_plan = 1` a mixed-version
/// cluster would stop aggregating altogether. Leaving them off costs nothing: such a peer has no adaptive
/// aggregation to drive, and the adaptive path is exact, so the result is the same either way.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ADAPTIVE_AGGREGATOR - 1;

QueryPlanSerializationSettings serializeAggregatingStep(bool enable_adaptive_aggregator, UInt64 version)
{
    auto type = std::make_shared<DataTypeUInt64>();
    Block header({ColumnWithTypeAndName(type->createColumn(), type, "k")});

    Aggregator::Params params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/false,
        /*enable_packed_string_keys=*/true);
    params.enable_adaptive_aggregator = enable_adaptive_aggregator;
    params.adaptive_aggregator_freeze_threshold = 4096;
    params.adaptive_aggregator_freeze_threshold_bytes = 4096;

    AggregatingStep step(
        std::make_shared<const Block>(header),
        std::move(params),
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
    return settings;
}

/// The names as they appear in the binary settings stream written by `writeChangedBinary`.
bool wireCarries(const QueryPlanSerializationSettings & settings, std::string_view name)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains(name);
}

}

TEST(AdaptiveAggregatorPlanSetting, CarriedTowardsAPeerThatKnowsTheNames)
{
    /// Both values are written for a peer at the current version, whichever way the setting is resolved, so the
    /// initiator's decision - including an admission that turned it off - reaches the remote aggregation.
    for (bool enabled : {false, true})
    {
        const auto settings = serializeAggregatingStep(enabled, current_version);
        EXPECT_TRUE(wireCarries(settings, "enable_adaptive_aggregator")) << "enabled = " << enabled;
        EXPECT_TRUE(wireCarries(settings, "adaptive_aggregator_freeze_threshold")) << "enabled = " << enabled;
        EXPECT_TRUE(wireCarries(settings, "adaptive_aggregator_freeze_threshold_bytes")) << "enabled = " << enabled;
    }
}

TEST(AdaptiveAggregatorPlanSetting, NotCarriedTowardsAPeerThatPredatesTheNames)
{
    /// Neither name may appear towards an older peer - it would reject the whole plan.
    for (bool enabled : {false, true})
    {
        const auto settings = serializeAggregatingStep(enabled, pre_setting_version);
        EXPECT_FALSE(wireCarries(settings, "enable_adaptive_aggregator")) << "enabled = " << enabled;
        EXPECT_FALSE(wireCarries(settings, "adaptive_aggregator_freeze_threshold")) << "enabled = " << enabled;
        EXPECT_FALSE(wireCarries(settings, "adaptive_aggregator_freeze_threshold_bytes")) << "enabled = " << enabled;
    }
}

TEST(AdaptiveAggregatorPlanSetting, DefaultsToPreFeatureBehaviorWhenAbsent)
{
    /// A new worker reading a version-6 plan does not receive the adaptive settings at all. Its
    /// defaults must preserve the behavior of the old initiator instead of enabling a strategy
    /// the initiator could not have selected.
    QueryPlanSerializationSettings settings;
    EXPECT_FALSE(settings[QueryPlanSerializationSetting::enable_adaptive_aggregator]);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::adaptive_aggregator_freeze_threshold], 0);
    EXPECT_EQ(settings[QueryPlanSerializationSetting::adaptive_aggregator_freeze_threshold_bytes], 0);
}
