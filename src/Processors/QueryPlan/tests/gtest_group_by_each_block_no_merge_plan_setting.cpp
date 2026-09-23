#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Common/Exception.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsBool group_by_each_block_no_merge;
}
namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}
}

using namespace DB;

/// `group_by_each_block_no_merge` may go on the wire only towards a peer whose query-plan serialization
/// version knows the name.
///
/// `QueryPlanSerializationSettings` is a strict named schema: `writeChangedBinary` writes every touched entry by
/// name and `readBinary` throws on a name it does not know, and `SettingFieldBool::operator=` marks the field as
/// changed even for `false`. Writing the name towards a peer that predates it would make every serialized
/// aggregation plan unreadable there, even with the feature off. Towards such a peer the name is left off when the
/// mode is off, and an enabled step is refused: the peer would silently run a fully merged aggregation instead.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_GROUP_BY_EACH_BLOCK_NO_MERGE - 1;

std::unique_ptr<AggregatingStep> makeAggregatingStep(bool group_by_each_block_no_merge)
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
    params.group_by_each_block_no_merge = group_by_each_block_no_merge;

    return std::make_unique<AggregatingStep>(
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
}

/// The binary settings stream as written by `writeChangedBinary`.
std::string serializeAggregatingStep(bool group_by_each_block_no_merge, UInt64 version)
{
    const auto step = makeAggregatingStep(group_by_each_block_no_merge);
    QueryPlanSerializationSettings settings;
    step->serializeSettings(settings, version);

    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str();
}

/// The value the executing node sees after reading the stream into a fresh settings object, as
/// `QueryPlan::deserialize` does per step.
bool roundTrip(const std::string & wire)
{
    ReadBufferFromString in(wire);
    QueryPlanSerializationSettings read;
    read.readBinary(in);
    return read[QueryPlanSerializationSetting::group_by_each_block_no_merge];
}

}

TEST(GroupByEachBlockNoMergePlanSetting, RoundTripsTowardsAPeerThatKnowsTheName)
{
    /// Both values are written for a peer at the current version, so the initiator's decision reaches
    /// the remote aggregation either way.
    for (bool enabled : {false, true})
    {
        const auto wire = serializeAggregatingStep(enabled, current_version);
        EXPECT_TRUE(wire.contains("group_by_each_block_no_merge")) << "enabled = " << enabled;
        EXPECT_EQ(roundTrip(wire), enabled) << "enabled = " << enabled;
    }
}

TEST(GroupByEachBlockNoMergePlanSetting, DisabledIsNotCarriedTowardsAPeerThatPredatesTheName)
{
    /// The name must not appear towards an older peer - it would reject the whole plan - and the
    /// peer's default keeps the mode off, which is what the initiator asked for.
    const auto wire = serializeAggregatingStep(/*group_by_each_block_no_merge=*/false, pre_setting_version);
    EXPECT_FALSE(wire.contains("group_by_each_block_no_merge"));
    EXPECT_FALSE(roundTrip(wire));
}

TEST(GroupByEachBlockNoMergePlanSetting, EnabledFailsClosedTowardsAPeerThatPredatesTheName)
{
    /// An older peer cannot run the per-block flush and would silently merge everything instead.
    const auto step = makeAggregatingStep(/*group_by_each_block_no_merge=*/true);
    QueryPlanSerializationSettings settings;
    try
    {
        step->serializeSettings(settings, pre_setting_version);
        FAIL() << "expected NOT_IMPLEMENTED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::NOT_IMPLEMENTED);
    }
}

TEST(GroupByEachBlockNoMergePlanSetting, DefaultsToOffWhenAbsent)
{
    /// A new worker reading a plan from an older initiator does not receive the setting at all and
    /// must run the ordinary, fully merged aggregation.
    QueryPlanSerializationSettings settings;
    EXPECT_FALSE(settings[QueryPlanSerializationSetting::group_by_each_block_no_merge]);
}
