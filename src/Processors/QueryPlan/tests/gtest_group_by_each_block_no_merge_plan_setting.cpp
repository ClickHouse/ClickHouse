#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsBool group_by_each_block_no_merge;
}
}

using namespace DB;

/// `group_by_each_block_no_merge` goes on the wire only when the mode is on, as an `IMPORTANT` setting.
///
/// A reader of `QueryPlanSerializationSettings` skips a name it does not know, unless the setting is
/// `IMPORTANT`. A peer that predates the name would otherwise skip it and silently run a fully merged
/// aggregation instead of the requested per-block flush. With the `IMPORTANT` flag such a peer rejects the
/// plan. A plan with the mode off does not carry the name at all, so such a peer reads it as before.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

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
        /*enable_packed_string_keys=*/true,
        /*enable_fixed_and_packed_string_keys=*/true);
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

/// The settings stream as written by `QueryPlanSerializationSettings::write`.
std::string serializeAggregatingStep(bool group_by_each_block_no_merge)
{
    const auto step = makeAggregatingStep(group_by_each_block_no_merge);
    QueryPlanSerializationSettings settings;
    step->serializeSettings(settings, current_version);

    WriteBufferFromOwnString out;
    settings.write(out);
    return out.str();
}

/// The value the executing node sees after reading the stream into a fresh settings object, as
/// `QueryPlan::deserialize` does per step.
bool roundTrip(const std::string & wire)
{
    ReadBufferFromString in(wire);
    QueryPlanSerializationSettings read;
    read.read(in);
    return read[QueryPlanSerializationSetting::group_by_each_block_no_merge];
}

}

TEST(GroupByEachBlockNoMergePlanSetting, EnabledIsCarriedAsAnImportantSetting)
{
    const auto wire = serializeAggregatingStep(/*group_by_each_block_no_merge=*/true);
    EXPECT_TRUE(wire.contains("group_by_each_block_no_merge"));
    EXPECT_TRUE(roundTrip(wire));

    /// The flags follow the name on the wire. The `IMPORTANT` flag (`0x01`) is what makes a peer that
    /// predates the name reject the plan instead of skipping the name.
    const std::string name = "group_by_each_block_no_merge";
    const auto pos = wire.find(name);
    ASSERT_NE(pos, std::string::npos);
    ASSERT_LT(pos + name.size(), wire.size());
    EXPECT_EQ(wire[pos + name.size()], '\x01');
}

TEST(GroupByEachBlockNoMergePlanSetting, DisabledIsNotCarried)
{
    /// The name does not appear, so a peer that predates it reads the plan as before, and the default of a
    /// peer that knows it keeps the mode off, which is what the initiator asked for.
    const auto wire = serializeAggregatingStep(/*group_by_each_block_no_merge=*/false);
    EXPECT_FALSE(wire.contains("group_by_each_block_no_merge"));
    EXPECT_FALSE(roundTrip(wire));
}

TEST(GroupByEachBlockNoMergePlanSetting, DefaultsToOffWhenAbsent)
{
    /// A new worker reading a plan from an older initiator does not receive the setting at all and
    /// must run the ordinary, fully merged aggregation.
    QueryPlanSerializationSettings settings;
    EXPECT_FALSE(settings[QueryPlanSerializationSetting::group_by_each_block_no_merge]);
}
