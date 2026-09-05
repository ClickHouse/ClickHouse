#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Common/tests/gtest_global_register.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsBool serialize_string_in_memory_with_zero_byte;
}
}

using namespace DB;

namespace
{

/// Drive the real production path: `serializeSettings` -> `writeChangedBinary` -> a FRESH
/// settings object -> `readBinary`, exactly as `QueryPlan::serialize` and
/// `QueryPlan::deserialize` do per step. Returns the value the executing node would see.
bool roundTripSerializeStringWithZeroByte(const IQueryPlanStep & step, UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
{
    QueryPlanSerializationSettings written;
    step.serializeSettings(written, version);

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);

    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in);

    return read[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte];
}

/// Whether the name appears in the binary settings stream written by `writeChangedBinary`.
bool wireCarriesSerializeStringWithZeroByte(const IQueryPlanStep & step, UInt64 version)
{
    QueryPlanSerializationSettings written;
    step.serializeSettings(written, version);

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);

    return out.str().contains("serialize_string_in_memory_with_zero_byte");
}

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

Aggregator::Params makeParams(bool serialize_string_with_zero_byte)
{
    /// Merge-only constructor.
    return Aggregator::Params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        serialize_string_with_zero_byte,
        /*enable_packed_string_keys=*/true);
}

std::unique_ptr<AggregatingStep> makeAggregatingStep(bool serialize_string_with_zero_byte)
{
    return std::make_unique<AggregatingStep>(
        makeHeader(),
        makeParams(serialize_string_with_zero_byte),
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

std::unique_ptr<MergingAggregatedStep> makeMergingAggregatedStep(bool serialize_string_with_zero_byte)
{
    return std::make_unique<MergingAggregatedStep>(
        makeHeader(),
        makeParams(serialize_string_with_zero_byte),
        GroupingSetsParamsList{},
        /*final=*/true,
        /*memory_efficient_aggregation=*/false,
        /*memory_efficient_merge_threads=*/1,
        /*should_produce_results_in_order_of_bucket_number=*/false,
        /*max_block_size=*/65536,
        /*memory_bound_merging_max_block_bytes=*/0,
        /*memory_bound_merging_of_aggregation_results_enabled=*/false);
}

}

/// Regression tests for `serialize_string_in_memory_with_zero_byte` being dropped from the serialized
/// query plan (https://github.com/ClickHouse/ClickHouse/issues/112079). A step that reads the setting
/// on deserialization but never writes it on serialization leaves the executing node at the declared
/// default `true` whatever the initiator ran with, so `false` is the direction that diverges.

TEST(AggregatingStepSettingsRoundTrip, SerializeStringWithZeroByteFalseSurvives)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    EXPECT_FALSE(roundTripSerializeStringWithZeroByte(*makeAggregatingStep(false)));
}

TEST(AggregatingStepSettingsRoundTrip, SerializeStringWithZeroByteTrueSurvives)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    EXPECT_TRUE(roundTripSerializeStringWithZeroByte(*makeAggregatingStep(true)));
}

TEST(MergingAggregatedStepSettingsRoundTrip, SerializeStringWithZeroByteFalseSurvives)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    EXPECT_FALSE(roundTripSerializeStringWithZeroByte(*makeMergingAggregatedStep(false)));
}

TEST(MergingAggregatedStepSettingsRoundTrip, SerializeStringWithZeroByteTrueSurvives)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    EXPECT_TRUE(roundTripSerializeStringWithZeroByte(*makeMergingAggregatedStep(true)));
}

/// A receiver predating the name serializes String keys the way `false` does, so the value must reach
/// the receiver, and at every version: `v25.8.12.129-lts` lacks the name while `v25.8.13.73-lts` has it, yet
/// both advertise version 0. A legacy stream names this one unconditionally; the framed format names it only
/// when it differs from the default, and an absent framed entry is reconstructed from that same default.
TEST(AggregatingStepSettingsRoundTrip, BothDirectionsReachTheReceiverAtEveryVersion)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const bool default_value = QueryPlanSerializationSettings{}[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte];
    for (UInt64 version : {UInt64{0}, UInt64{DBMS_QUERY_PLAN_SERIALIZATION_VERSION}})
    {
        const bool framed = version >= DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_OUTLINE;
        for (bool value : {false, true})
        {
            /// A legacy stream names this one unconditionally; the framed format names it only when it
            /// differs from the default. Either way the receiver ends up with the initiator's value:
            /// an absent framed entry is reconstructed from the default the writer also omitted for.
            const bool named_on_wire = !framed || value != default_value;
            EXPECT_EQ(wireCarriesSerializeStringWithZeroByte(*makeAggregatingStep(value), version), named_on_wire)
                << "version " << version << ", value " << value;
            EXPECT_EQ(wireCarriesSerializeStringWithZeroByte(*makeMergingAggregatedStep(value), version), named_on_wire)
                << "version " << version << ", value " << value;

            /// And survive the full write -> read round trip, not merely appear on the wire.
            EXPECT_EQ(roundTripSerializeStringWithZeroByte(*makeAggregatingStep(value), version), value)
                << "version " << version;
            EXPECT_EQ(roundTripSerializeStringWithZeroByte(*makeMergingAggregatedStep(value), version), value)
                << "version " << version;
        }
    }
}
