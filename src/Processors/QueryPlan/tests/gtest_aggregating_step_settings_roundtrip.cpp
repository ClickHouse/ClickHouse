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
bool roundTripSerializeStringWithZeroByte(const IQueryPlanStep & step)
{
    QueryPlanSerializationSettings written;
    step.serializeSettings(written, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);

    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in);

    return read[QueryPlanSerializationSetting::serialize_string_in_memory_with_zero_byte];
}

/// The name as it appears in the binary settings stream written by `writeChangedBinary`. A peer that predates the
/// setting rejects any plan carrying it (`readBinary` throws on an unknown name), so a plan must only carry it when
/// the value cannot be left at the receiver's default.
bool wireCarriesSerializeStringWithZeroByte(const IQueryPlanStep & step)
{
    QueryPlanSerializationSettings written;
    step.serializeSettings(written, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);

    return out.str().find("serialize_string_in_memory_with_zero_byte") != std::string::npos;
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
        /*explicit_sorting_required_for_aggregation_in_order=*/false,
        /*enable_sharding_aggregator=*/false);
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

/// Regression tests for `serialize_string_in_memory_with_zero_byte` being dropped from the
/// serialized query plan (https://github.com/ClickHouse/ClickHouse/issues/112079).
///
/// Only settings that `serializeSettings` assigns are put on the wire: `writeChangedBinary`
/// emits the changed subset, and `readBinary` leaves an absent name at its declared default,
/// which for this setting is `true`. So a step that reads the setting on deserialization but
/// never writes it on serialization silently forces the executing node to `true` whatever the
/// initiator ran with. The `false` direction is therefore the one that diverges.

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

/// Patch releases inside one query-plan serialization version disagree on whether they know this name
/// (`v25.8.12.129-lts` does not, `v25.8.13.73-lts` does, both at version 0), so the version cannot gate it and the
/// default value must stay off the wire for every receiver.
TEST(AggregatingStepSettingsRoundTrip, DefaultValueStaysOffTheWire)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    EXPECT_FALSE(wireCarriesSerializeStringWithZeroByte(*makeAggregatingStep(true)));
    EXPECT_FALSE(wireCarriesSerializeStringWithZeroByte(*makeMergingAggregatedStep(true)));

    EXPECT_TRUE(wireCarriesSerializeStringWithZeroByte(*makeAggregatingStep(false)));
    EXPECT_TRUE(wireCarriesSerializeStringWithZeroByte(*makeMergingAggregatedStep(false)));
}
