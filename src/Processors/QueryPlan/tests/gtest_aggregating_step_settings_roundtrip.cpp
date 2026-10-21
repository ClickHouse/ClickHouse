#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/MergingAggregatedStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsBool serialize_string_in_memory_with_zero_byte;
    extern const QueryPlanSerializationSettingsBool use_aggregation_memory_tracker;
}
namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCORRECT_DATA;
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

std::unique_ptr<AggregatingStep> makeAggregatingStepFromParams(Aggregator::Params params)
{
    return std::make_unique<AggregatingStep>(
        makeHeader(),
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

std::unique_ptr<AggregatingStep> makeAggregatingStep(bool serialize_string_with_zero_byte)
{
    return makeAggregatingStepFromParams(makeParams(serialize_string_with_zero_byte));
}

/// Serialize a step through the production path and return its byte stream.
String serializeStep(const IQueryPlanStep & step, UInt64 version, bool for_cache_key = false)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    ctx.for_cache_key = for_cache_key;
    step.serialize(ctx);
    return out.str();
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

/// A receiver predating the name serializes String keys the way `false` does, so both values must reach
/// the wire, and at every version: `v25.8.12.129-lts` lacks the name while `v25.8.13.73-lts` has it, yet
/// both advertise version 0.
TEST(AggregatingStepSettingsRoundTrip, BothDirectionsReachTheWireAtEveryVersion)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    for (UInt64 version : {UInt64{0}, UInt64{DBMS_QUERY_PLAN_SERIALIZATION_VERSION}})
    {
        for (bool value : {false, true})
        {
            EXPECT_TRUE(wireCarriesSerializeStringWithZeroByte(*makeAggregatingStep(value), version))
                << "version " << version << ", value " << value;
            EXPECT_TRUE(wireCarriesSerializeStringWithZeroByte(*makeMergingAggregatedStep(value), version))
                << "version " << version << ", value " << value;

            /// And survive the full write -> read round trip, not merely appear on the wire.
            EXPECT_EQ(roundTripSerializeStringWithZeroByte(*makeAggregatingStep(value), version), value)
                << "version " << version;
            EXPECT_EQ(roundTripSerializeStringWithZeroByte(*makeMergingAggregatedStep(value), version), value)
                << "version " << version;
        }
    }
}

/// `use_aggregation_memory_tracker` must travel in the serialized plan: a deserialized `AggregatingStep` takes
/// every other aggregation setting from the plan, so reading this one from the receiver's session would drop
/// a `SETTINGS use_aggregation_memory_tracker = 0` of the initiator
/// (https://github.com/ClickHouse/ClickHouse/issues/123756).
namespace
{

QueryPlanSerializationSettings writeAndReadSettings(const IQueryPlanStep & step, UInt64 version)
{
    QueryPlanSerializationSettings written;
    step.serializeSettings(written, version);

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);

    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in);
    return read;
}

bool deserializedUseAggregationMemoryTracker(
    bool value, UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION, bool receiver_session_value = true)
{
    auto params = makeParams(true);
    params.use_aggregation_memory_tracker = value;
    auto step = makeAggregatingStepFromParams(std::move(params));

    String bytes = serializeStep(*step, version);
    QueryPlanSerializationSettings settings = writeAndReadSettings(*step, version);

    auto receiver_context = Context::createCopy(getContext().context);
    receiver_context->setSetting("use_aggregation_memory_tracker", Field(receiver_session_value));

    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    auto header = makeHeader();
    SharedHeaders input_headers{header};
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, receiver_context, input_headers, header, settings, 0, version, 0, false};

    auto restored = AggregatingStep::deserialize(ctx);
    return typeid_cast<AggregatingStep &>(*restored).getParams().use_aggregation_memory_tracker;
}

}

TEST(AggregatingStepSettingsRoundTrip, UseAggregationMemoryTrackerSurvivesDeserialization)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// The receiver's session keeps the default `true`, so `false` is the direction that diverges.
    EXPECT_FALSE(deserializedUseAggregationMemoryTracker(false));
    EXPECT_TRUE(deserializedUseAggregationMemoryTracker(true));
}

TEST(AggregatingStepSettingsRoundTrip, UseAggregationMemoryTrackerFromReceiverSessionForOlderStreams)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// An older stream does not carry the name, so the receiver's session decides, not the default `true`.
    constexpr UInt64 older_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_AGGREGATION_MEMORY_TRACKER - 1;
    EXPECT_FALSE(deserializedUseAggregationMemoryTracker(true, older_version, /*receiver_session_value=*/false));
    EXPECT_TRUE(deserializedUseAggregationMemoryTracker(false, older_version, /*receiver_session_value=*/true));

    /// A current stream carries the initiator's value, whatever the receiver's session says.
    EXPECT_FALSE(deserializedUseAggregationMemoryTracker(false, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*receiver_session_value=*/true));
    EXPECT_TRUE(deserializedUseAggregationMemoryTracker(true, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*receiver_session_value=*/false));
}

TEST(AggregatingStepSettingsRoundTrip, UseAggregationMemoryTrackerNotWrittenToOlderPeers)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto params = makeParams(true);
    params.use_aggregation_memory_tracker = false;
    auto step = makeAggregatingStepFromParams(std::move(params));

    /// An older peer throws on a name it does not know, so the name must stay off the wire towards it.
    QueryPlanSerializationSettings written;
    step->serializeSettings(written, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_AGGREGATION_MEMORY_TRACKER - 1);
    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);
    EXPECT_FALSE(out.str().contains("use_aggregation_memory_tracker"));

    auto current_peer = writeAndReadSettings(*step, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_AGGREGATION_MEMORY_TRACKER);
    EXPECT_FALSE(current_peer[QueryPlanSerializationSetting::use_aggregation_memory_tracker]);
}

/// Version gates of the `only_merge` flag (bit 128 on `AggregatingStep`, introduced in
/// `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ONLY_MERGE_AGGREGATION`): only a gtest can
/// drive a peer version below it. End-to-end round-trip coverage of the flag itself is carried
/// by the executed pushdown tests (every executed pushed query serializes and deserializes its
/// distributed fragments). The merge-only `Params` constructor used by `makeAggregatingStep`
/// sets `only_merge`, so every step above already carries the flag; `cloneWithKeys` below
/// produces the ordinary twin.

TEST(AggregatingStepOnlyMergeVersionGates, SerializationBelowMinVersionThrows)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto step = makeAggregatingStep(false);
    try
    {
        serializeStep(*step, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ONLY_MERGE_AGGREGATION - 1);
        FAIL() << "expected SUPPORT_IS_DISABLED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }
}

TEST(AggregatingStepOnlyMergeVersionGates, Bit128InStreamBelowMinVersionThrows)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// The exact bytes a current initiator emits, replayed as a stream one version older than
    /// the flag: the bit is garbage there and must be rejected, mirroring the serialize gate.
    auto step = makeAggregatingStep(false);
    String bytes = serializeStep(*step, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    auto header = makeHeader();
    SharedHeaders input_headers{header};
    QueryPlanSerializationSettings settings;
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, getContext().context, input_headers, header, settings, 0,
        DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ONLY_MERGE_AGGREGATION - 1, 0, false};
    try
    {
        AggregatingStep::deserialize(ctx);
        FAIL() << "expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

/// `only_merge` changes how the input columns are interpreted (state columns vs argument
/// columns), so two steps differing only in it must not share a stats/preallocation cache key.
TEST(AggregatingStepOnlyMergeVersionGates, CacheKeySerializationIsolatesOnlyMerge)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    auto merge_only_params = makeParams(false);
    auto ordinary_params = merge_only_params.cloneWithKeys(merge_only_params.keys, /*only_merge_=*/false);
    auto merge_only = makeAggregatingStepFromParams(std::move(merge_only_params));
    auto ordinary = makeAggregatingStepFromParams(std::move(ordinary_params));

    EXPECT_NE(
        serializeStep(*ordinary, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*for_cache_key=*/true),
        serializeStep(*merge_only, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*for_cache_key=*/true));
}
