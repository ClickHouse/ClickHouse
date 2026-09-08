#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
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
    extern const QueryPlanSerializationSettingsString temporary_files_codec;
    extern const QueryPlanSerializationSettingsBool spill_codec_authorized;
    extern const QueryPlanSerializationSettingsNonZeroUInt64 temporary_files_buffer_size;
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

Aggregator::Params makeParams(
    bool serialize_string_with_zero_byte,
    const String & temporary_files_codec = "LZ4",
    bool spill_codec_authorized = false,
    size_t temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
{
    return Aggregator::Params(
        Names{"k"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_rows_to_group_by=*/0,
        OverflowMode::THROW,
        /*group_by_two_level_threshold=*/1,
        /*group_by_two_level_threshold_bytes=*/0,
        /*max_bytes_before_external_group_by=*/1,
        /*empty_result_for_aggregation_by_empty_set=*/false,
        /*tmp_data_scope=*/nullptr,
        temporary_files_codec,
        spill_codec_authorized,
        temporary_files_buffer_size,
        /*max_threads=*/1,
        /*min_free_disk_space=*/0,
        /*compile_aggregate_expressions=*/false,
        /*min_count_to_compile_aggregate_expression=*/0,
        /*max_block_size=*/65536,
        /*enable_prefetch=*/false,
        /*only_merge=*/false,
        /*optimize_group_by_constant_keys=*/false,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation=*/false,
        serialize_string_with_zero_byte,
        /*enable_parallel_single_level_merge=*/false,
        /*enable_packed_string_keys=*/true,
        /*enable_adaptive_aggregator=*/false,
        /*adaptive_aggregator_freeze_threshold=*/0,
        /*adaptive_aggregator_freeze_threshold_bytes=*/0);
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

std::unique_ptr<AggregatingStep> makeAggregatingStep(
    bool serialize_string_with_zero_byte,
    const String & temporary_files_codec = "LZ4",
    bool spill_codec_authorized = false,
    size_t temporary_files_buffer_size = DBMS_DEFAULT_BUFFER_SIZE)
{
    return makeAggregatingStepFromParams(
        makeParams(serialize_string_with_zero_byte, temporary_files_codec, spill_codec_authorized, temporary_files_buffer_size));
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

TEST(AggregatingStepSettingsRoundTrip, SpillSettingsSurviveWithoutInitiatorTemporaryStorage)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const auto step = makeAggregatingStep(
        /*serialize_string_with_zero_byte=*/false,
        /*temporary_files_codec=*/"ZXC",
        /*spill_codec_authorized=*/true,
        /*temporary_files_buffer_size=*/123456);
    QueryPlanSerializationSettings written;
    step->serializeSettings(written, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);
    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in);

    EXPECT_EQ(read[QueryPlanSerializationSetting::temporary_files_codec].value, "ZXC");
    EXPECT_TRUE(read[QueryPlanSerializationSetting::spill_codec_authorized]);
    EXPECT_EQ(read[QueryPlanSerializationSetting::temporary_files_buffer_size], 123456);
}

TEST(AggregatingStepSettingsRoundTrip, ExperimentalSpillCodecOptInIsVersioned)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const auto step = makeAggregatingStep(
        /*serialize_string_with_zero_byte=*/false,
        /*temporary_files_codec=*/"ZXC",
        /*spill_codec_authorized=*/true);
    /// A peer that cannot know the setting name is not told: it predates the `temporary_files_codec` gate,
    /// so it never looks for an opt-in and would reject the unknown name.
    QueryPlanSerializationSettings old_peer_settings;
    step->serializeSettings(old_peer_settings, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXPERIMENTAL_SPILL_CODEC - 1);
    WriteBufferFromOwnString old_peer_out;
    old_peer_settings.writeChangedBinary(old_peer_out);
    EXPECT_FALSE(old_peer_out.str().contains("spill_codec_authorized"));

    QueryPlanSerializationSettings settings;
    step->serializeSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    EXPECT_TRUE(out.str().contains("spill_codec_authorized"));
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
        DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_ONLY_MERGE_AGGREGATION - 1, false};
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
