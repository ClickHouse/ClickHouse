#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB
{
namespace QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 max_bytes_before_external_group_by;
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

/// The input header carries BOTH the aggregate argument column `x` (what an ordinary aggregation
/// reads) and the state column `s` (what a merge-only aggregation reads), so the very same header
/// and the very same `Params` are valid for both `only_merge` values - the two steps under test
/// differ in nothing else. That makes the cache-key test below meaningful: any byte difference
/// between their serializations can come only from the `only_merge` flag.
SharedHeader makeInputHeader()
{
    auto key_type = std::make_shared<DataTypeUInt64>();
    auto arg_type = std::make_shared<DataTypeUInt64>();

    AggregateFunctionProperties properties;
    auto sum = AggregateFunctionFactory::instance().get("sum", NullsAction::EMPTY, DataTypes{arg_type}, {}, properties);
    auto state_type = std::make_shared<DataTypeAggregateFunction>(sum, DataTypes{arg_type}, Array{});

    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(key_type->createColumn(), key_type, "k"),
        ColumnWithTypeAndName(arg_type->createColumn(), arg_type, "x"),
        ColumnWithTypeAndName(state_type->createColumn(), state_type, "s")});
}

AggregateDescriptions makeSumAggregate()
{
    AggregateDescription description;
    AggregateFunctionProperties properties;
    description.function
        = AggregateFunctionFactory::instance().get("sum", NullsAction::EMPTY, DataTypes{std::make_shared<DataTypeUInt64>()}, {}, properties);
    description.argument_names = {"x"};
    description.column_name = "s";
    return {description};
}

/// The FULL `Params` constructor: the merge step synthesized by the aggregation pushdown carries
/// a full copy of the original aggregation's params (spill limits included), never the reduced
/// merge-only constructor.
Aggregator::Params makeParams(bool only_merge, size_t max_bytes_before_external_group_by)
{
    return Aggregator::Params(
        Names{"k"},
        makeSumAggregate(),
        /*overflow_row_=*/false,
        /*max_rows_to_group_by_=*/0,
        OverflowMode::THROW,
        /*group_by_two_level_threshold_=*/0,
        /*group_by_two_level_threshold_bytes_=*/0,
        max_bytes_before_external_group_by,
        /*empty_result_for_aggregation_by_empty_set_=*/false,
        /*tmp_data_scope_=*/nullptr,
        /*max_threads_=*/1,
        /*min_free_disk_space_=*/0,
        /*compile_aggregate_expressions_=*/false,
        /*min_count_to_compile_aggregate_expression_=*/0,
        /*max_block_size_=*/65536,
        /*enable_prefetch_=*/false,
        only_merge,
        /*optimize_group_by_constant_keys_=*/false,
        /*min_hit_rate_to_use_consecutive_keys_optimization_=*/0.5f,
        StatsCollectingParams{},
        /*enable_producing_buckets_out_of_order_in_aggregation_=*/true,
        /*serialize_string_with_zero_byte_=*/false,
        /*enable_parallel_single_level_merge_=*/false,
        /*enable_packed_string_keys_=*/true,
        /*enable_adaptive_aggregator_=*/false,
        /*adaptive_aggregator_freeze_threshold_=*/0,
        /*adaptive_aggregator_freeze_threshold_bytes_=*/0);
}

std::unique_ptr<AggregatingStep> makeStep(bool only_merge, size_t max_bytes_before_external_group_by = 0)
{
    return std::make_unique<AggregatingStep>(
        makeInputHeader(),
        makeParams(only_merge, max_bytes_before_external_group_by),
        GroupingSetsParamsList{},
        /*final_=*/true,
        /*max_block_size_=*/65536,
        /*aggregation_in_order_max_block_bytes_=*/0,
        /*merge_threads_=*/1,
        /*temporary_data_merge_threads_=*/1,
        /*storage_has_evenly_distributed_read_=*/false,
        /*group_by_use_nulls_=*/false,
        SortDescription{},
        SortDescription{},
        /*should_produce_results_in_order_of_bucket_number_=*/false,
        /*memory_bound_merging_of_aggregation_results_enabled_=*/false,
        /*explicit_sorting_required_for_aggregation_in_order_=*/false,
        /*enable_sharding_aggregator_=*/false);
}

/// Serialize a step through the production path and return its byte stream.
String serializeStep(const IQueryPlanStep & step, UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION, bool for_cache_key = false)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    ctx.for_cache_key = for_cache_key;
    step.serialize(ctx);
    return out.str();
}

/// Deserialize a byte stream through the production path. `settings` is what
/// `QueryPlan::deserialize` hands each step: a fresh object filled only from the names the
/// step's `serializeSettings` wrote (a defaulted object unless the caller drives that path).
QueryPlanStepPtr deserializeStep(
    const String & bytes,
    const QueryPlanSerializationSettings & settings,
    UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    auto header = makeInputHeader();
    SharedHeaders input_headers{header};
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{in, registry, {}, context, input_headers, header, settings, 0, version, false};

    return AggregatingStep::deserialize(ctx);
}

UInt8 flagsByte(const String & bytes)
{
    return static_cast<UInt8>(bytes.at(0));
}

const Aggregator::Params & stepParams(const QueryPlanStepPtr & step)
{
    return assert_cast<const AggregatingStep &>(*step).getParams();
}

void registerAll()
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();
    getContext(); /// `AggregatingStep::deserialize` reads the global context for temporary data.
}

}

/// `only_merge` travels as flag bit 128 since query plan serialization version 10: without it a
/// worker deserializing a distributed task fragment would rebuild the pushed-down merge step as a
/// raw aggregation over state columns.

TEST(AggregatingStepOnlyMergeSerialization, OnlyMergeSurvivesRoundTrip)
{
    registerAll();

    auto step = makeStep(/*only_merge=*/true);
    String first = serializeStep(*step);
    EXPECT_EQ(flagsByte(first) & 128, 128);

    auto restored = deserializeStep(first, QueryPlanSerializationSettings{});
    EXPECT_TRUE(stepParams(restored).only_merge);

    /// Re-serializing the restored step pins the whole wire format, not just one accessor.
    EXPECT_EQ(first, serializeStep(*restored));
}

TEST(AggregatingStepOnlyMergeSerialization, DeserializedOutputHeaderMatches)
{
    registerAll();

    auto step = makeStep(/*only_merge=*/true);
    auto restored = deserializeStep(serializeStep(*step), QueryPlanSerializationSettings{});

    /// The executing node must produce exactly the header the initiator planned around.
    EXPECT_TRUE(blocksHaveEqualStructure(*restored->getOutputHeader(), *step->getOutputHeader()))
        << "restored: " << restored->getOutputHeader()->dumpStructure()
        << ", original: " << step->getOutputHeader()->dumpStructure();
}

TEST(AggregatingStepOnlyMergeSerialization, Version9SerializationThrows)
{
    registerAll();

    auto step = makeStep(/*only_merge=*/true);
    try
    {
        serializeStep(*step, /*version=*/9);
        FAIL() << "expected SUPPORT_IS_DISABLED";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
    }
}

TEST(AggregatingStepOnlyMergeSerialization, Bit128InVersion9StreamThrows)
{
    registerAll();

    /// The exact bytes a version-10 initiator emits, replayed as a version-9 stream: the bit is
    /// garbage there and must be rejected, mirroring the serialize-side gate.
    auto step = makeStep(/*only_merge=*/true);
    String bytes = serializeStep(*step);

    try
    {
        deserializeStep(bytes, QueryPlanSerializationSettings{}, /*version=*/9);
        FAIL() << "expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}

TEST(AggregatingStepOnlyMergeSerialization, OrdinaryStepRoundTripsUnchanged)
{
    registerAll();

    auto step = makeStep(/*only_merge=*/false);
    String first = serializeStep(*step);
    EXPECT_EQ(flagsByte(first) & 128, 0);

    auto restored = deserializeStep(first, QueryPlanSerializationSettings{});
    EXPECT_FALSE(stepParams(restored).only_merge);
    EXPECT_EQ(first, serializeStep(*restored));

    /// And an ordinary step stays serializable towards a version-9 peer.
    EXPECT_NO_THROW(serializeStep(*step, /*version=*/9));
}

/// The merge-only step runs the full `Aggregator`, whose `mergeOnBlock` spill path is governed by
/// `max_bytes_before_external_group_by` - the value must reach the worker through the settings
/// round-trip, or worker-side spilling is silently disabled.
TEST(AggregatingStepOnlyMergeSerialization, ExternalGroupByLimitSurvives)
{
    registerAll();

    constexpr size_t limit = 123456789;
    auto step = makeStep(/*only_merge=*/true, limit);

    /// Drive the real production path: `serializeSettings` -> `writeChangedBinary` -> a FRESH
    /// settings object -> `readBinary`, exactly as `QueryPlan::serialize` / `deserialize` do.
    QueryPlanSerializationSettings written;
    step->serializeSettings(written, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    WriteBufferFromOwnString out;
    written.writeChangedBinary(out);
    ReadBufferFromString in(out.str());
    QueryPlanSerializationSettings read;
    read.readBinary(in);
    EXPECT_EQ(read[QueryPlanSerializationSetting::max_bytes_before_external_group_by], limit);

    auto restored = deserializeStep(serializeStep(*step), read);
    EXPECT_TRUE(stepParams(restored).only_merge);
    EXPECT_EQ(stepParams(restored).max_bytes_before_external_group_by, limit);
}

/// Bit 128 must participate in cache-key serialization UNCONDITIONALLY (never guarded by
/// `!for_cache_key`): `only_merge` changes how the input is interpreted (state columns vs argument
/// columns), so two steps differing only in it must not share a stats/preallocation cache key.
/// The two steps here are built over an IDENTICAL input header with otherwise identical arguments,
/// so this test fails if the bit were `!for_cache_key`-guarded.
TEST(AggregatingStepOnlyMergeSerialization, CacheKeySerializationIsolatesOnlyMerge)
{
    registerAll();

    auto ordinary = makeStep(/*only_merge=*/false);
    auto merge_only = makeStep(/*only_merge=*/true);

    String ordinary_bytes = serializeStep(*ordinary, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*for_cache_key=*/true);
    String merge_only_bytes = serializeStep(*merge_only, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, /*for_cache_key=*/true);

    EXPECT_NE(ordinary_bytes, merge_only_bytes);

    /// The difference is exactly bit 128 of the flags byte; everything after it is identical.
    EXPECT_EQ(flagsByte(ordinary_bytes) ^ flagsByte(merge_only_bytes), 128);
    ASSERT_EQ(ordinary_bytes.size(), merge_only_bytes.size());
    EXPECT_EQ(ordinary_bytes.substr(1), merge_only_bytes.substr(1));
}
