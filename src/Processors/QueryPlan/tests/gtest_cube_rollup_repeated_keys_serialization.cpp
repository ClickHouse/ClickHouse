#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/CubeStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/RollupStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <QueryPipeline/SizeLimits.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block(
        {ColumnWithTypeAndName(type->createColumn(), type, "a"),
         ColumnWithTypeAndName(type->createColumn(), type, "b")}));
}

Aggregator::Params makeParams()
{
    return Aggregator::Params(
        Names{"a", "b"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/true,
        /*enable_packed_string_keys=*/true);
}

template <typename Step>
String serializeStep(const Step & step, UInt64 step_version = 1)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    ctx.step_version = step_version;
    step.serialize(ctx);
    return out.str();
}

template <typename Deserializer>
QueryPlanStepPtr deserializeStep(Deserializer deserializer, const String & bytes, UInt64 step_version = 1)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    auto header = makeHeader();
    SharedHeaders input_headers{header};
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version, false};

    return deserializer(ctx);
}

/// `GROUP BY CUBE(a, b, a)`.
const std::vector<size_t> repeated_positions{0, 1, 0};

}

TEST(CubeRollupRepeatedKeysSerialization, PositionsSurviveRoundTrip)
{
    tryRegisterAggregateFunctions();

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    const String cube_bytes = serializeStep(cube);
    auto cube_restored = deserializeStep(&CubeStep::deserialize, cube_bytes);
    ASSERT_NE(cube_restored, nullptr);
    EXPECT_EQ(cube_bytes, serializeStep(*assert_cast<CubeStep *>(cube_restored.get())));

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    const String rollup_bytes = serializeStep(rollup);
    auto rollup_restored = deserializeStep(&RollupStep::deserialize, rollup_bytes);
    ASSERT_NE(rollup_restored, nullptr);
    EXPECT_EQ(rollup_bytes, serializeStep(*assert_cast<RollupStep *>(rollup_restored.get())));
}

TEST(CubeRollupRepeatedKeysSerialization, RepeatedKeysBelowStepVersionThrow)
{
    tryRegisterAggregateFunctions();

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    EXPECT_THROW(serializeStep(cube, /*step_version=*/0), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    EXPECT_THROW(serializeStep(rollup, /*step_version=*/0), Exception);
}

TEST(CubeRollupRepeatedKeysSerialization, NoRepeatedKeysBelowStepVersionIsFine)
{
    tryRegisterAggregateFunctions();

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false);
    EXPECT_NO_THROW(serializeStep(cube, /*step_version=*/0));

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false);
    EXPECT_NO_THROW(serializeStep(rollup, /*step_version=*/0));
}

TEST(CubeRollupRepeatedKeysSerialization, PositionsNotCoveringEveryKeyAreRejected)
{
    tryRegisterAggregateFunctions();
    const std::vector<size_t> not_covering{0, 0};

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, not_covering);
    EXPECT_THROW(deserializeStep(&CubeStep::deserialize, serializeStep(cube)), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, not_covering);
    EXPECT_THROW(deserializeStep(&RollupStep::deserialize, serializeStep(rollup)), Exception);
}

TEST(CubeRollupRepeatedKeysSerialization, PositionsOutOfFirstOccurrenceOrderAreRejected)
{
    tryRegisterAggregateFunctions();
    const std::vector<size_t> out_of_order{1, 0};

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, out_of_order);
    EXPECT_THROW(deserializeStep(&CubeStep::deserialize, serializeStep(cube)), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, out_of_order);
    EXPECT_THROW(deserializeStep(&RollupStep::deserialize, serializeStep(rollup)), Exception);
}
