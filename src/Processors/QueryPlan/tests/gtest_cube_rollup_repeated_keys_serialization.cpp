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

namespace DB::ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

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
    /// Merge-only constructor, as the planner builds for these steps.
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

/// Serialize a step through the production path at `version` and return its byte stream.
template <typename Step>
String serializeStep(const Step & step, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    step.serialize(ctx);
    return out.str();
}

template <typename Deserializer>
QueryPlanStepPtr deserializeStep(Deserializer deserializer, const String & bytes, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    auto header = makeHeader();
    SharedHeaders input_headers{header};
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, header, settings, 0, version, false};

    return deserializer(ctx);
}

/// `GROUP BY ... (a, b, a)`: two distinct keys, three positions, and `a` at both ends. The list is
/// 0, 1, 0 - the shape a serializer that sorted or deduplicated the positions would destroy while
/// still passing a test that only repeated one key.
const std::vector<size_t> repeated_positions{0, 1, 0};

}

/// The ordered positions survive a round trip, not merely the fact that a key repeats.
TEST(CubeRollupRepeatedKeysSerialization, OrderedPositionsSurviveRoundTrip)
{
    tryRegisterAggregateFunctions();

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    const String first = serializeStep(cube, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto restored = deserializeStep(&CubeStep::deserialize, first, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(first, serializeStep(*assert_cast<CubeStep *>(restored.get()), DBMS_QUERY_PLAN_SERIALIZATION_VERSION));

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    const String rollup_first = serializeStep(rollup, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    auto rollup_restored = deserializeStep(&RollupStep::deserialize, rollup_first, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);
    ASSERT_NE(rollup_restored, nullptr);
    EXPECT_EQ(
        rollup_first, serializeStep(*assert_cast<RollupStep *>(rollup_restored.get()), DBMS_QUERY_PLAN_SERIALIZATION_VERSION));
}

/// A peer below the minimum version would expand from the deduplicated key list and answer with the
/// grouping sets this change corrects, so the sender refuses rather than downgrading in silence.
TEST(CubeRollupRepeatedKeysSerialization, RepeatedKeysBelowMinVersionThrow)
{
    tryRegisterAggregateFunctions();
    constexpr UInt64 too_old = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_REPEATED_GROUPING_KEYS - 1;

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    EXPECT_THROW(serializeStep(cube, too_old), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    EXPECT_THROW(serializeStep(rollup, too_old), Exception);
}

/// The refusal is keyed on the payload, not on the version, so a plan whose GROUP BY list repeats
/// nothing still ships to that same older peer.
TEST(CubeRollupRepeatedKeysSerialization, WithoutRepeatedKeysOlderPeersStillAccepted)
{
    tryRegisterAggregateFunctions();
    constexpr UInt64 too_old = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_REPEATED_GROUPING_KEYS - 1;

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, /*key_positions=*/{});
    EXPECT_NO_THROW(serializeStep(cube, too_old));

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, /*key_positions=*/{});
    EXPECT_NO_THROW(serializeStep(rollup, too_old));
}
