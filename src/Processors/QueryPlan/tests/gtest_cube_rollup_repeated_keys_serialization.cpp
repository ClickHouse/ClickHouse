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

/// Serialize a step through the production path at the given Cube/Rollup step serialization version
/// and return its byte stream. Version 1 carries the repeated-key positions; version 0 is the older
/// format without them.
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
    const String first = serializeStep(cube);
    auto restored = deserializeStep(&CubeStep::deserialize, first);
    ASSERT_NE(restored, nullptr);
    EXPECT_EQ(first, serializeStep(*assert_cast<CubeStep *>(restored.get())));

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    const String rollup_first = serializeStep(rollup);
    auto rollup_restored = deserializeStep(&RollupStep::deserialize, rollup_first);
    ASSERT_NE(rollup_restored, nullptr);
    EXPECT_EQ(
        rollup_first, serializeStep(*assert_cast<RollupStep *>(rollup_restored.get())));
}

/// Toward a peer below the step version that carries the positions (step version 0), the sender
/// would expand from the deduplicated key list and answer with the grouping sets this change
/// corrects, so it refuses rather than downgrading in silence.
TEST(CubeRollupRepeatedKeysSerialization, RepeatedKeysBelowStepVersionThrow)
{
    tryRegisterAggregateFunctions();

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    EXPECT_THROW(serializeStep(cube, /*step_version=*/0), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, repeated_positions);
    EXPECT_THROW(serializeStep(rollup, /*step_version=*/0), Exception);
}

/// A payload whose positions do not reference every deduplicated key is not one the sender could
/// have built - the positions come from the GROUP BY list, where every key appears at least once.
/// Executing it anyway would silently drop the unreferenced key from every grouping set, so the
/// deserializer must reject the stream. The constructor does not validate, which is what lets this
/// test serialize the impossible payload in the first place.
TEST(CubeRollupRepeatedKeysSerialization, PositionsNotCoveringEveryKeyAreRejected)
{
    tryRegisterAggregateFunctions();
    /// Two keys, but every position points at the first: key `b` is never referenced.
    const std::vector<size_t> not_covering{0, 0};

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, not_covering);
    const String cube_bytes = serializeStep(cube);
    EXPECT_THROW(
        deserializeStep(&CubeStep::deserialize, cube_bytes), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, not_covering);
    const String rollup_bytes = serializeStep(rollup);
    EXPECT_THROW(
        deserializeStep(&RollupStep::deserialize, rollup_bytes), Exception);
}

/// First appearances of key indexes must arrive as 0, 1, 2, ...: the planner assigns a new index to
/// each expression the first time it sees it while walking the GROUP BY list, so `[1, 0]` covers
/// every key yet is unreachable from any query. `RollupTransform`'s `__grouping_set` numbering
/// relies on that order, so executing the payload would return wrong `GROUPING()` bits instead of
/// failing. Together with the coverage test above this pins the exact set of accepted payloads.
TEST(CubeRollupRepeatedKeysSerialization, PositionsOutOfFirstOccurrenceOrderAreRejected)
{
    tryRegisterAggregateFunctions();
    /// Covers both keys, so only the ordering rule can reject it.
    const std::vector<size_t> out_of_order{1, 0};

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, out_of_order);
    const String cube_bytes = serializeStep(cube);
    EXPECT_THROW(
        deserializeStep(&CubeStep::deserialize, cube_bytes), Exception);

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, out_of_order);
    const String rollup_bytes = serializeStep(rollup);
    EXPECT_THROW(
        deserializeStep(&RollupStep::deserialize, rollup_bytes), Exception);
}

/// The refusal is keyed on the payload, not on the version, so a plan whose GROUP BY list repeats
/// nothing still ships at step version 0 to that same older peer.
TEST(CubeRollupRepeatedKeysSerialization, WithoutRepeatedKeysOlderPeersStillAccepted)
{
    tryRegisterAggregateFunctions();

    CubeStep cube(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, /*key_positions=*/{});
    EXPECT_NO_THROW(serializeStep(cube, /*step_version=*/0));

    RollupStep rollup(makeHeader(), makeParams(), /*final=*/true, /*use_nulls=*/false, /*key_positions=*/{});
    EXPECT_NO_THROW(serializeStep(rollup, /*step_version=*/0));
}
