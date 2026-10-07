#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Aggregator.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

namespace DB
{
void registerAggregatingStep(QueryPlanStepRegistry & registry);

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}
}

using namespace DB;

/// Pins the wire format of the `GROUP BY` top-K parameters carried by `Aggregating` step version 1:
/// every field survives `serialize -> deserialize -> serialize`, and a stream written for a peer
/// below `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_GROUP_BY_TOP_K` omits them entirely.

namespace
{

constexpr auto step_name = "Aggregating";

/// A registry holding just the step under test, registered the way `registerPlanSteps` registers it,
/// so the version list is the production one. Not the process-wide instance: other tests in this
/// binary populate that one, and a step cannot be registered twice.
QueryPlanStepRegistry & stepRegistry()
{
    static QueryPlanStepRegistry registry;
    static const bool registered = []
    {
        registerAggregatingStep(registry);
        return true;
    }();
    (void)registered;
    return registry;
}

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({
        ColumnWithTypeAndName(type->createColumn(), type, "a"),
        ColumnWithTypeAndName(type->createColumn(), type, "b")}));
}

std::unique_ptr<AggregatingStep> makeStep(std::optional<Aggregator::Params::TopKParams> top_k)
{
    /// Merge-only constructor.
    Aggregator::Params params(
        Names{"a", "b"},
        AggregateDescriptions{},
        /*overflow_row=*/false,
        /*max_threads=*/1,
        /*max_block_size=*/65536,
        /*min_hit_rate_to_use_consecutive_keys_optimization=*/0.5f,
        /*serialize_string_with_zero_byte=*/false,
        /*enable_packed_string_keys=*/true);
    params.top_k = std::move(top_k);

    return std::make_unique<AggregatingStep>(
        makeHeader(),
        std::move(params),
        GroupingSetsParamsList{},
        /*final=*/false,
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

Aggregator::Params::TopKParams makeTopK(size_t key_columns, std::vector<int> directions, std::vector<int> nulls_directions)
{
    Aggregator::Params::TopKParams top_k;
    top_k.k = 7;
    top_k.key_columns = key_columns;
    top_k.observation_rows = 12345;
    top_k.directions = std::move(directions);
    top_k.nulls_directions = std::move(nulls_directions);
    return top_k;
}

/// Serialize the step the way `QueryPlan::serialize` does: the step version comes from the registry.
String serializeStep(const IQueryPlanStep & step, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    ctx.step_version = stepRegistry().versionToWrite(step_name, version);
    step.serialize(ctx);
    return out.str();
}

std::unique_ptr<AggregatingStep> deserializeStep(const String & bytes, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    auto header = makeHeader();
    SharedHeaders input_headers{header};
    QueryPlanSerializationSettings settings;

    const UInt64 step_version = stepRegistry().versionToWrite(step_name, version);
    stepRegistry().checkVersionReadable(step_name, step_version);
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, getContext().context, input_headers, header, settings, 0, version, step_version, false};

    auto step = AggregatingStep::deserialize(ctx);
    EXPECT_TRUE(in.eof()) << "the step left unread bytes in the stream";
    return std::unique_ptr<AggregatingStep>(assert_cast<AggregatingStep *>(step.release()));
}

void expectSameTopK(const Aggregator::Params::TopKParams & expected, const Aggregator::Params::TopKParams & actual)
{
    EXPECT_EQ(actual.k, expected.k);
    EXPECT_EQ(actual.key_columns, expected.key_columns);
    EXPECT_EQ(actual.observation_rows, expected.observation_rows);
    EXPECT_EQ(actual.directions, expected.directions);
    EXPECT_EQ(actual.nulls_directions, expected.nulls_directions);
}

}

TEST(AggregatingStepTopKSerialization, MultiKeyMixedDirectionsRoundTrip)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// `ORDER BY a DESC NULLS FIRST, b ASC NULLS LAST` over `GROUP BY a, b`.
    const auto top_k = makeTopK(2, {-1, 1}, {-1, 1});
    const UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

    const String bytes = serializeStep(*makeStep(top_k), version);
    auto restored = deserializeStep(bytes, version);

    ASSERT_TRUE(restored->getParams().top_k.has_value());
    expectSameTopK(top_k, *restored->getParams().top_k);
    EXPECT_EQ(serializeStep(*restored, version), bytes);
}

TEST(AggregatingStepTopKSerialization, PrefixOfGroupByKeysRoundTrip)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    /// `ORDER BY a DESC` over `GROUP BY a, b`: the heap ranks on the leading key only.
    const auto top_k = makeTopK(1, {-1}, {1});
    const UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

    const String bytes = serializeStep(*makeStep(top_k), version);
    auto restored = deserializeStep(bytes, version);

    ASSERT_TRUE(restored->getParams().top_k.has_value());
    expectSameTopK(top_k, *restored->getParams().top_k);
    EXPECT_EQ(serializeStep(*restored, version), bytes);
}

TEST(AggregatingStepTopKSerialization, AbsentTopKRoundTrip)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

    const String bytes = serializeStep(*makeStep(std::nullopt), version);
    auto restored = deserializeStep(bytes, version);

    EXPECT_FALSE(restored->getParams().top_k.has_value());
    EXPECT_EQ(serializeStep(*restored, version), bytes);
}

/// Towards an older peer the parameters are omitted rather than rejected: the stream must be byte for
/// byte the one written for a step without top-K, which is what a version 0 reader expects.
TEST(AggregatingStepTopKSerialization, OlderPeerOmitsTopK)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const UInt64 version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_GROUP_BY_TOP_K - 1;
    ASSERT_EQ(stepRegistry().versionToWrite(step_name, version), 0u);
    ASSERT_EQ(stepRegistry().versionToWrite(step_name, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_GROUP_BY_TOP_K), 1u);

    const String with_top_k = serializeStep(*makeStep(makeTopK(2, {-1, 1}, {-1, 1})), version);
    const String without_top_k = serializeStep(*makeStep(std::nullopt), version);
    EXPECT_EQ(with_top_k, without_top_k);

    auto restored = deserializeStep(with_top_k, version);
    EXPECT_FALSE(restored->getParams().top_k.has_value());
}

/// The reader validates the payload instead of trusting it: more ranked columns than `GROUP BY` keys
/// would index past the key columns in the heap.
TEST(AggregatingStepTopKSerialization, MoreRankedColumnsThanKeysIsRejected)
{
    tryRegisterFunctions();
    tryRegisterAggregateFunctions();

    const UInt64 version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    const String bytes = serializeStep(*makeStep(makeTopK(3, {1, 1, 1}, {1, 1, 1})), version);

    try
    {
        deserializeStep(bytes, version);
        FAIL() << "expected INCORRECT_DATA";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::INCORRECT_DATA);
    }
}
