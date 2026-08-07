#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/SpillingHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/Exception.h>
#include <Common/tests/gtest_global_register.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>

#include <limits>
#include <memory>

using namespace DB;

namespace DB::Setting
{
    extern const SettingsUInt64 grace_hash_join_initial_buckets;
}

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

QueryPlanSerializationSettings roundTrip(const JoinSettings & source)
{
    QueryPlanSerializationSettings serialized;
    source.updatePlanSettings(serialized);

    WriteBufferFromOwnString out;
    serialized.writeChangedBinary(out);
    const String serialized_data = out.str();
    ReadBufferFromString in(serialized_data);

    QueryPlanSerializationSettings result;
    result.readBinary(in);
    return result;
}

std::unique_ptr<JoinStepLogical> makeJoinStep(UInt64 initial_buckets)
{
    tryRegisterFunctions();

    Settings query_settings;
    query_settings[Setting::grace_hash_join_initial_buckets] = initial_buckets;

    auto empty_header = std::make_shared<const Block>();
    return std::make_unique<JoinStepLogical>(
        empty_header,
        empty_header,
        JoinOperator{},
        JoinExpressionActions{},
        NameSet{},
        std::unordered_map<String, const ActionsDAG::Node *>{},
        false,
        JoinSettings(query_settings),
        SortingStep::Settings(query_settings));
}

String serializeJoinStep(const JoinStepLogical & step)
{
    SerializedSetsRegistry registry;
    WriteBufferFromOwnString out;
    IQueryPlanStep::Serialization ctx{
        .out = out,
        .registry = registry,
        .version = 0,
    };
    step.serialize(ctx);
    return out.str();
}

UInt8 readJoinStepFlags(const String & serialized_data)
{
    ReadBufferFromString in(serialized_data);
    UInt8 flags = 0;
    readIntBinary(flags, in);
    return flags;
}

UInt64 roundTripJoinStep(const JoinStepLogical & source, const String & serialized_data)
{
    auto serialized_settings = roundTrip(source.getJoinSettings());

    ReadBufferFromString in(serialized_data);
    DeserializedSetsRegistry registry;
    ContextPtr context;
    const auto & input_headers = source.getInputHeaders();
    const auto & output_header = source.getOutputHeader();
    IQueryPlanStep::Deserialization ctx{
        .in = in,
        .registry = registry,
        .storage_holders = {},
        .context = context,
        .input_headers = input_headers,
        .output_header = output_header,
        .settings = serialized_settings,
        .max_type_complexity = 0,
        .version = 0,
        .skipping = false,
    };

    auto restored = JoinStepLogical::deserialize(ctx);
    return static_cast<JoinStepLogical &>(*restored).getJoinSettings().grace_hash_join_initial_buckets;
}

#ifndef DEBUG_OR_SANITIZER_BUILD
void expectZeroExternalJoinThresholdRejected(bool concurrent)
{
    auto table_join = std::make_shared<TableJoin>(
        SizeLimits{},
        /*use_nulls=*/false,
        JoinKind::Inner,
        JoinStrictness::All,
        Names{});
    auto header = std::make_shared<const Block>();

    try
    {
        std::unique_ptr<SpillingHashJoin> join;
        if (concurrent)
            join = std::make_unique<SpillingHashJoin>(table_join, header, header, nullptr, 1, 1024, 2);
        else
            join = std::make_unique<SpillingHashJoin>(table_join, header, header, nullptr, 1, 1024);
        FAIL() << "Expected an exception";
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::LOGICAL_ERROR);
        EXPECT_TRUE(e.message().contains("SpillingHashJoin"));
        EXPECT_TRUE(e.message().contains("greater than 0"));
    }
}
#endif

}


TEST(QueryPlanSerializationSettings, PreservesLegacyGraceBucketsWhenWireFieldIsMissing)
{
    Settings query_settings;
    query_settings[Setting::grace_hash_join_initial_buckets] = 0;
    const auto serialized = roundTrip(JoinSettings(query_settings));

    EXPECT_EQ(JoinSettings(serialized).grace_hash_join_initial_buckets, 1);
}

TEST(QueryPlanSerializationSettings, PreservesExplicitGraceBuckets)
{
    for (UInt64 explicit_value : {1, 8})
    {
        Settings query_settings;
        query_settings[Setting::grace_hash_join_initial_buckets] = explicit_value;
        const auto serialized = roundTrip(JoinSettings(query_settings));

        EXPECT_EQ(JoinSettings(serialized).grace_hash_join_initial_buckets, explicit_value);
    }
}

TEST(QueryPlanSerializationSettings, RoundTripsAutomaticGraceBucketsThroughJoinStepFlag)
{
    const auto source = makeJoinStep(0);
    const auto serialized_data = serializeJoinStep(*source);

    EXPECT_EQ(readJoinStepFlags(serialized_data), 1);
    EXPECT_EQ(roundTripJoinStep(*source, serialized_data), 0);
}

TEST(QueryPlanSerializationSettings, ReadsLegacyMissingGraceBucketsAsOne)
{
    const auto source = makeJoinStep(0);
    auto serialized_data = serializeJoinStep(*source);
    serialized_data.front() = 0;

    EXPECT_EQ(roundTripJoinStep(*source, serialized_data), 1);
}

TEST(QueryPlanSerializationSettings, RoundTripsExplicitGraceBucketsWithoutAutoFlag)
{
    for (UInt64 explicit_value : {1, 8})
    {
        const auto source = makeJoinStep(explicit_value);
        const auto serialized_data = serializeJoinStep(*source);

        EXPECT_EQ(readJoinStepFlags(serialized_data), 0);
        EXPECT_EQ(roundTripJoinStep(*source, serialized_data), explicit_value);
    }
}


TEST(GraceHashJoinBuckets, PreservesExplicitValues)
{
    using Params = GraceHashJoin::InitialBucketsParams;

    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(1, 1024, Params{}, 0, 0), 1);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(3, 1024, Params{}, 0, 0), 4);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(3, 3, Params{}, 0, 0), 2);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(2048, 1024, Params{}, 0, 0), 1024);
    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(
            2,
            1024,
            Params{.total_rows_estimation = 1000000, .current_rows = 1000, .current_bytes = 1000000},
            1,
            1),
        2);
}

TEST(GraceHashJoinBuckets, UsesSizeEstimateInAutoMode)
{
    using Params = GraceHashJoin::InitialBucketsParams;

    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, Params{}, 0, 0), 1);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, Params{.total_rows_estimation = 999}, 1000, 0), 1);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, Params{.total_rows_estimation = 1000}, 1000, 0), 2);
    EXPECT_EQ(GraceHashJoin::getInitialNumBuckets(0, 1024, Params{.total_rows_estimation = 4000}, 1000, 0), 8);
}

TEST(GraceHashJoinBuckets, CombinesPlannerAndRuntimeInformation)
{
    using Params = GraceHashJoin::InitialBucketsParams;

    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(
            0,
            1024,
            Params{.total_rows_estimation = std::nullopt, .current_rows = 999, .current_bytes = 99900},
            0,
            100000),
        2);
    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(
            0,
            1024,
            Params{.total_rows_estimation = std::nullopt, .current_rows = 1000, .current_bytes = 100000},
            0,
            100000),
        4);
    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(
            0,
            1024,
            Params{.total_rows_estimation = 4000, .current_rows = 1000, .current_bytes = 100000},
            0,
            100000),
        16);
}

TEST(GraceHashJoinBuckets, ClampsAutoValueWithoutOverflow)
{
    using Params = GraceHashJoin::InitialBucketsParams;

    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(
            0,
            8,
            Params{.total_rows_estimation = std::numeric_limits<size_t>::max()},
            1,
            0),
        8);
}

TEST(GraceHashJoinBuckets, LimitsAutomaticTemporaryFileBufferMemory)
{
    constexpr size_t configured_buffer_size = 1uz << 20;

    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 1, 0), configured_buffer_size);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 128, 0), 1uz << 13);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 1024, 0), 1uz << 10);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 128, 1uz << 30), configured_buffer_size);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 256, 1uz << 30), 1uz << 19);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 1024, 1uz << 30), 1uz << 17);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 256, 100uz << 10), 50);
    EXPECT_EQ(GraceHashJoin::getTemporaryFilesBufferSize(configured_buffer_size, 1024, 1), 1);
}

TEST(SpillingHashJoin, RejectsZeroExternalJoinThreshold)
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    GTEST_SKIP() << "`LOGICAL_ERROR` aborts in debug and sanitizer builds";
#else
    expectZeroExternalJoinThresholdRejected(/*concurrent=*/false);
    expectZeroExternalJoinThresholdRejected(/*concurrent=*/true);
#endif
}
