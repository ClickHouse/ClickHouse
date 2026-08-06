#include <gtest/gtest.h>

#include <Core/Settings.h>
#include <Interpreters/GraceHashJoin.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SpillingHashJoin.h>
#include <Interpreters/TableJoin.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>

#include <limits>
#include <memory>

using namespace DB;

namespace DB::Setting
{
    extern const SettingsUInt64 grace_hash_join_initial_buckets;
}

namespace DB::QueryPlanSerializationSetting
{
    extern const QueryPlanSerializationSettingsUInt64 grace_hash_join_initial_buckets;
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

}


TEST(QueryPlanSerializationSettings, EncodesAutomaticGraceBucketsByFieldAbsence)
{
    Settings query_settings;
    query_settings[Setting::grace_hash_join_initial_buckets] = 0;
    const auto serialized = roundTrip(JoinSettings(query_settings));

    EXPECT_FALSE(serialized.isChanged("grace_hash_join_initial_buckets"));
    EXPECT_EQ(serialized[QueryPlanSerializationSetting::grace_hash_join_initial_buckets], 1);
    EXPECT_EQ(JoinSettings(serialized).grace_hash_join_initial_buckets, 0);
}

TEST(QueryPlanSerializationSettings, PreservesExplicitGraceBuckets)
{
    for (UInt64 explicit_value : {1, 8})
    {
        Settings query_settings;
        query_settings[Setting::grace_hash_join_initial_buckets] = explicit_value;
        const auto serialized = roundTrip(JoinSettings(query_settings));

        EXPECT_TRUE(serialized.isChanged("grace_hash_join_initial_buckets"));
        EXPECT_EQ(JoinSettings(serialized).grace_hash_join_initial_buckets, explicit_value);
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
        GraceHashJoin::getInitialNumBuckets(0, 1024, Params{.current_rows = 999, .current_bytes = 99900}, 0, 100000),
        2);
    EXPECT_EQ(
        GraceHashJoin::getInitialNumBuckets(0, 1024, Params{.current_rows = 1000, .current_bytes = 100000}, 0, 100000),
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

TEST(SpillingHashJoin, RejectsZeroExternalJoinThreshold)
{
    expectZeroExternalJoinThresholdRejected(/*concurrent=*/false);
    expectZeroExternalJoinThresholdRejected(/*concurrent=*/true);
}
