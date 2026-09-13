#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_setting_version
    = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_PARALLEL_ORDERED_MERGE_MATERIALIZATION - 1;

QueryPlanSerializationSettings serializeSortingSettings(UInt64 version)
{
    QueryPlanSerializationSettings default_settings;
    SortingStep::Settings sort_settings(default_settings);
    sort_settings.max_parallel_ordered_merge_materialization_threads = 4;

    auto type = std::make_shared<DataTypeUInt64>();
    auto header = std::make_shared<const Block>(Block{{type->createColumn(), type, "key"}});
    SortDescription sort_description;
    sort_description.emplace_back("key");
    SortingStep step(header, sort_description, sort_description, sort_settings, 0);

    QueryPlanSerializationSettings settings;
    step.serializeSettings(settings, version);
    return settings;
}

bool wireCarriesSetting(const QueryPlanSerializationSettings & settings)
{
    WriteBufferFromOwnString out;
    settings.writeChangedBinary(out);
    return out.str().contains("max_parallel_ordered_merge_materialization_threads");
}

}

TEST(ParallelOrderedMergeMaterializationPlanSetting, CarriedTowardsPeerThatKnowsTheName)
{
    EXPECT_TRUE(wireCarriesSetting(serializeSortingSettings(current_version)));
}

TEST(ParallelOrderedMergeMaterializationPlanSetting, NotCarriedTowardsPeerThatPredatesTheName)
{
    EXPECT_FALSE(wireCarriesSetting(serializeSortingSettings(pre_setting_version)));
}

TEST(ParallelOrderedMergeMaterializationPlanSetting, RoundTrips)
{
    auto written_settings = serializeSortingSettings(current_version);
    WriteBufferFromOwnString out;
    written_settings.writeChangedBinary(out);

    String serialized_settings = out.str();
    ReadBufferFromString in(serialized_settings);
    QueryPlanSerializationSettings read_settings;
    read_settings.readBinary(in);

    SortingStep::Settings sort_settings(read_settings);
    EXPECT_EQ(sort_settings.max_parallel_ordered_merge_materialization_threads, 4);
}
