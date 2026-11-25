#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

constexpr UInt64 legacy_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_FLOAT_KEY_SCATTER - 1;
constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;

SharedHeader makeHeader()
{
    auto float_type = std::make_shared<DataTypeFloat64>();
    auto uint_type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({
        ColumnWithTypeAndName(float_type->createColumn(), float_type, "k"),
        ColumnWithTypeAndName(uint_type->createColumn(), uint_type, "u")}));
}

std::unique_ptr<SortingStep> makeWindowSortingStep(const SharedHeader & header, const String & key)
{
    SortDescription description;
    description.push_back(SortColumnDescription(key));
    auto step = std::make_unique<SortingStep>(header, description, description, UInt64{0}, SortingStep::Settings(65536));
    step->setWindowSorting();
    return step;
}

String serializeStep(const IQueryPlanStep & step, UInt64 version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    step.serialize(ctx);
    return out.str();
}

std::unique_ptr<SortingStep> deserializeStep(const String & bytes, const SharedHeader & header, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    ContextPtr context = getContext().context;

    SharedHeaders input_headers{header};
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, header, settings, 0, version, 0, false};

    auto step = SortingStep::deserialize(ctx);
    EXPECT_TRUE(in.eof());

    return std::unique_ptr<SortingStep>(static_cast<SortingStep *>(step.release()));
}

std::unique_ptr<SortingStep> roundTrip(const String & key, UInt64 version)
{
    auto header = makeHeader();
    auto step = makeWindowSortingStep(header, key);
    return deserializeStep(serializeStep(*step, version), header, version);
}

}

TEST(SortingStepWindowFloatKeySerialization, OlderPeerGetsNoPartitionKeys)
{
    auto restored = roundTrip("k", legacy_version);
    EXPECT_FALSE(restored->hasPartitions());
    EXPECT_TRUE(restored->isWindowSorting());
}

TEST(SortingStepWindowFloatKeySerialization, CurrentPeerKeepsPartitionKeys)
{
    auto restored = roundTrip("k", current_version);
    ASSERT_TRUE(restored->hasPartitions());
    ASSERT_EQ(restored->getPartitionByDescription().size(), 1u);
    EXPECT_EQ(restored->getPartitionByDescription().front().column_name, "k");
    EXPECT_TRUE(restored->isWindowSorting());
}

TEST(SortingStepWindowFloatKeySerialization, IntegerKeyKeepsPartitionKeysForOlderPeer)
{
    auto restored = roundTrip("u", legacy_version);
    EXPECT_TRUE(restored->hasPartitions());
    EXPECT_TRUE(restored->isWindowSorting());
}
