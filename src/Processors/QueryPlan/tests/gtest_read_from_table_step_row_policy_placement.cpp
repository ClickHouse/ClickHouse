#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/ReadFromTableStep.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Common/tests/gtest_global_context.h>

namespace DB
{
void registerReadFromTableStep(QueryPlanStepRegistry & registry);
}

using namespace DB;

namespace
{

using Placement = ReadFromTableStep::RowPolicyPlacement;

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "x")}));
}

String serializeStep(Placement placement, UInt64 step_version)
{
    ReadFromTableStep step(makeHeader(), "db.t", TableExpressionModifiers{}, /*use_parallel_replicas_=*/ false, placement);
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry, /*for_cache_key=*/ false, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version};
    step.serialize(ctx);
    return out.str();
}

Placement placementAfterRoundTrip(Placement placement, UInt64 step_version)
{
    const String bytes = serializeStep(placement, step_version);
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    ContextPtr context = getContext().context;
    auto header = makeHeader();
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, SharedHeaders{}, header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version, false};

    auto step = ReadFromTableStep::deserialize(ctx);
    EXPECT_TRUE(in.eof());
    return static_cast<const ReadFromTableStep &>(*step).getRowPolicyPlacement();
}

}

/// A peer that predates the placement is sent the same bytes as before, and they read back as unknown.
TEST(ReadFromTableStepRowPolicyPlacement, StepVersionZeroIsUnchanged)
{
    EXPECT_EQ(serializeStep(Placement::FilterStep, 0), serializeStep(Placement::Unknown, 0));
    EXPECT_EQ(serializeStep(Placement::NotInPlan, 0), serializeStep(Placement::Unknown, 0));
    for (auto placement : {Placement::NotInPlan, Placement::FilterStep})
        EXPECT_EQ(placementAfterRoundTrip(placement, 0), Placement::Unknown);
}

TEST(ReadFromTableStepRowPolicyPlacement, RoundTripsAtStepVersionOne)
{
    for (auto placement : {Placement::NotInPlan, Placement::FilterStep})
        EXPECT_EQ(placementAfterRoundTrip(placement, 1), placement);
}

TEST(ReadFromTableStepRowPolicyPlacement, StepVersionOneOnlyTowardsAPeerThatKnowsIt)
{
    QueryPlanStepRegistry registry;
    registerReadFromTableStep(registry);
    /// 26.9 writes global version 19.
    EXPECT_EQ(registry.versionToWrite("ReadFromTable", 19), 0);
    EXPECT_EQ(registry.versionToWrite("ReadFromTable", DBMS_QUERY_PLAN_SERIALIZATION_VERSION), 1);
}
