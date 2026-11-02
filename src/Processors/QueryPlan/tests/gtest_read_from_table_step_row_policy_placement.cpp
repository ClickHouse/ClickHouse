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

String serializeStep(Placement placement, UInt64 step_version, std::optional<Names> selected_columns = {})
{
    ReadFromTableStep step(
        makeHeader(), "db.t", TableExpressionModifiers{}, /*use_parallel_replicas_=*/ false, placement, std::move(selected_columns));
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry, /*for_cache_key=*/ false, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version};
    step.serialize(ctx);
    return out.str();
}

QueryPlanStepPtr roundTrip(Placement placement, UInt64 step_version, std::optional<Names> selected_columns = {})
{
    const String bytes = serializeStep(placement, step_version, std::move(selected_columns));
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    ContextPtr context = getContext().context;
    auto header = makeHeader();
    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, SharedHeaders{}, header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, step_version, false};

    auto step = ReadFromTableStep::deserialize(ctx);
    EXPECT_TRUE(in.eof());
    return step;
}

Placement placementAfterRoundTrip(Placement placement, UInt64 step_version)
{
    return static_cast<const ReadFromTableStep &>(*roundTrip(placement, step_version)).getRowPolicyPlacement();
}

std::optional<Names> selectedColumnsAfterRoundTrip(std::optional<Names> selected_columns, UInt64 step_version)
{
    return static_cast<const ReadFromTableStep &>(*roundTrip(Placement::NotInPlan, step_version, std::move(selected_columns)))
        .getSelectedColumns();
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

/// The receiver checks its grants on these columns, so they arrive exactly, including an empty list (`SELECT count()`).
TEST(ReadFromTableStepRowPolicyPlacement, SelectedColumnsRoundTripAtStepVersionOne)
{
    for (const auto & selected_columns : {Names{"x", "alias_of_x"}, Names{}})
        EXPECT_EQ(selectedColumnsAfterRoundTrip(selected_columns, 1), selected_columns);
    EXPECT_EQ(selectedColumnsAfterRoundTrip(std::nullopt, 1), std::nullopt);

    EXPECT_EQ(serializeStep(Placement::NotInPlan, 0, Names{"x"}), serializeStep(Placement::Unknown, 0));
    EXPECT_EQ(selectedColumnsAfterRoundTrip(Names{"x"}, 0), std::nullopt);
}

TEST(ReadFromTableStepRowPolicyPlacement, StepVersionOneOnlyTowardsAPeerThatKnowsIt)
{
    QueryPlanStepRegistry registry;
    registerReadFromTableStep(registry);
    /// 26.9 writes global version 19.
    EXPECT_EQ(registry.versionToWrite("ReadFromTable", 19), 0);
    /// 26.10 writes global version 20 and knows only version 0 of the step.
    EXPECT_EQ(registry.versionToWrite("ReadFromTable", 20), 0);
    EXPECT_EQ(registry.versionToWrite("ReadFromTable", DBMS_QUERY_PLAN_SERIALIZATION_VERSION), 1);
}
