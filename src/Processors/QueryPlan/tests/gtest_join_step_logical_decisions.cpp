#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/Context.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Interpreters/SetSerialization.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_decision_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_JOIN_DECISIONS - 1;

SharedHeader makeHeader(const String & column_name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

/// The two-header constructor adds one DAG input per column and leaves the outputs empty, while
/// JoinStepLogical takes its output header from the DAG's result columns. A CROSS JOIN with no ON
/// expression passes both sides through, so every input is also an output.
JoinExpressionActions makeExpressionActions(const Block & left_header, const Block & right_header)
{
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);
    return expression_actions;
}

std::unique_ptr<JoinStepLogical> makeStep()
{
    auto left_header = makeHeader("l");
    auto right_header = makeHeader("r");
    QueryPlanSerializationSettings settings;

    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator{},
        makeExpressionActions(*left_header, *right_header),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings),
        SortingStep::Settings(settings));
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

/// The QueryPlanSerializationSettings object is left at its DECLARE defaults, which is what
/// QueryPlan::deserialize hands each step.
std::unique_ptr<JoinStepLogical> deserializeStep(const String & bytes, UInt64 version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    SharedHeaders input_headers{makeHeader("l"), makeHeader("r")};
    SharedHeader output_header = makeHeader("l");
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, version, false};

    auto step = JoinStepLogical::deserialize(ctx);

    /// The buffer holds one step, so a byte left unread here is a byte that would be taken for the
    /// next node of a real plan.
    EXPECT_TRUE(in.eof());

    return std::unique_ptr<JoinStepLogical>(static_cast<JoinStepLogical *>(step.release()));
}

}

TEST(JoinStepLogicalDecisions, RoundTripsAtCurrentVersion)
{
    auto decided = makeStep();
    decided->setOptimized();
    auto undecided = makeStep();

    const String decided_bytes = serializeStep(*decided, current_version);
    const String undecided_bytes = serializeStep(*undecided, current_version);

    /// Without this the assertions below hold for a serializer that writes the same bytes either way.
    EXPECT_NE(decided_bytes, undecided_bytes);

    auto restored_decided = deserializeStep(decided_bytes, current_version);
    EXPECT_TRUE(restored_decided->isOptimized());
    EXPECT_EQ(decided_bytes, serializeStep(*restored_decided, current_version));

    auto restored_undecided = deserializeStep(undecided_bytes, current_version);
    EXPECT_FALSE(restored_undecided->isOptimized());
    EXPECT_EQ(undecided_bytes, serializeStep(*restored_undecided, current_version));
}

TEST(JoinStepLogicalDecisions, SmallProbeDecisionRoundTripsAtCurrentVersion)
{
    auto declined = makeStep();
    declined->setRuntimeFilterDeclinedForSmallProbe();
    auto undecided = makeStep();

    const String declined_bytes = serializeStep(*declined, current_version);
    const String undecided_bytes = serializeStep(*undecided, current_version);

    EXPECT_NE(declined_bytes, undecided_bytes);

    auto restored_declined = deserializeStep(declined_bytes, current_version);
    EXPECT_TRUE(restored_declined->isRuntimeFilterDeclinedForSmallProbe());
    EXPECT_EQ(declined_bytes, serializeStep(*restored_declined, current_version));

    auto restored_undecided = deserializeStep(undecided_bytes, current_version);
    EXPECT_FALSE(restored_undecided->isRuntimeFilterDeclinedForSmallProbe());
    EXPECT_EQ(undecided_bytes, serializeStep(*restored_undecided, current_version));
}

/// The two decisions share one byte, so a serializer that conflated them would still pass the two
/// tests above. Each of the four combinations has to survive on its own.
TEST(JoinStepLogicalDecisions, DecisionsAreIndependentlyObservable)
{
    for (const bool order_decided : {false, true})
    {
        for (const bool probe_declined : {false, true})
        {
            auto step = makeStep();
            if (order_decided)
                step->setOptimized();
            if (probe_declined)
                step->setRuntimeFilterDeclinedForSmallProbe();

            auto restored = deserializeStep(serializeStep(*step, current_version), current_version);
            EXPECT_EQ(restored->isOptimized(), order_decided);
            EXPECT_EQ(restored->isRuntimeFilterDeclinedForSmallProbe(), probe_declined);
        }
    }
}

/// A clone is the initiator's own copy of the fragment it ships, so it has to reach the same
/// decisions as the copy the replicas deserialize.
TEST(JoinStepLogicalDecisions, CloneCarriesTheDecisions)
{
    auto step = makeStep();
    step->setOptimized();
    step->setRuntimeFilterDeclinedForSmallProbe();

    auto cloned = step->clone();
    const auto * cloned_join = typeid_cast<const JoinStepLogical *>(cloned.get());
    ASSERT_TRUE(cloned_join);
    EXPECT_TRUE(cloned_join->isOptimized());
    EXPECT_TRUE(cloned_join->isRuntimeFilterDeclinedForSmallProbe());

    EXPECT_EQ(serializeStep(*cloned_join, current_version), serializeStep(*step, current_version));
}

TEST(JoinStepLogicalDecisions, PreVersionCarriesNothing)
{
    auto decided = makeStep();
    decided->setOptimized();
    decided->setRuntimeFilterDeclinedForSmallProbe();

    /// A receiver at the older version reads no byte of it, so a sender must write none.
    EXPECT_EQ(serializeStep(*decided, pre_decision_version), serializeStep(*makeStep(), pre_decision_version));

    auto restored = deserializeStep(serializeStep(*decided, pre_decision_version), pre_decision_version);
    EXPECT_FALSE(restored->isOptimized());
    EXPECT_FALSE(restored->isRuntimeFilterDeclinedForSmallProbe());
}
