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
constexpr UInt64 pre_decision_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_JOIN_ORDER_DECIDED - 1;

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
    return std::unique_ptr<JoinStepLogical>(static_cast<JoinStepLogical *>(step.release()));
}

}

TEST(JoinStepLogicalJoinOrderDecided, RoundTripsAtCurrentVersion)
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

TEST(JoinStepLogicalJoinOrderDecided, PreVersionCarriesNothing)
{
    auto decided = makeStep();
    decided->setOptimized();

    /// A receiver at the older version reads no byte of it, so a sender must write none.
    EXPECT_EQ(serializeStep(*decided, pre_decision_version), serializeStep(*makeStep(), pre_decision_version));
    EXPECT_FALSE(deserializeStep(serializeStep(*decided, pre_decision_version), pre_decision_version)->isOptimized());
}
