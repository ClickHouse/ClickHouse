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
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/typeid_cast.h>

namespace DB
{
void registerJoinStep(QueryPlanStepRegistry & registry);
}

using namespace DB;

/// The mark of the semi or anti join an `INTERSECT DISTINCT` or `EXCEPT DISTINCT` is executed as changes how
/// the physical join is built, so a copied or shipped plan fragment has to keep it.
namespace
{

constexpr UInt64 current_version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
constexpr UInt64 pre_mark_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_SET_OPERATION_JOIN - 1;

SharedHeader makeHeader(const String & column_name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

JoinExpressionActions makeExpressionActions(const Block & left_header, const Block & right_header)
{
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);
    return expression_actions;
}

std::unique_ptr<JoinStepLogical> makeStep(bool is_set_operation)
{
    auto left_header = makeHeader("l");
    auto right_header = makeHeader("r");
    QueryPlanSerializationSettings settings;

    auto step = std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator{},
        makeExpressionActions(*left_header, *right_header),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings, current_version),
        SortingStep::Settings(settings));
    step->setIsSetOperation(is_set_operation);
    return step;
}

String serializeStep(const IQueryPlanStep & step, UInt64 version, UInt64 step_version)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    ctx.step_version = step_version;
    step.serialize(ctx);
    return out.str();
}

std::unique_ptr<JoinStepLogical> deserializeStep(const String & bytes, UInt64 version, UInt64 step_version)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    SharedHeaders input_headers{makeHeader("l"), makeHeader("r")};
    SharedHeader output_header = makeHeader("l");
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, version, step_version, false};

    auto step = JoinStepLogical::deserialize(ctx);
    EXPECT_TRUE(in.eof());

    return std::unique_ptr<JoinStepLogical>(static_cast<JoinStepLogical *>(step.release()));
}

}

TEST(JoinStepLogicalSetOperation, MarkRoundTripsAtTheCurrentVersion)
{
    QueryPlanStepRegistry registry;
    registerJoinStep(registry);
    const auto step_version = registry.versionToWrite("Join", current_version);
    EXPECT_EQ(step_version, 1);

    for (const bool is_set_operation : {false, true})
    {
        auto step = makeStep(is_set_operation);
        const String bytes = serializeStep(*step, current_version, step_version);
        auto restored = deserializeStep(bytes, current_version, step_version);
        EXPECT_EQ(restored->isSetOperation(), is_set_operation);
        EXPECT_EQ(bytes, serializeStep(*restored, current_version, step_version));
    }

    EXPECT_NE(
        serializeStep(*makeStep(true), current_version, step_version),
        serializeStep(*makeStep(false), current_version, step_version));
}

TEST(JoinStepLogicalSetOperation, MarkIsNotCarriedTowardsAnOlderPeer)
{
    QueryPlanStepRegistry registry;
    registerJoinStep(registry);
    const auto step_version = registry.versionToWrite("Join", pre_mark_version);
    EXPECT_EQ(step_version, 0);

    /// A receiver at the older version takes the byte for empty flags, so a sender must write it so.
    EXPECT_EQ(
        serializeStep(*makeStep(true), pre_mark_version, step_version),
        serializeStep(*makeStep(false), pre_mark_version, step_version));
    auto restored = deserializeStep(serializeStep(*makeStep(true), pre_mark_version, step_version), pre_mark_version, step_version);
    EXPECT_FALSE(restored->isSetOperation());
}

TEST(JoinStepLogicalSetOperation, CloneCarriesTheMark)
{
    auto step = makeStep(true);
    auto cloned = step->clone();
    auto * cloned_join = typeid_cast<JoinStepLogical *>(cloned.get());
    ASSERT_TRUE(cloned_join);
    EXPECT_TRUE(cloned_join->isSetOperation());
}
