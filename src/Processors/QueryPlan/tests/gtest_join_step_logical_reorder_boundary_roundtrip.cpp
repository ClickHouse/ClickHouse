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
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/Sources/NullSource.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace
{

SharedHeader makeHeader(const String & column_name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

/// JoinExpressionActions refuses column pointers in the headers it is built from, so they are
/// stripped here. Same recipe as `gtest_distributed_query.cpp`.
ColumnsWithTypeAndName withoutColumnPointers(const ColumnsWithTypeAndName & header)
{
    ColumnsWithTypeAndName result = header;
    for (auto & element : result)
        element.column = nullptr;
    return result;
}

/// A minimal INNER JOIN on `left.k = right.k`. An equality condition is required: a step with an
/// empty expression is a cross join, whose serialization takes a different path.
std::unique_ptr<JoinStepLogical> makeStep(bool mark_as_boundary)
{
    auto left_header = makeHeader("left_k");
    auto right_header = makeHeader("right_k");

    JoinExpressionActions expression_actions(
        withoutColumnPointers(left_header->getColumnsWithTypeAndName()),
        withoutColumnPointers(right_header->getColumnsWithTypeAndName()));

    JoinOperator join_operator(JoinKind::Inner);
    {
        auto actions_dag = expression_actions.getActionsDAG();
        actions_dag->getOutputs() = actions_dag->getInputs();

        join_operator.expression.push_back(JoinActionRef::transform(
            {
                JoinActionRef(actions_dag->tryFindInOutputs("left_k"), expression_actions),
                JoinActionRef(actions_dag->tryFindInOutputs("right_k"), expression_actions),
            },
            JoinActionRef::AddFunction(JoinConditionOperator::Equals)));
    }

    ContextPtr context = getContext().context;
    NameSet required_output_columns = {"left_k", "right_k"};

    auto step = std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        std::move(join_operator),
        std::move(expression_actions),
        required_output_columns,
        std::unordered_map<String, const ActionsDAG::Node *>{},
        /*use_nulls_=*/false,
        JoinSettings(context->getSettingsRef()),
        SortingStep::Settings(context->getSettingsRef()));

    if (mark_as_boundary)
        step->setJoinReorderBoundary();

    return step;
}

/// Serialize through the production path and return the byte stream.
String serializeStep(const IQueryPlanStep & step)
{
    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = DBMS_QUERY_PLAN_SERIALIZATION_VERSION;
    step.serialize(ctx);
    return out.str();
}

/// Deserialize through the production path. The two input headers are what
/// `JoinStepLogical::deserialize` reconstructs the left and right sides from.
QueryPlanStepPtr deserializeStep(const String & bytes)
{
    ReadBufferFromString in(bytes);
    DeserializedSetsRegistry registry;
    QueryPlanSerializationSettings settings;
    auto output_header = makeHeader("left_k");
    SharedHeaders input_headers{makeHeader("left_k"), makeHeader("right_k")};
    ContextPtr context = getContext().context;

    IQueryPlanStep::Deserialization ctx{
        in, registry, {}, context, input_headers, output_header, settings, 0, DBMS_QUERY_PLAN_SERIALIZATION_VERSION, false};

    return JoinStepLogical::deserialize(ctx);
}

bool roundTripBoundary(bool mark_as_boundary)
{
    auto restored = deserializeStep(serializeStep(*makeStep(mark_as_boundary)));
    return assert_cast<JoinStepLogical &>(*restored).isJoinReorderBoundary();
}

UInt8 flagsByte(const String & bytes)
{
    return static_cast<UInt8>(bytes.at(0));
}

/// A source relation the join order optimizer accepts as a leaf. It has no row estimate, which is
/// what an unanalyzed relation looks like to the optimizer anyway.
QueryPlanStepPtr makeSourceStep(const String & column_name)
{
    return std::make_unique<ReadFromPreparedSource>(Pipe(std::make_shared<NullSource>(makeHeader(column_name))));
}

/// Runs the reorder pass over a two-relation join graph whose root carries the boundary, and hands
/// back the step of the node the pass leaves behind. `chooseJoinOrder` assembles that node from the
/// graph, so it is a different object from the one passed in; `rebuilt` reports whether it is, which
/// is what makes an assertion on the result meaningful.
QueryPlanStepPtr reorderRootJoin(bool mark_as_boundary, bool & rebuilt)
{
    ContextPtr context = getContext().context;

    QueryPlan::Nodes nodes;
    auto & left_node = nodes.emplace_back();
    left_node.step = makeSourceStep("left_k");
    auto & right_node = nodes.emplace_back();
    right_node.step = makeSourceStep("right_k");

    QueryPlan::Node root;
    auto step = makeStep(mark_as_boundary);
    const IQueryPlanStep * step_before = step.get();
    root.step = std::move(step);
    root.children = {&left_node, &right_node};

    QueryPlanOptimizations::optimizeJoinLogical(root, nodes, QueryPlanOptimizationSettings(context));

    rebuilt = root.step.get() != step_before;
    return std::move(root.step);
}

struct JoinStepLogicalReorderBoundaryRoundTrip : public ::testing::Test
{
    void SetUp() override
    {
        /// The join condition is a function node, so the DAG cannot be deserialized without the
        /// function factory being populated.
        tryRegisterFunctions();
    }
};

}

/// A view read is what marks a join as a reorder boundary, and no view is read on a node handed an
/// already-expanded plan fragment: `executeQuery` optimizes a deserialized plan right after
/// `resolveStorages`. So the mark cannot be re-derived there and has to travel in the step's own
/// bytes. A stateless test cannot observe this - a plan is only written to a stream for a shard that
/// is genuinely remote, and a single-server test has none - which is why the property is asserted
/// here instead.

TEST_F(JoinStepLogicalReorderBoundaryRoundTrip, MarkedBoundarySurvivesTheWire)
{
    EXPECT_TRUE(roundTripBoundary(/*mark_as_boundary=*/true));
}

/// Without this, a `deserialize` that set the flag unconditionally would still pass the arm above.
TEST_F(JoinStepLogicalReorderBoundaryRoundTrip, UnmarkedJoinStaysUnmarked)
{
    EXPECT_FALSE(roundTripBoundary(/*mark_as_boundary=*/false));
}

/// Pins the wire encoding rather than only the accessor, and fails if a future second flag is given
/// the same bit. The whole `flags` byte is asserted, not just the bit, because it is the only flag
/// in it today.
TEST_F(JoinStepLogicalReorderBoundaryRoundTrip, BoundaryBitIsTheOnlyFlagInTheByte)
{
    EXPECT_EQ(flagsByte(serializeStep(*makeStep(/*mark_as_boundary=*/true))), 1);
    EXPECT_EQ(flagsByte(serializeStep(*makeStep(/*mark_as_boundary=*/false))), 0);
}

/// Backward compatibility, which is what justifies not bumping DBMS_QUERY_PLAN_SERIALIZATION_VERSION:
/// a sender that predates the bit writes the byte as a literal zero, and such a stream must restore a
/// step that is not a boundary, i.e. exactly the behaviour those senders have.
///
/// The stream is produced by serializing a marked step and clearing its first byte, rather than by
/// hand-writing the payload: the rest of the payload is a serialized `ActionsDAG` plus a
/// `JoinOperator`, so hand-writing it would pin those two formats here as well.
TEST_F(JoinStepLogicalReorderBoundaryRoundTrip, OldFormatZeroFlagsByteYieldsFalse)
{
    String bytes = serializeStep(*makeStep(/*mark_as_boundary=*/true));
    ASSERT_EQ(flagsByte(bytes), 1);

    bytes[0] = 0;
    auto restored = deserializeStep(bytes);
    EXPECT_FALSE(assert_cast<JoinStepLogical &>(*restored).isJoinReorderBoundary());
}

/// The node a reordered join leaves behind is assembled from the query graph, so it is not the step
/// the boundary was set on. It is also the node a plan fragment is cloned and streamed from, and the
/// arms above cannot see that: they serialize a step that was never reordered.
TEST_F(JoinStepLogicalReorderBoundaryRoundTrip, RebuiltRootKeepsTheBoundaryThroughTheWire)
{
    bool rebuilt = false;
    auto reordered = reorderRootJoin(/*mark_as_boundary=*/true, rebuilt);
    /// Guards the arm itself: if the pass ever stopped rebuilding the node, everything below would
    /// pass by describing the step this test constructed rather than the one reordering produced.
    ASSERT_TRUE(rebuilt);

    auto * reordered_join = typeid_cast<JoinStepLogical *>(reordered.get());
    ASSERT_TRUE(reordered_join);
    EXPECT_TRUE(reordered_join->isJoinReorderBoundary());

    auto restored = deserializeStep(serializeStep(*reordered_join));
    EXPECT_TRUE(assert_cast<JoinStepLogical &>(*restored).isJoinReorderBoundary());
}

/// The counterpart: reordering must not invent a boundary that the plan never carried.
TEST_F(JoinStepLogicalReorderBoundaryRoundTrip, RebuiltRootOfAnUnmarkedJoinStaysUnmarked)
{
    bool rebuilt = false;
    auto reordered = reorderRootJoin(/*mark_as_boundary=*/false, rebuilt);
    ASSERT_TRUE(rebuilt);

    auto * reordered_join = typeid_cast<JoinStepLogical *>(reordered.get());
    ASSERT_TRUE(reordered_join);
    EXPECT_FALSE(reordered_join->isJoinReorderBoundary());
}
