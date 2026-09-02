#include <gtest/gtest.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/SetSerialization.h>
#include <Interpreters/WindowDescription.h>
#include <Processors/QueryPlan/Serialization.h>
#include <Processors/QueryPlan/WindowStep.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

/// Below `DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_AGGREGATE_TREE_THRESHOLD` the stream has no
/// place for `min_window_frame_rows_for_aggregate_tree`, and a peer that old always recomputes the frame.
/// `WindowStep::serialize` must therefore refuse a step that could use the frame aggregate tree - whose
/// floating-point results differ from the recompute path - and still serialize a step that cannot.
namespace
{

constexpr UInt64 pre_threshold_version = DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_WINDOW_AGGREGATE_TREE_THRESHOLD - 1;
constexpr UInt64 default_threshold = 2048;
constexpr UInt64 disabled_threshold = std::numeric_limits<UInt64>::max();

WindowFrame rowsFrame(UInt64 preceding)
{
    WindowFrame frame;
    frame.is_default = false;
    frame.type = WindowFrame::FrameType::ROWS;
    frame.begin_type = WindowFrame::BoundaryType::Offset;
    frame.begin_offset = preceding;
    frame.begin_preceding = true;
    frame.end_type = WindowFrame::BoundaryType::Current;
    return frame;
}

WindowFrame rangeFrame(WindowFrame::BoundaryType begin_type)
{
    WindowFrame frame;
    frame.is_default = false;
    frame.type = WindowFrame::FrameType::RANGE;
    frame.begin_type = begin_type;
    frame.begin_preceding = true;
    frame.end_type = WindowFrame::BoundaryType::Current;
    return frame;
}

bool serializes(const String & function_name, const WindowFrame & frame, UInt64 threshold, UInt64 version)
{
    tryRegisterAggregateFunctions();

    auto type = std::make_shared<DataTypeFloat64>();
    auto header = std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "v")}));

    WindowDescription description;
    description.window_name = "w";
    description.frame = frame;

    WindowFunctionDescription function;
    function.column_name = function_name + "(v) OVER w";
    AggregateFunctionProperties properties;
    function.aggregate_function = AggregateFunctionFactory::instance().get(function_name, NullsAction::EMPTY, {type}, {}, properties);
    function.argument_types = {type};
    function.argument_names = {"v"};

    WindowStep step(header, description, {function}, /*streams_fan_out_=*/false, threshold);

    WriteBufferFromOwnString out;
    SerializedSetsRegistry registry;
    IQueryPlanStep::Serialization ctx{out, registry};
    ctx.version = version;
    try
    {
        step.serialize(ctx);
        return true;
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), ErrorCodes::SUPPORT_IS_DISABLED);
        return false;
    }
}

}

TEST(WindowStepAggregateTreeVersion, RejectsOlderPeerWhenTheTreeCouldRun)
{
    EXPECT_FALSE(serializes("sum", rowsFrame(default_threshold), default_threshold, pre_threshold_version));
    /// A RANGE frame with a moving start may span any number of rows.
    EXPECT_FALSE(serializes("sum", rangeFrame(WindowFrame::BoundaryType::Current), default_threshold, pre_threshold_version));
}

TEST(WindowStepAggregateTreeVersion, AcceptsOlderPeerWhenTheTreeCannotRun)
{
    /// The frame never reaches the threshold.
    EXPECT_TRUE(serializes("sum", rowsFrame(default_threshold - 2), default_threshold, pre_threshold_version));
    /// The frame start never moves.
    EXPECT_TRUE(serializes("sum", rangeFrame(WindowFrame::BoundaryType::Unbounded), default_threshold, pre_threshold_version));
    /// The tree is disabled, as `compatibility` with a version before the tree does.
    EXPECT_TRUE(serializes("sum", rowsFrame(default_threshold), disabled_threshold, pre_threshold_version));
    /// The function keeps the recompute path (`count` has a constant-time batch add).
    EXPECT_TRUE(serializes("count", rowsFrame(default_threshold), default_threshold, pre_threshold_version));
}

TEST(WindowStepAggregateTreeVersion, AcceptsCurrentPeer)
{
    EXPECT_TRUE(serializes("sum", rowsFrame(default_threshold), default_threshold, DBMS_QUERY_PLAN_SERIALIZATION_VERSION));
}
