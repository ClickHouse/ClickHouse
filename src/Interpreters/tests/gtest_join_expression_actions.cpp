#include <gtest/gtest.h>

#include <base/defines.h>
#include <Common/Exception.h>
#include <Core/Block.h>
#include <Core/ColumnWithTypeAndName.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinExpressionActions.h>

using namespace DB;

namespace
{
ColumnWithTypeAndName makeColumn(const String & name, const DataTypePtr & type)
{
    return ColumnWithTypeAndName(type->createColumn(), type, name);
}
}

/// Regression test for "Left and right columns have same names: [__join_result_dummy], [__join_result_dummy]".
///
/// A JoinStepLogical that has no required output columns (e.g. count() over a join) emits a
/// constant marker column named `__join_result_dummy`. When two such joins are the children of a
/// parent join (e.g. count() over a CROSS join of two count-pruned joins), BOTH of the parent's
/// input headers carry `__join_result_dummy`. The in-process planner builds this with the
/// two-argument JoinExpressionActions constructor, which is fine. A distributed-plan worker
/// reconstructs the same step with the three-argument constructor (from a serialized ActionsDAG plus
/// the two child output headers); that path must accept the duplicate name instead of rejecting the
/// (valid) plan with a LOGICAL_ERROR.
TEST(JoinExpressionActions, DuplicateDummyColumnInBothHeaders)
{
    const auto u64 = std::make_shared<DataTypeUInt64>();
    const auto u8 = std::make_shared<DataTypeUInt8>();
    const String dummy = "__join_result_dummy";

    /// Both inputs of the (CROSS) join carry only the dummy marker, exactly as produced by two
    /// count-pruned child joins.
    Block left_header{makeColumn("left_key", u64), makeColumn(dummy, u8)};
    Block right_header{makeColumn("right_key", u64), makeColumn(dummy, u8)};

    /// The serialized DAG that the deserialize path feeds to the three-argument constructor: one
    /// INPUT node per header column, ordered [left columns..., right columns...].
    ActionsDAG actions_dag;
    actions_dag.addInput("left_key", u64);
    actions_dag.addInput(dummy, u8);
    actions_dag.addInput("right_key", u64);
    actions_dag.addInput(dummy, u8);

    /// Previously threw LOGICAL_ERROR "Left and right columns have same names".
    JoinExpressionActions expression_actions(left_header, right_header, std::move(actions_dag));

    /// Source attribution must still be unambiguous: the key columns map to their own sides, and the
    /// two duplicate dummy inputs are split left/right by position so every input is exactly one side.
    EXPECT_TRUE(expression_actions.findNode("left_key", /*is_input=*/ true).fromLeft());
    EXPECT_TRUE(expression_actions.findNode("right_key", /*is_input=*/ true).fromRight());

    const auto & inputs = expression_actions.getActionsDAG()->getInputs();
    ASSERT_EQ(inputs.size(), 4u);
    /// inputs[1] is the left dummy, inputs[3] is the right dummy.
    EXPECT_TRUE(JoinActionRef(inputs[1], expression_actions).fromLeft());
    EXPECT_TRUE(JoinActionRef(inputs[3], expression_actions).fromRight());
}

/// A column present in only one header keeps mapping to that side (the common case must not regress).
TEST(JoinExpressionActions, DistinctColumnNamesMapToTheirSide)
{
    const auto u64 = std::make_shared<DataTypeUInt64>();

    Block left_header{makeColumn("a", u64), makeColumn("b", u64)};
    Block right_header{makeColumn("c", u64)};

    ActionsDAG actions_dag;
    actions_dag.addInput("a", u64);
    actions_dag.addInput("b", u64);
    actions_dag.addInput("c", u64);

    JoinExpressionActions expression_actions(left_header, right_header, std::move(actions_dag));

    EXPECT_TRUE(expression_actions.findNode("a", /*is_input=*/ true).fromLeft());
    EXPECT_TRUE(expression_actions.findNode("b", /*is_input=*/ true).fromLeft());
    EXPECT_TRUE(expression_actions.findNode("c", /*is_input=*/ true).fromRight());
}

/// A name unique to one header is always resolved by name, even when JoinStepLogical::preCalculateKeys
/// has appended computed key inputs so the input order is no longer [left columns..., right columns...].
/// Here the inputs are [left id, right id, left key, right key] while left_header.columns() == 2.
TEST(JoinExpressionActions, InterleavedInputsWithUniqueNamesMapByName)
{
    const auto u64 = std::make_shared<DataTypeUInt64>();

    Block left_header{makeColumn("l_id", u64), makeColumn("l_key", u64)};
    Block right_header{makeColumn("r_id", u64), makeColumn("r_key", u64)};

    ActionsDAG actions_dag;
    actions_dag.addInput("l_id", u64);
    actions_dag.addInput("r_id", u64);
    actions_dag.addInput("l_key", u64);
    actions_dag.addInput("r_key", u64);

    JoinExpressionActions expression_actions(left_header, right_header, std::move(actions_dag));

    EXPECT_TRUE(expression_actions.findNode("l_id", /*is_input=*/ true).fromLeft());
    EXPECT_TRUE(expression_actions.findNode("l_key", /*is_input=*/ true).fromLeft());
    EXPECT_TRUE(expression_actions.findNode("r_id", /*is_input=*/ true).fromRight());
    EXPECT_TRUE(expression_actions.findNode("r_key", /*is_input=*/ true).fromRight());
}

/// A duplicate name must NOT be silently mis-attributed by position when the input layout is not the
/// canonical [left columns..., right columns...] (e.g. after preCalculateKeys interleaves key inputs).
/// Such a plan is not produced today (a join carrying the duplicate dummy has no equality keys, so
/// preCalculateKeys does not run), so rather than guess a side we reject it explicitly. In debug and
/// sanitizer builds a LOGICAL_ERROR aborts, so this is a death test there; in release it is thrown.
namespace
{
void buildDuplicateNameWithNonCanonicalLayout()
{
    const auto u64 = std::make_shared<DataTypeUInt64>();
    const String dup = "dup";

    /// left_header.columns() == 2, so position would treat inputs[0..1] as left; but the real layout
    /// here is [left dup, right dup, left key, right key], so position is meaningless.
    Block left_header{makeColumn(dup, u64), makeColumn("l_key", u64)};
    Block right_header{makeColumn(dup, u64), makeColumn("r_key", u64)};

    ActionsDAG actions_dag;
    actions_dag.addInput(dup, u64);
    actions_dag.addInput(dup, u64);
    actions_dag.addInput("l_key", u64);
    actions_dag.addInput("r_key", u64);

    JoinExpressionActions(left_header, right_header, std::move(actions_dag));
}
}

#ifdef DEBUG_OR_SANITIZER_BUILD
TEST(JoinExpressionActionsDeathTest, DuplicateNameWithNonCanonicalLayoutThrows)
{
    EXPECT_DEATH(buildDuplicateNameWithNonCanonicalLayout(), ".*Cannot determine the source side.*");
}
#else
TEST(JoinExpressionActions, DuplicateNameWithNonCanonicalLayoutThrows)
{
    EXPECT_THROW(buildDuplicateNameWithNonCanonicalLayout(), DB::Exception);
}
#endif
