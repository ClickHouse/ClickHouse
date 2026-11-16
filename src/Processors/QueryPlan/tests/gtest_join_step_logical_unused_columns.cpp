#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

namespace
{

SharedHeader makeHeader(const Names & names)
{
    Block header;
    for (const auto & name : names)
    {
        auto type = std::make_shared<DataTypeUInt64>();
        header.insert({type->createColumn(), type, name});
    }
    return std::make_shared<const Block>(std::move(header));
}

/// A CROSS JOIN whose DAG reads `right_inputs` of the right side first and then `appended_left_inputs` of the left
/// side, as when a left input was added after the others, such as one consuming a column the left side appended. Every
/// input is an output, so the join reads nothing for itself.
std::unique_ptr<JoinStepLogical> makeJoin(const Names & left_inputs, const Names & right_inputs, const Names & appended_left_inputs)
{
    Names left_names = left_inputs;
    left_names.insert(left_names.end(), appended_left_inputs.begin(), appended_left_inputs.end());
    auto left_header = makeHeader(left_names);
    auto right_header = makeHeader(right_inputs);

    JoinExpressionActions expression_actions(*makeHeader(left_inputs), *right_header);
    for (const auto & name : appended_left_inputs)
        expression_actions.addInput(name, std::make_shared<DataTypeUInt64>(), 0);

    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);

    QueryPlanSerializationSettings settings;
    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator{},
        std::move(expression_actions),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION),
        SortingStep::Settings(settings));
}

std::vector<size_t> allPositions(const IQueryPlanStep & step)
{
    std::vector<size_t> positions(step.getOutputHeader()->columns());
    for (size_t position = 0; position < positions.size(); ++position)
        positions[position] = position;
    return positions;
}

}

/// The right side's only input comes right after the first left one, where the left header width does not point.
/// Nothing reads any output, so the join keeps the first column of each side, and only the appended left one goes.
TEST(JoinStepLogicalUnusedColumns, KeepsAColumnOfEachSideWhenInputsAreNotLeftThenRight)
{
    const auto join = makeJoin({"a"}, {"c"}, {"d"});
    ASSERT_TRUE(join->canRemoveUnusedColumns());

    const auto unneeded = join->getUnneededColumns(allPositions(*join));
    ASSERT_EQ(unneeded.size(), 2u);
    EXPECT_EQ(unneeded[0], std::vector<size_t>({1}));
    EXPECT_TRUE(unneeded[1].empty());
}

/// The only left input comes after the right one, so the first input of the join is a right one.
TEST(JoinStepLogicalUnusedColumns, KeepsTheLeftColumnWhenTheFirstInputIsARightOne)
{
    const auto join = makeJoin({}, {"c"}, {"d"});
    ASSERT_TRUE(join->canRemoveUnusedColumns());

    const auto unneeded = join->getUnneededColumns(allPositions(*join));
    ASSERT_EQ(unneeded.size(), 2u);
    EXPECT_TRUE(unneeded[0].empty());
    EXPECT_TRUE(unneeded[1].empty());
}
