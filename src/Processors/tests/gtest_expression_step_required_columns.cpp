#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>

using namespace DB;

namespace
{

ColumnWithTypeAndName column(const String & name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return {type->createColumn(), type, name};
}

/// Input header (a, b, c) and a DAG reading a and b, so c is a pass-through.
/// Output header is (x, y, c).
std::unique_ptr<ExpressionStep> makeStep()
{
    auto input_header = std::make_shared<const Block>(Block{column("a"), column("b"), column("c")});

    ActionsDAG dag;
    const auto & a = dag.addInput(column("a"));
    const auto & b = dag.addInput(column("b"));
    dag.getOutputs() = {&dag.addAlias(a, "x"), &dag.addAlias(b, "y")};

    return std::make_unique<ExpressionStep>(input_header, std::move(dag));
}

/// Two header columns share a name and the DAG reads both, so a position cannot be recovered from the
/// name alone. The second input is the one that survives below.
std::unique_ptr<ExpressionStep> makeStepWithDuplicateNames()
{
    auto input_header = std::make_shared<const Block>(Block{column("d"), column("d")});

    ActionsDAG dag;
    const auto & first = dag.addInput(column("d"));
    const auto & second = dag.addInput(column("d"));
    dag.getOutputs() = {&dag.addAlias(first, "first"), &dag.addAlias(second, "second")};

    return std::make_unique<ExpressionStep>(input_header, std::move(dag));
}

void checkAnswersMatch(
    const std::function<std::unique_ptr<ExpressionStep>()> & make_step,
    const std::vector<size_t> & required_output_positions,
    bool remove_inputs)
{
    const auto step = make_step();
    const auto before = step->getOutputHeader()->dumpStructure();
    const auto dag_before = step->getExpression().dumpDAG();

    const auto predicted = step->getRequiredColumns(required_output_positions, remove_inputs);

    /// The question must not change the step.
    EXPECT_EQ(step->getOutputHeader()->dumpStructure(), before);
    EXPECT_EQ(step->getExpression().dumpDAG(), dag_before);

    const auto applied = make_step()->removeUnusedColumns(required_output_positions, remove_inputs);

    EXPECT_EQ(predicted.changed, applied.changed);
    EXPECT_EQ(predicted.required_input_positions, applied.required_input_positions);
    EXPECT_EQ(predicted.kept_output_positions, applied.kept_output_positions);
}

}

TEST(ExpressionStepRequiredColumns, MatchesRemoveUnusedColumns)
{
    for (bool remove_inputs : {true, false})
    {
        /// Keep one DAG output, the other output and the pass-through go away.
        checkAnswersMatch(makeStep, {0}, remove_inputs);
        /// Keep a DAG output and the pass-through.
        checkAnswersMatch(makeStep, {0, 2}, remove_inputs);
        /// Keep the pass-through only.
        checkAnswersMatch(makeStep, {2}, remove_inputs);
        /// Keep everything: nothing to do.
        checkAnswersMatch(makeStep, {0, 1, 2}, remove_inputs);

        checkAnswersMatch(makeStepWithDuplicateNames, {0}, remove_inputs);
        checkAnswersMatch(makeStepWithDuplicateNames, {1}, remove_inputs);
    }
}

TEST(ExpressionStepRequiredColumns, ReportsThePositionEachInputReads)
{
    const auto step = makeStep();

    /// Output 1 is an alias of b, which reads header position 1.
    const auto result = step->getRequiredColumns({1}, /*remove_inputs=*/true);
    ASSERT_TRUE(result.changed);
    ASSERT_EQ(result.required_input_positions.size(), 1u);
    EXPECT_EQ(result.required_input_positions.front(), std::vector<size_t>({1}));
    EXPECT_EQ(result.kept_output_positions, std::vector<size_t>({1}));
}

TEST(ExpressionStepRequiredColumns, DuplicateNamesKeepTheirOwnPosition)
{
    const auto step = makeStepWithDuplicateNames();

    /// Output 1 is an alias of the second input, which reads header position 1. Resolving the surviving
    /// input by name would answer 0 and feed the expression the wrong column.
    const auto result = step->getRequiredColumns({1}, /*remove_inputs=*/true);
    ASSERT_TRUE(result.changed);
    ASSERT_EQ(result.required_input_positions.size(), 1u);
    EXPECT_EQ(result.required_input_positions.front(), std::vector<size_t>({1}));
}
