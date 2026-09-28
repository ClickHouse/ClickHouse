#include <gtest/gtest.h>

#include <numeric>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>

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

/// Input header (a, b, c) filtered by `f`, computed from a. The DAG outputs are (f, x) where x aliases
/// b, so c is a pass-through. `remove_filter_column` decides whether f shows up in the output header.
std::unique_ptr<FilterStep> makeFilterStep(bool remove_filter_column)
{
    auto input_header = std::make_shared<const Block>(Block{column("a"), column("b"), column("c")});

    ActionsDAG dag;
    const auto & a = dag.addInput(column("a"));
    const auto & b = dag.addInput(column("b"));
    dag.getOutputs() = {&dag.addAlias(a, "f"), &dag.addAlias(b, "x")};

    return std::make_unique<FilterStep>(input_header, std::move(dag), "f", remove_filter_column);
}

template <typename Step>
void checkAnswersMatch(
    const std::function<std::unique_ptr<Step>()> & make_step,
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

    EXPECT_EQ(predicted.step_changed, applied.step_changed);
    EXPECT_EQ(predicted.inputs_changed, applied.inputs_changed);
    EXPECT_EQ(predicted.required_input_positions, applied.required_input_positions);
    EXPECT_EQ(predicted.kept_output_positions, applied.kept_output_positions);
    EXPECT_EQ(predicted.added_output_count, applied.added_output_count);
}

}

TEST(ExpressionStepRequiredColumns, MatchesRemoveUnusedColumns)
{
    for (bool remove_inputs : {true, false})
    {
        /// Keep one DAG output, the other output and the pass-through go away.
        checkAnswersMatch<ExpressionStep>(makeStep, {0}, remove_inputs);
        /// Keep a DAG output and the pass-through.
        checkAnswersMatch<ExpressionStep>(makeStep, {0, 2}, remove_inputs);
        /// Keep the pass-through only.
        checkAnswersMatch<ExpressionStep>(makeStep, {2}, remove_inputs);
        /// Keep everything: nothing to do.
        checkAnswersMatch<ExpressionStep>(makeStep, {0, 1, 2}, remove_inputs);

        checkAnswersMatch<ExpressionStep>(makeStepWithDuplicateNames, {0}, remove_inputs);
        checkAnswersMatch<ExpressionStep>(makeStepWithDuplicateNames, {1}, remove_inputs);
    }
}

TEST(FilterStepRequiredColumns, MatchesRemoveUnusedColumns)
{
    for (bool remove_inputs : {true, false})
    {
        /// Output header is (x, c): the filter column is erased, so the caller's positions have to be
        /// mapped back over it.
        const auto make_without_filter_column = [] { return makeFilterStep(/*remove_filter_column=*/true); };
        checkAnswersMatch<FilterStep>(make_without_filter_column, {0}, remove_inputs);
        checkAnswersMatch<FilterStep>(make_without_filter_column, {1}, remove_inputs);
        checkAnswersMatch<FilterStep>(make_without_filter_column, {0, 1}, remove_inputs);

        /// Output header is (f, x, c). Not asking for f makes the step drop it from the header, while
        /// the DAG still has to compute it to filter.
        const auto make_with_filter_column = [] { return makeFilterStep(/*remove_filter_column=*/false); };
        checkAnswersMatch<FilterStep>(make_with_filter_column, {0}, remove_inputs);
        checkAnswersMatch<FilterStep>(make_with_filter_column, {1}, remove_inputs);
        checkAnswersMatch<FilterStep>(make_with_filter_column, {2}, remove_inputs);
        checkAnswersMatch<FilterStep>(make_with_filter_column, {0, 1, 2}, remove_inputs);
    }
}

TEST(FilterStepRequiredColumns, KeepsTheFilterInputAndDropsTheRest)
{
    const auto step = makeFilterStep(/*remove_filter_column=*/true);

    /// Output header is (x, c). Asking for c only still needs a, because the filter reads it.
    const auto result = step->getRequiredColumns({1}, /*remove_inputs=*/true);
    ASSERT_TRUE(result.step_changed);
    ASSERT_TRUE(result.inputs_changed);
    ASSERT_EQ(result.required_input_positions.size(), 1u);
    EXPECT_EQ(result.required_input_positions.front(), std::vector<size_t>({0, 2}));

    /// The question left the step alone, so applying it now must give the same answer.
    const auto applied = step->removeUnusedColumns({1}, /*remove_inputs=*/true);
    EXPECT_EQ(applied.required_input_positions, result.required_input_positions);
    EXPECT_EQ(step->getOutputHeader()->dumpNames(), "c");
}

TEST(ExpressionStepRequiredColumns, ReportsThePositionEachInputReads)
{
    const auto step = makeStep();

    /// Output 1 is an alias of b, which reads header position 1.
    const auto result = step->getRequiredColumns({1}, /*remove_inputs=*/true);
    ASSERT_TRUE(result.step_changed);
    ASSERT_TRUE(result.inputs_changed);
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
    ASSERT_TRUE(result.step_changed);
    ASSERT_TRUE(result.inputs_changed);
    ASSERT_EQ(result.required_input_positions.size(), 1u);
    EXPECT_EQ(result.required_input_positions.front(), std::vector<size_t>({1}));
}

/// Asked for every output, the step changes nothing, and says so with every position rather than with
/// empty lists.
TEST(ExpressionStepRequiredColumns, ReportsEverythingWhenNothingChanges)
{
    const auto step = makeStep();
    const auto output_columns = step->getOutputHeader()->columns();
    const auto input_columns = step->getInputHeaders().front()->columns();

    std::vector<size_t> all_outputs(output_columns);
    std::iota(all_outputs.begin(), all_outputs.end(), 0);

    const auto result = step->getRequiredColumns(all_outputs, /*remove_inputs=*/false);
    EXPECT_FALSE(result.step_changed);
    EXPECT_FALSE(result.inputs_changed);
    EXPECT_EQ(result.kept_output_positions, all_outputs);
    EXPECT_EQ(result.added_output_count, 0u);
    ASSERT_EQ(result.required_input_positions.size(), 1u);
    EXPECT_EQ(result.required_input_positions.front().size(), input_columns);
}
