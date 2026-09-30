#include <gtest/gtest.h>

#include <algorithm>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>

#include <fmt/ranges.h>

using namespace DB;

namespace
{

/// The sorted positions in `[0, count)` that are not in the sorted `positions`.
std::vector<size_t> complementPositions(size_t count, const std::vector<size_t> & positions)
{
    std::vector<size_t> complement;
    for (size_t position = 0; position < count; ++position)
        if (!std::ranges::binary_search(positions, position))
            complement.push_back(position);
    return complement;
}

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

/// A child that kept `positions` of `header`, and appended `appended` after them.
IQueryPlanStep::PrunedInput prunedChild(const Block & header, const std::vector<size_t> & positions, const ColumnsWithTypeAndName & appended = {})
{
    Block pruned_header;
    for (size_t position : positions)
        pruned_header.insert(header.getByPosition(position));
    for (const auto & column : appended)
        pruned_header.insert(column);
    return {complementPositions(header.columns(), positions), std::make_shared<const Block>(std::move(pruned_header))};
}

/// The columns at `positions` of `header`, by name.
String namesAt(const Block & header, const std::vector<size_t> & positions)
{
    Names names;
    for (size_t position : positions)
        names.push_back(header.getByPosition(position).name);
    return fmt::format("{}", fmt::join(names, ", "));
}

/// Tells the step that only the columns at `required_output_positions` are needed, asks it what it does not
/// need, prunes it with a child that drops exactly that, and checks that the step outputs exactly the
/// required columns and reads exactly what it did not give up.
template <typename Step>
void checkPruning(const std::function<std::unique_ptr<Step>()> & make_step, const std::vector<size_t> & required_output_positions)
{
    const auto step = make_step();
    const auto before = step->getOutputHeader()->dumpStructure();
    const auto dag_before = step->getExpression().dumpDAG();
    const auto output_before = *step->getOutputHeader();
    const auto input_header = *step->getInputHeaders().front();
    const auto unneeded_output_positions = complementPositions(output_before.columns(), required_output_positions);

    const auto unneeded = step->getUnneededColumns(unneeded_output_positions);

    /// The question must not change the step.
    EXPECT_EQ(step->getOutputHeader()->dumpStructure(), before);
    EXPECT_EQ(step->getExpression().dumpDAG(), dag_before);
    ASSERT_EQ(unneeded.size(), 1u);

    const auto required_inputs = complementPositions(input_header.columns(), unneeded.front());
    const auto applied = step->removeUnusedColumns(unneeded_output_positions, {prunedChild(input_header, required_inputs)});

    EXPECT_EQ(applied.dropped_output_positions, unneeded_output_positions);
    EXPECT_EQ(applied.step_changed, !unneeded_output_positions.empty());
    EXPECT_EQ(step->getInputHeaders().front()->dumpNames(), namesAt(input_header, required_inputs));
    EXPECT_EQ(step->getOutputHeader()->dumpNames(), namesAt(output_before, required_output_positions));
}

}

TEST(ExpressionStepRequiredColumns, PrunesToTheRequiredColumns)
{
    /// Keep one DAG output, the other output and the pass-through go away.
    checkPruning<ExpressionStep>(makeStep, {0});
    /// Keep a DAG output and the pass-through.
    checkPruning<ExpressionStep>(makeStep, {0, 2});
    /// Keep the pass-through only.
    checkPruning<ExpressionStep>(makeStep, {2});
    /// Keep everything: nothing to do.
    checkPruning<ExpressionStep>(makeStep, {0, 1, 2});

    checkPruning<ExpressionStep>(makeStepWithDuplicateNames, {0});
    checkPruning<ExpressionStep>(makeStepWithDuplicateNames, {1});
}

TEST(FilterStepRequiredColumns, PrunesToTheRequiredColumns)
{
    /// Output header is (x, c): the filter column is erased, so the caller's positions have to be mapped
    /// back over it.
    const auto make_without_filter_column = [] { return makeFilterStep(/*remove_filter_column=*/true); };
    checkPruning<FilterStep>(make_without_filter_column, {0});
    checkPruning<FilterStep>(make_without_filter_column, {1});
    checkPruning<FilterStep>(make_without_filter_column, {0, 1});

    /// Output header is (f, x, c). Not asking for f makes the step drop it from the header, while the DAG
    /// still has to compute it to filter.
    const auto make_with_filter_column = [] { return makeFilterStep(/*remove_filter_column=*/false); };
    checkPruning<FilterStep>(make_with_filter_column, {0});
    checkPruning<FilterStep>(make_with_filter_column, {1});
    checkPruning<FilterStep>(make_with_filter_column, {2});
    checkPruning<FilterStep>(make_with_filter_column, {0, 1, 2});
}

TEST(FilterStepRequiredColumns, KeepsTheFilterInputAndDropsTheRest)
{
    const auto step = makeFilterStep(/*remove_filter_column=*/true);
    const auto input_header = *step->getInputHeaders().front();

    /// Output header is (x, c). Without x, b is not needed, but a is, because the filter reads it.
    const auto unneeded = step->getUnneededColumns({0});
    ASSERT_EQ(unneeded.size(), 1u);
    EXPECT_EQ(unneeded.front(), std::vector<size_t>({1}));

    step->removeUnusedColumns({0}, {prunedChild(input_header, {0, 2})});
    EXPECT_EQ(step->getOutputHeader()->dumpNames(), "c");
    EXPECT_EQ(step->getInputHeaders().front()->dumpNames(), "a, c");
}

TEST(ExpressionStepRequiredColumns, ReportsThePositionEachInputReads)
{
    const auto step = makeStep();

    /// Output header is (x, y, c). Output 1 is an alias of b, which reads header position 1, so without the
    /// other outputs a and c are not needed.
    const auto unneeded = step->getUnneededColumns({0, 2});
    ASSERT_EQ(unneeded.size(), 1u);
    EXPECT_EQ(unneeded.front(), std::vector<size_t>({0, 2}));
}

TEST(ExpressionStepRequiredColumns, DuplicateNamesKeepTheirOwnPosition)
{
    const auto step = makeStepWithDuplicateNames();

    /// Output 1 is an alias of the second input, which reads header position 1, so without output 0 the
    /// first input is not needed. Resolving the surviving input by name would answer 1 and feed the
    /// expression the wrong column.
    const auto unneeded = step->getUnneededColumns({0});
    ASSERT_EQ(unneeded.size(), 1u);
    EXPECT_EQ(unneeded.front(), std::vector<size_t>({0}));
}

/// Asked for every output, the step needs every input, and pruning it with a child that did not change
/// changes nothing.
TEST(ExpressionStepRequiredColumns, ChangesNothingWhenEverythingIsRequired)
{
    const auto step = makeStep();
    const auto input_header = step->getInputHeaders().front();

    const auto unneeded = step->getUnneededColumns({});
    ASSERT_EQ(unneeded.size(), 1u);
    EXPECT_TRUE(unneeded.front().empty());

    const auto applied = step->removeUnusedColumns({}, {IQueryPlanStep::PrunedInput::unchanged(input_header)});
    EXPECT_FALSE(applied.step_changed);
    EXPECT_TRUE(applied.dropped_output_positions.empty());
}

/// A child can keep more than it was asked for - a FINAL read keeps its sorting key - and append columns
/// of its own - a join its dummy column. The step consumes them, and outputs the required columns only.
TEST(ExpressionStepRequiredColumns, ConsumesWhatTheChildKeepsBeyondTheAsk)
{
    const auto step = makeStep();
    const auto input_header = *step->getInputHeaders().front();

    /// Without y and c, the step needs only a; the child keeps a, b and c, and appends z.
    const auto unneeded = step->getUnneededColumns({1, 2});
    EXPECT_EQ(unneeded.front(), std::vector<size_t>({1, 2}));

    const auto applied = step->removeUnusedColumns({1, 2}, {prunedChild(input_header, {0, 1, 2}, {column("z")})});
    EXPECT_TRUE(applied.step_changed);
    EXPECT_EQ(step->getInputHeaders().front()->dumpNames(), "a, b, c, z");
    EXPECT_EQ(step->getOutputHeader()->dumpNames(), "x");
}

TEST(FilterStepRequiredColumns, ConsumesWhatTheChildKeepsBeyondTheAsk)
{
    const auto step = makeFilterStep(/*remove_filter_column=*/true);
    const auto input_header = *step->getInputHeaders().front();

    /// Output header is (x, c). Without c, the step needs a and b; the child keeps c as well.
    const auto unneeded = step->getUnneededColumns({1});
    EXPECT_EQ(unneeded.front(), std::vector<size_t>({2}));

    step->removeUnusedColumns({1}, {prunedChild(input_header, {0, 1, 2})});
    EXPECT_EQ(step->getInputHeaders().front()->dumpNames(), "a, b, c");
    EXPECT_EQ(step->getOutputHeader()->dumpNames(), "x");
}
