#include <gtest/gtest.h>

#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/typeid_cast.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/Joins.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/JoinStepLogical.h>

using namespace DB;

namespace
{

ColumnWithTypeAndName plainUInt32(const String & name)
{
    return ColumnWithTypeAndName(std::make_shared<DataTypeUInt32>(), name);
}

ColumnWithTypeAndName constUInt32(const String & name, UInt32 value)
{
    auto type = std::make_shared<DataTypeUInt32>();
    return ColumnWithTypeAndName(type->createColumnConst(0, value), type, name);
}

/// Built positionally rather than through `Block::insert`, which is how a header carrying several
/// columns of one name arrives in practice: `insert` admits a duplicate after a structure check that
/// skips comparing constant values for empty columns, and header constants are empty.
Block headerOf(ColumnsWithTypeAndName columns)
{
    return Block(std::move(columns));
}

/// Builds a `JoinStepLogical` whose `ON` clause is `left[left_index] >= right[right_index]` conjoined
/// with a left-only comparison over the LAST left column, and hands out the pushed-down filter for the
/// left side. The second conjunct keeps the filter buildable when the first folds to a constant.
struct JoinFixture
{
    SharedHeader left_header;
    SharedHeader right_header;
    std::unique_ptr<JoinStepLogical> step;

    JoinFixture(
        ColumnsWithTypeAndName left_columns,
        ColumnsWithTypeAndName right_columns,
        size_t right_index,
        size_t left_index = 0)
    {
        left_header = std::make_shared<const Block>(headerOf(std::move(left_columns)));
        right_header = std::make_shared<const Block>(headerOf(std::move(right_columns)));

        /// Built directly rather than through SQL because the analyzer qualifies columns to
        /// `__table1.x`, so a cross-side name collision is unreachable from a query but reachable here:
        /// this constructor records one relation per input without comparing names.
        JoinExpressionActions expression_actions(*left_header, *right_header);

        /// The constructor appends the left inputs first, then the right ones, in header order, so the
        /// inputs are addressed positionally: when names collide, a name lookup cannot tell them apart.
        auto & dag = *expression_actions.getActionsDAG();
        const auto & inputs = dag.getInputs();
        const auto * left_key = inputs.at(left_index);
        /// The left-only conjunct reads the LAST left column, which is never the key, so it stays
        /// independent of `left_index`.
        const auto * left_only = inputs.at(left_header->columns() - 1);
        const auto * right_key = inputs.at(left_header->columns() + right_index);

        auto context = getContext().context;
        auto greater_or_equals = FunctionFactory::instance().get("greaterOrEquals", context);
        auto greater = FunctionFactory::instance().get("greater", context);

        auto zero = constUInt32("zero", 0);
        const auto & zero_node
            = dag.addColumn(typeid_cast<const ColumnConst *>(zero.column.get())->getPtr(), zero.type, "zero");
        const auto & key_cmp = dag.addFunction(greater_or_equals, {left_key, right_key}, "key_cmp");
        const auto & left_only_cmp = dag.addFunction(greater, {left_only, &zero_node}, "left_only_cmp");

        /// `JoinStepLogical` derives its output header from the expression DAG's outputs, so at least
        /// one is required. The left inputs stand in for the columns the join would return.
        dag.getOutputs() = {left_key, left_only};

        JoinOperator join_operator(JoinKind::Inner, JoinStrictness::All, JoinLocality::Unspecified);
        join_operator.expression = {
            JoinActionRef(&key_cmp, expression_actions),
            JoinActionRef(&left_only_cmp, expression_actions),
        };

        step = std::make_unique<JoinStepLogical>(
            left_header,
            right_header,
            std::move(join_operator),
            std::move(expression_actions),
            /* actions_after_join_= */ std::vector<const ActionsDAG::Node *>{left_key, left_only},
            JoinSettings(context->getSettingsRef()),
            SortingStep::Settings(context->getSettingsRef()));
    }

    std::optional<ActionsDAG::ActionsForFilterPushDown> leftFilter() const
    {
        return step->getFilterActions(JoinTableSide::Left, left_header);
    }

    /// True when the `ON` clause still holds a condition, i.e. nothing was extracted from it.
    bool onClauseStillHasConditions() const { return !step->getJoinOperator().expression.empty(); }

    size_t onClauseConditionCount() const { return step->getJoinOperator().expression.size(); }
};

/// Runs `filter` over a block of `left_key_values.size()` rows and returns the filter column's values.
/// Probe values sit at header position `key_position`; every other column carries 95, above both the
/// left-only conjunct's zero and every bound below, so a filter reading the wrong column accepts all.
std::vector<UInt8> runFilter(
    const ActionsDAG::ActionsForFilterPushDown & filter,
    const Block & left_header,
    const std::vector<UInt32> & left_key_values,
    size_t key_position = 0)
{
    /// Executing the filter is the oracle for every assertion below, because DAG SHAPE cannot answer the
    /// question: `createActionsForConjunction` adds one input per stream-header column regardless of what
    /// the predicate depends on, so counting inputs can never detect a folded comparison.
    auto dag = filter.dag.clone();
    const auto & filter_column_name = dag.getOutputs()[filter.filter_pos]->result_name;
    ExpressionActions actions(std::move(dag));

    /// Built positionally, like `headerOf`: `Block::insert` maintains a name index that cannot hold two
    /// positions for one name, and the probe block mirrors a header that carries duplicate names.
    ColumnsWithTypeAndName probe_columns;
    for (size_t position = 0; position < left_header.columns(); ++position)
    {
        const auto & header_column = left_header.getByPosition(position);
        auto column = ColumnUInt32::create();
        for (UInt32 left_key_value : left_key_values)
            column->insertValue(position == key_position ? left_key_value : 95);
        probe_columns.emplace_back(std::move(column), header_column.type, header_column.name);
    }
    Block block(std::move(probe_columns));

    size_t num_rows = left_key_values.size();
    actions.execute(block, num_rows, /* dry_run= */ false, /* allow_duplicates_in_input= */ true);

    const auto & filter_column = block.getByName(filter_column_name).column;
    std::vector<UInt8> result;
    for (size_t row = 0; row < left_key_values.size(); ++row)
        result.push_back(filter_column->getUInt(row) != 0);
    return result;
}

}

/// Which references belong to the OPPOSITE relation is decided from each input's SOURCE RELATION, and
/// the condition is then removed from `ON` because the produced filter enforces it. Deciding by NAME
/// would fold `x >= x` to the tautology `50 >= 50` while still erasing it, enforcing it nowhere.
GTEST_TEST(JoinOnConstantPushdown, SharedColumnNameFilterStillComparesAgainstLeftColumn)
{
    tryRegisterFunctions();
    JoinFixture fixture(
        /* left_columns= */ {plainUInt32("x"), plainUInt32("y")},
        /* right_columns= */ {constUInt32("x", 50)},
        /* right_index= */ 0);

    auto filter = fixture.leftFilter();
    ASSERT_TRUE(filter.has_value()) << "the range predicate against a constant opposite side must be pushed down";

    /// `x >= 50`: the row below the bound must be rejected.
    EXPECT_EQ(runFilter(*filter, *fixture.left_header, {40, 60}), (std::vector<UInt8>{0, 1}))
        << "the pushed-down filter must still compare the LEFT column against the right side's value. "
        << "Resolving the substitution by name rather than by source relation replaces the left "
        << "reference too, folding the comparison into a tautology that accepts every row, while the "
        << "condition is still erased from the ON clause. Filter DAG:\n"
        << filter->dag.dumpDAG();
}

/// Companion positive case: with DISTINCT names the substitution must fire, so the assertion above is
/// not satisfied by simply refusing every candidate.
GTEST_TEST(JoinOnConstantPushdown, DistinctColumnNamesStillSubstitute)
{
    tryRegisterFunctions();
    JoinFixture fixture(
        /* left_columns= */ {plainUInt32("l_x"), plainUInt32("y")},
        /* right_columns= */ {constUInt32("r_x", 50)},
        /* right_index= */ 0);

    auto filter = fixture.leftFilter();
    ASSERT_TRUE(filter.has_value()) << "a range predicate against a constant opposite side must be pushed down";
    EXPECT_EQ(runFilter(*filter, *fixture.left_header, {40, 60}), (std::vector<UInt8>{0, 1}))
        << "the pushed-down filter must compare the left column against 50. Filter DAG:\n"
        << filter->dag.dumpDAG();
    EXPECT_FALSE(fixture.onClauseStillHasConditions())
        << "both conditions were pushed down, so the ON clause must be empty";
}

/// An opposite-side input's value must come from its OWN header occurrence: two right columns share the
/// name `x` and the predicate references the SECOND. Resolution is positional, so this must substitute.
GTEST_TEST(JoinOnConstantPushdown, DuplicateOppositeNameUsesItsOwnOccurrence)
{
    tryRegisterFunctions();
    JoinFixture fixture(
        /* left_columns= */ {plainUInt32("l_x"), plainUInt32("y")},
        /* right_columns= */ {constUInt32("x", 50), constUInt32("x", 80)},
        /* right_index= */ 1);

    auto filter = fixture.leftFilter();
    ASSERT_TRUE(filter.has_value())
        << "the opposite-side constant substitution must fire for a duplicate-named opposite header: "
        << "the referenced occurrence is resolved by position, so declining it is a regression";

    /// Asserted before the value check so that "not extracted at all" and "extracted but bound to the
    /// wrong occurrence" are distinguishable failures.
    EXPECT_FALSE(fixture.onClauseStillHasConditions())
        << "both conditions were pushed down, so the ON clause must be empty";

    /// `l_x >= 80`: 60 and 70 are below the referenced bound and must be rejected. Resolving by name
    /// would compare against 50 and accept both.
    EXPECT_EQ(runFilter(*filter, *fixture.left_header, {60, 70, 90}), (std::vector<UInt8>{0, 0, 1}))
        << "the filter must compare against the referenced occurrence's value (80), not the first "
        << "column that happens to share its name (50). Filter DAG:\n"
        << filter->dag.dumpDAG();
}

/// The mirror image on the CURRENT side: the predicate references the SECOND left `x`, but the filter is
/// bound to the stream BY NAME, so the clone's single `x` input binds to header position 0 and the
/// condition is erased from `ON` regardless, leaving the intended comparison enforced nowhere.
GTEST_TEST(JoinOnConstantPushdown, DuplicateCurrentSideNameDeclinesSubstitution)
{
    /// A regression rather than an inherited gap: master's `belongs_to_side` is false for a
    /// both-relations predicate, so master keeps it in `ON` where the join enforces it. The
    /// single-sided case, which `belongs_to_side` extracts on master too, is pre-existing.
    tryRegisterFunctions();
    JoinFixture fixture(
        /* left_columns= */ {plainUInt32("x"), plainUInt32("x"), plainUInt32("y")},
        /* right_columns= */ {constUInt32("r_bound", 80)},
        /* right_index= */ 0,
        /* left_index= */ 1);

    auto filter = fixture.leftFilter();
    /// The left-only conjunct is pushed either way, so the filter itself is not the observable.
    ASSERT_TRUE(filter.has_value()) << "the left-only conjunct must still be pushed down";

    if (fixture.onClauseStillHasConditions())
    {
        /// The expected outcome: the range predicate was declined and the join enforces it.
        EXPECT_EQ(fixture.onClauseConditionCount(), 1u)
            << "only the range predicate may remain; the left-only conjunct is pushed down";
        return;
    }

    /// It was extracted, so the produced filter must compare the REFERENCED occurrence. The probe
    /// values go to header position 1; position 0 carries 95, so a filter bound to the first `x`
    /// accepts every row.
    EXPECT_EQ(runFilter(*filter, *fixture.left_header, {60, 70, 90}, /* key_position= */ 1), (std::vector<UInt8>{0, 0, 1}))
        << "the pushed-down filter must compare the referenced occurrence of `x` against 80. Binding "
        << "it to the first column sharing that name compares an unrelated value while the condition "
        << "is erased from the ON clause, so the intended comparison is enforced nowhere. Filter DAG:\n"
        << filter->dag.dumpDAG();
}

/// A current-side name duplicated only in a ONE-SIDED conjunct must not veto the whole filter: the
/// duplicate-name guard is a property of the conjunct being substituted. Vetoing on the combined
/// predicate also discarded the one-sided pushdown master performs without any substitution.
GTEST_TEST(JoinOnConstantPushdown, DuplicateNameOutsideSubstitutedConjunctStillPushesDown)
{
    tryRegisterFunctions();
    JoinFixture fixture(
        /* left_columns= */ {plainUInt32("y"), plainUInt32("x"), plainUInt32("x")},
        /* right_columns= */ {constUInt32("r_lo", 50)},
        /* right_index= */ 0,
        /* left_index= */ 0);

    auto filter = fixture.leftFilter();
    ASSERT_TRUE(filter.has_value())
        << "the left-only conjunct over the duplicated `x` is pushed down on master without any "
        << "substitution, so a veto raised by its duplicate name is a regression";

    /// `y >= 50` over probe values at header position 0. The duplicated `x` columns carry 95, above the
    /// left-only conjunct's zero, so they accept every row and the observable is the substituted bound.
    EXPECT_EQ(runFilter(*filter, *fixture.left_header, {40, 60}), (std::vector<UInt8>{0, 1}))
        << "the substituted conjunct must still compare `y` against 50. Filter DAG:\n"
        << filter->dag.dumpDAG();
}
