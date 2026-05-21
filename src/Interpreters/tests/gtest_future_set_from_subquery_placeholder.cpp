#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <QueryPipeline/SizeLimits.h>

using namespace DB;

namespace
{

/// Build a one-column input header used by the source step.
SharedHeader makeHeader()
{
    return std::make_shared<const Block>(Block{
        ColumnWithTypeAndName(ColumnUInt64::create(), std::make_shared<DataTypeUInt64>(), "x")});
}

/// Build a `QueryPlan` consisting of a NullSource feeding an `ExpressionStep` whose
/// `ActionsDAG` exposes a `PLACEHOLDER` node as one of its outputs. This is the exact
/// shape `PlannerActionsVisitor::visitCorrelatedColumn` produces for a correlated
/// subquery, and the only shape under which `IQueryPlanStep::hasCorrelatedExpressions`
/// returns true for a non-source step.
std::unique_ptr<QueryPlan> makePlanWithPlaceholder()
{
    auto header = makeHeader();

    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::make_unique<ReadNothingStep>(header));

    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto & placeholder = dag.addPlaceholder("correlated_x", std::make_shared<DataTypeUInt64>());
    dag.getOutputs().push_back(&placeholder);

    plan->addStep(std::make_unique<ExpressionStep>(header, std::move(dag)));
    return plan;
}

/// Same shape but without a `PLACEHOLDER` — a plain `ExpressionStep` that re-exposes
/// the only input column. Used to verify the guard is targeted (it must not refuse
/// non-correlated plans).
std::unique_ptr<QueryPlan> makePlanWithoutPlaceholder()
{
    auto header = makeHeader();

    auto plan = std::make_unique<QueryPlan>();
    plan->addStep(std::make_unique<ReadNothingStep>(header));

    ActionsDAG dag(header->getColumnsWithTypeAndName());
    plan->addStep(std::make_unique<ExpressionStep>(header, std::move(dag)));
    return plan;
}

std::shared_ptr<FutureSetFromSubquery> wrapInFutureSet(std::unique_ptr<QueryPlan> plan)
{
    FutureSet::Hash hash{0, 0};
    return std::make_shared<FutureSetFromSubquery>(
        hash,
        /*ast=*/ nullptr,
        std::move(plan),
        /*external_table=*/ nullptr,
        /*external_table_set=*/ nullptr,
        /*transform_null_in=*/ false,
        SizeLimits{},
        /*max_size_for_index=*/ 0);
}

}

/// Regression test for issue #105501 (STID 1611-343c) — `Trying to execute PLACEHOLDER action`.
///
/// `FutureSetFromSubquery::build` is reached by `addCreatingSetsStep`,
/// `addCreatingSetsTransform`, and `DelayedCreatingSetsStep::makePlansForSets`. Before the fix
/// it did not short-circuit on plans whose source carried `PLACEHOLDER` actions, so any of
/// those callers could wrap a correlated-subquery plan in a `CreatingSetStep`, build a pipeline,
/// and crash on the first chunk in `ExpressionActions::execute`.
///
/// The fix mirrors the existing guard in `buildSetInplace` / `buildOrderedSetInplace`. This test
/// exercises the guard directly: it constructs a `FutureSetFromSubquery` whose source plan
/// contains a `PLACEHOLDER` and asserts `build()` returns `nullptr`. Removing the guard makes the
/// assertion fail (the call returns a non-null plan with a `CreatingSetStep` on top).
///
/// SQL-level reproduction is not stable because the analyzer rejects correlated `IN`/CTE
/// subqueries before they reach the planner (different error in current master), so a unit test
/// is the only way to exercise this code path reliably.
TEST(FutureSetFromSubquery, BuildSkipsPlanWithPlaceholder)
{
    auto plan = makePlanWithPlaceholder();

    // Sanity: the plan's root step really exposes a correlated expression.
    ASSERT_TRUE(plan->getRootNode()->step->hasCorrelatedExpressions());

    auto future_set = wrapInFutureSet(std::move(plan));

    auto built = future_set->build(SizeLimits{}, /*prepared_sets_cache=*/ nullptr);
    EXPECT_EQ(built, nullptr);
}

/// Targeted-guard sanity: a non-correlated source plan must still be wrapped in a
/// `CreatingSetStep` by `build()` — the new guard must not over-fire.
TEST(FutureSetFromSubquery, BuildKeepsPlanWithoutPlaceholder)
{
    auto plan = makePlanWithoutPlaceholder();

    ASSERT_FALSE(plan->getRootNode()->step->hasCorrelatedExpressions());

    auto future_set = wrapInFutureSet(std::move(plan));

    auto built = future_set->build(SizeLimits{}, /*prepared_sets_cache=*/ nullptr);
    ASSERT_NE(built, nullptr);
    EXPECT_EQ(built->getRootNode()->step->getName(), "CreatingSet");
}
