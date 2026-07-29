#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

/// Smallest plan that owns a root node: a single source step.
QueryPlan makeSourcePlan()
{
    QueryPlan plan;
    plan.addStep(std::make_unique<ReadNothingStep>(makeHeader()));
    return plan;
}

}

/// `max_threads` and `concurrency_control` are plan-level, not part of the node tree, so every
/// boundary that derives a plan from another one has to carry them over explicitly. They are
/// invisible in query results (they only shape pipeline scheduling), so assert them directly.
///
/// Without the carry-over the derived plan runs with the default thread fan-out and no
/// concurrency control: `buildQueryPipeline` applies `max_threads` only when it is non-zero
/// (`limitMaxThreads`) and then forwards `concurrency_control` to the pipeline verbatim.

TEST(QueryPlanExecutionLimits, ClonePreservesLimitsAndResources)
{
    auto source = makeSourcePlan();
    source.setMaxThreads(4);
    source.setConcurrencyControl(true);
    source.addInterpreterContext(Context::createCopy(getContext().context));

    auto clone = source.clone();

    EXPECT_EQ(clone.getMaxThreads(), 4u);
    EXPECT_TRUE(clone.getConcurrencyControl());
    /// `append` shares the handle rather than moving it, so the source keeps its own copy.
    EXPECT_EQ(clone.getInterpretersContexts().size(), 1u);
    EXPECT_EQ(source.getInterpretersContexts().size(), 1u);
}

TEST(QueryPlanExecutionLimits, ExtractSubplanPreservesLimitsAndResources)
{
    auto source = makeSourcePlan();
    source.setMaxThreads(4);
    source.setConcurrencyControl(true);
    source.addInterpreterContext(Context::createCopy(getContext().context));

    auto subplan = source.extractSubplan(source.getRootNode());

    EXPECT_EQ(subplan.getMaxThreads(), 4u);
    EXPECT_TRUE(subplan.getConcurrencyControl());
    EXPECT_EQ(subplan.getInterpretersContexts().size(), 1u);
    EXPECT_EQ(source.getInterpretersContexts().size(), 1u);
}

/// `unitePlans` already merged `max_threads` across the children; `concurrency_control` has to be
/// OR-ed the same way, so a united plan honours it whenever any child asked for it.
TEST(QueryPlanExecutionLimits, UnitePlansMergesLimitsFromChildren)
{
    auto without = std::make_unique<QueryPlan>(makeSourcePlan());
    without->setMaxThreads(2);
    without->setConcurrencyControl(false);

    auto with = std::make_unique<QueryPlan>(makeSourcePlan());
    with->setMaxThreads(4);
    with->setConcurrencyControl(true);

    SharedHeaders input_headers{makeHeader(), makeHeader()};
    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::move(without));
    plans.emplace_back(std::move(with));

    QueryPlan united;
    united.unitePlans(std::make_unique<UnionStep>(input_headers), std::move(plans));

    EXPECT_EQ(united.getMaxThreads(), 4u);
    EXPECT_TRUE(united.getConcurrencyControl());
}

/// The merge must not invent a limit no child asked for: a plan united from children that all run
/// uncapped stays uncapped.
TEST(QueryPlanExecutionLimits, UnitePlansKeepsDefaultsWhenNoChildAsks)
{
    auto left = std::make_unique<QueryPlan>(makeSourcePlan());
    auto right = std::make_unique<QueryPlan>(makeSourcePlan());

    SharedHeaders input_headers{makeHeader(), makeHeader()};
    std::vector<QueryPlanPtr> plans;
    plans.emplace_back(std::move(left));
    plans.emplace_back(std::move(right));

    QueryPlan united;
    united.unitePlans(std::make_unique<UnionStep>(input_headers), std::move(plans));

    EXPECT_EQ(united.getMaxThreads(), 0u);
    EXPECT_FALSE(united.getConcurrencyControl());
}
