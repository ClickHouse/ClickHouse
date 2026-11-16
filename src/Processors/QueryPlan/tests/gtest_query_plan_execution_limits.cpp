#include <gtest/gtest.h>

#include <Core/Block.h>
#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/QueryPlanStepRegistry.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Common/tests/gtest_global_context.h>

using namespace DB;

namespace DB
{
void registerReadNothingStep(QueryPlanStepRegistry & registry);
}

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

void tryRegisterReadNothingStep()
{
    static struct Register
    {
        Register()
        {
            registerReadNothingStep(QueryPlanStepRegistry::instance());
        }
    } registered;
}

/// Smallest plan that owns a root node: a single source step.
QueryPlan makeSourcePlan()
{
    /// `registerStep` rejects duplicate names, and other tests in this binary register
    /// overlapping subsets. Register only the step deserialized in this test file.
    tryRegisterReadNothingStep();

    QueryPlan plan;
    plan.addStep(std::make_unique<ReadNothingStep>(makeHeader()));
    return plan;
}

}

/// `max_threads` and `concurrency_control` are plan-level, not part of the node tree, so they are
/// invisible in query results and have to be asserted directly.

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

TEST(QueryPlanExecutionLimits, CloneSubtreePreservesSourceLimitsAndResources)
{
    auto source = makeSourcePlan();
    source.setMaxThreads(4);
    source.setConcurrencyControl(true);
    source.addInterpreterContext(Context::createCopy(getContext().context));

    auto subtree = QueryPlan::cloneSubtree(source.getRootNode(), source);

    EXPECT_EQ(subtree.getMaxThreads(), 4u);
    EXPECT_TRUE(subtree.getConcurrencyControl());
    EXPECT_EQ(subtree.getInterpretersContexts().size(), 1u);
    EXPECT_EQ(source.getInterpretersContexts().size(), 1u);
}

TEST(QueryPlanExecutionLimits, SerializationPreservesLimits)
{
    auto source = makeSourcePlan();
    source.setMaxThreads(4);
    source.setConcurrencyControl(true);

    WriteBufferFromOwnString out;
    source.serialize(out, DBMS_QUERY_PLAN_SERIALIZATION_VERSION);

    ReadBufferFromString in(out.str());
    auto deserialized = QueryPlan::deserialize(in, getContext().context, /*max_type_complexity=*/0);
    auto restored = QueryPlan::makeSets(std::move(deserialized), getContext().context);

    EXPECT_EQ(restored.getMaxThreads(), 4u);
    EXPECT_TRUE(restored.getConcurrencyControl());
}

TEST(QueryPlanExecutionLimits, SerializationRejectsOlderPeerWhenLimitsAreSet)
{
    auto source = makeSourcePlan();
    source.setMaxThreads(4);

    WriteBufferFromOwnString out;
    EXPECT_THROW(source.serialize(out, DBMS_MIN_QUERY_PLAN_SERIALIZATION_VERSION_WITH_EXECUTION_LIMITS - 1), Exception);
}

TEST(QueryPlanExecutionLimits, ReplaceNodeWithPlanMergesConcurrencyControl)
{
    auto destination = makeSourcePlan();
    destination.setMaxThreads(2);

    auto replacement = makeSourcePlan();
    replacement.setMaxThreads(4);
    replacement.setConcurrencyControl(true);

    destination.replaceNodeWithPlan(destination.getRootNode(), std::move(replacement));

    EXPECT_EQ(destination.getMaxThreads(), 4u);
    EXPECT_TRUE(destination.getConcurrencyControl());
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
/// OR-ed the same way, so a united plan honours it whenever any child asked for it. Both child
/// orders are asserted: the folds accumulate over every child, so the result must not depend on
/// which child happens to come last.
TEST(QueryPlanExecutionLimits, UnitePlansMergesLimitsFromChildren)
{
    for (bool asking_child_first : {false, true})
    {
        auto without = std::make_unique<QueryPlan>(makeSourcePlan());
        without->setMaxThreads(2);
        without->setConcurrencyControl(false);

        auto with = std::make_unique<QueryPlan>(makeSourcePlan());
        with->setMaxThreads(4);
        with->setConcurrencyControl(true);

        std::vector<QueryPlanPtr> plans;
        if (asking_child_first)
        {
            plans.emplace_back(std::move(with));
            plans.emplace_back(std::move(without));
        }
        else
        {
            plans.emplace_back(std::move(without));
            plans.emplace_back(std::move(with));
        }

        SharedHeaders input_headers{makeHeader(), makeHeader()};
        QueryPlan united;
        united.unitePlans(std::make_unique<UnionStep>(input_headers), std::move(plans));

        EXPECT_EQ(united.getMaxThreads(), 4u) << "asking_child_first=" << asking_child_first;
        EXPECT_TRUE(united.getConcurrencyControl()) << "asking_child_first=" << asking_child_first;
    }
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
