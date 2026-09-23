#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/mergedPlanDAG.h>
#include <Processors/QueryPlan/ReadNothingStep.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

ColumnWithTypeAndName column(const String & name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return {type->createColumn(), type, name};
}

struct TestPlan
{
    QueryPlan::Nodes nodes;

    QueryPlan::Node & addSource(const Block & header)
    {
        auto & node = nodes.emplace_back();
        node.step = std::make_unique<ReadNothingStep>(std::make_shared<const Block>(header));
        return node;
    }

    QueryPlan::Node & addStep(QueryPlanStepPtr step, QueryPlan::Node & child)
    {
        auto & node = nodes.emplace_back();
        node.step = std::move(step);
        node.children = {&child};
        return node;
    }
};

/// Renames the first two columns of `header`, leaving the rest to pass through.
ActionsDAG makeDAG(const Block & header, const String & first_name, const String & second_name)
{
    ActionsDAG dag(header.getColumnsWithTypeAndName());
    const auto * first = dag.getOutputs()[0];
    const auto * second = dag.getOutputs()[1];
    dag.getOutputs() = {&dag.addAlias(*first, first_name), &dag.addAlias(*second, second_name)};
    return dag;
}

}

TEST(MergedPlanDAG, MergesAChainIntoOneDAG)
{
    const Block header{column("a"), column("b"), column("c")};

    TestPlan plan;
    auto & source = plan.addSource(header);
    /// (a, b, c) -> (f, x, c), then filter on `cond` computed from `f`, which the filter drops again.
    auto & expression = plan.addStep(std::make_unique<ExpressionStep>(source.step->getOutputHeader(), makeDAG(header, "f", "x")), source);
    auto & filter = plan.addStep(
        std::make_unique<FilterStep>(
            expression.step->getOutputHeader(),
            makeDAG(*expression.step->getOutputHeader(), "cond", "x2"),
            "cond",
            /*remove_filter_column=*/true),
        expression);

    const auto merged = buildMergedPlanDAG(filter);

    /// One opaque source, standing for every column the read produces.
    ASSERT_EQ(merged.sources.size(), 1u);
    EXPECT_EQ(merged.sources.front().plan_node, &source);
    ASSERT_EQ(merged.sources.front().inputs.size(), header.columns());
    EXPECT_EQ(merged.sources.front().inputs[0]->result_name, "a");
    EXPECT_EQ(merged.sources.front().inputs[2]->result_name, "c");

    /// The DAG reproduces the header of the top step, filter column erased.
    ASSERT_EQ(merged.getOutputs().size(), filter.step->getOutputHeader()->columns());
    EXPECT_EQ(merged.getDAG().getOutputs().size(), merged.getOutputs().size());

    /// The filter condition was picked up, and the whole subtree reads one source, so everything in it
    /// can be recomputed on that source's rows.
    ASSERT_EQ(merged.filter_nodes.size(), 1u);
    EXPECT_EQ(merged.filter_nodes.front()->result_name, "cond");

    /// There is no join, so nothing here is gated by one.
    EXPECT_TRUE(merged.stuffings.empty());
    for (const auto & node : merged.getDAG().getNodes())
    {
        EXPECT_EQ(merged.getSources(&node).count(), 1u) << node.result_name;
        EXPECT_EQ(merged.getNearestStuffing(&node), nullptr) << node.result_name;
    }
}

TEST(MergedPlanDAG, TreatsArrayJoinAsASource)
{
    auto array_type = std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>());
    const Block header{ColumnWithTypeAndName{array_type->createColumn(), array_type, "arr"}};

    TestPlan plan;
    auto & source = plan.addSource(header);

    ActionsDAG dag(header.getColumnsWithTypeAndName());
    dag.getOutputs() = {&dag.addArrayJoin(*dag.getOutputs()[0], "joined")};

    auto & expression = plan.addStep(std::make_unique<ExpressionStep>(source.step->getOutputHeader(), std::move(dag)), source);

    /// An `arrayJoin` changes the number of rows, so the walk stops at the step computing it and that
    /// step stands in as a source, rather than the whole subtree being given up on.
    const auto merged = buildMergedPlanDAG(expression);
    ASSERT_EQ(merged.sources.size(), 1u);
    EXPECT_EQ(merged.sources.front().plan_node, &expression);
    EXPECT_EQ(merged.getOutputs().size(), expression.step->getOutputHeader()->columns());
}

TEST(MergedPlanDAG, TreatsAnUnknownStepAsASource)
{
    const Block header{column("a"), column("b"), column("c")};

    TestPlan plan;
    auto & source = plan.addSource(header);
    auto & expression = plan.addStep(std::make_unique<ExpressionStep>(source.step->getOutputHeader(), makeDAG(header, "f", "x")), source);

    /// Building from the source alone gives one source and no expressions of its own.
    const auto merged = buildMergedPlanDAG(source);
    EXPECT_EQ(merged.sources.size(), 1u);
    EXPECT_EQ(merged.getOutputs().size(), header.columns());
    EXPECT_TRUE(merged.filter_nodes.empty());

    /// And the step above it does not become one.
    const auto merged_above = buildMergedPlanDAG(expression);
    EXPECT_EQ(merged_above.sources.size(), 1u);
    EXPECT_EQ(merged_above.sources.front().plan_node, &source);
}
