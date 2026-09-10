#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/lazyFrontier.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Common/tests/gtest_global_context.h>
#include <Common/tests/gtest_global_register.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

ColumnWithTypeAndName column(const String & name)
{
    auto type = std::make_shared<DataTypeUInt64>();
    return {type->createColumn(), type, name};
}

const ActionsDAG::Node & addFunction(ActionsDAG & dag, const String & name, ActionsDAG::NodeRawConstPtrs children)
{
    auto function = FunctionFactory::instance().get(name, getContext().context);
    return dag.addFunction(function, std::move(children), {});
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

const ActionsDAG::Node * findOutput(const MergedPlanDAG & merged, const String & name)
{
    for (const auto * output : merged.getOutputs())
        if (output->result_name == name)
            return output;
    return nullptr;
}

/// The DAG the steps were built from was moved into them and cloned on the way in, so a node of the
/// merged DAG has to be looked up rather than remembered.
const ActionsDAG::Node * findNodeContaining(const MergedPlanDAG & merged, const String & name_part)
{
    for (const auto & node : merged.getDAG().getNodes())
        if (node.result_name.contains(name_part))
            return &node;
    return nullptr;
}

size_t countCrossing(const LazyFrontier & frontier)
{
    return std::ranges::count_if(
        frontier.placement, [](const auto & entry) { return entry.second.below == Placement::Below::ComputedAndCrossing; });
}

size_t outputPosition(const MergedPlanDAG & merged, const String & name)
{
    const auto & outputs = merged.getOutputs();
    for (size_t position = 0; position < outputs.size(); ++position)
        if (outputs[position]->result_name == name)
            return position;
    return outputs.size();
}

}

/// `select a, sum, heavy from t where sum > 0 order by a limit 10`, where `sum` is `a + b`: the filter
/// needs `a + b` below the LIMIT and the result needs it above, which is the case that used to leak an
/// intermediate column across the plan.
TEST(LazyFrontier, RecomputesAValueTheFilterAlreadyUsed)
{
    tryRegisterFunctions();
    const Block header{column("a"), column("b"), column("heavy")};

    TestPlan plan;
    auto & source = plan.addSource(header);

    ActionsDAG dag(header.getColumnsWithTypeAndName());
    const auto * a = dag.getOutputs()[0];
    const auto * b = dag.getOutputs()[1];
    const auto * heavy = dag.getOutputs()[2];
    const auto & sum = addFunction(dag, "plus", {a, b});
    const auto & condition = addFunction(dag, "greater", {&sum, a});
    dag.getOutputs() = {&condition, a, &dag.addAlias(sum, "sum"), heavy};

    auto & filter = plan.addStep(
        std::make_unique<FilterStep>(source.step->getOutputHeader(), std::move(dag), condition.result_name, true), source);

    const auto merged = buildMergedPlanDAG(filter);
    ASSERT_TRUE(merged.has_value());
    ASSERT_EQ(merged->sources.size(), 1u);

    /// ORDER BY a.
    const auto frontier = chooseLazyFrontier(*merged, {outputPosition(*merged, "a")}, {true});

    EXPECT_TRUE(frontier.defersAnything());

    /// `a + b` is computed below the LIMIT for the filter, and does not cross it: it is recomputed above
    /// instead. There is no join here, so nothing gates the recomputation.
    const auto * sum_output = findOutput(*merged, "sum");
    const auto * sum_in_merged = findNodeContaining(*merged, "plus(");
    ASSERT_TRUE(sum_output != nullptr);
    ASSERT_TRUE(sum_in_merged != nullptr);
    EXPECT_EQ(frontier.at(sum_output).above, Placement::Above::Recomputed);
    EXPECT_EQ(frontier.at(sum_in_merged).above, Placement::Above::Recomputed);
    EXPECT_EQ(frontier.at(sum_in_merged).below, Placement::Below::Computed);
    EXPECT_EQ(merged->getNearestStuffing(sum_in_merged), std::nullopt);

    /// Only the sort key crosses the LIMIT; `b` and the heavy column are read for the surviving rows.
    const auto * a_input = merged->sources.front().inputs[0];
    EXPECT_EQ(frontier.at(a_input).below, Placement::Below::ComputedAndCrossing);
    EXPECT_EQ(countCrossing(frontier), 1u);

    const auto reads = collectLazyReads(*merged, frontier);
    ASSERT_EQ(reads.size(), 1u);
    EXPECT_EQ(reads[0].size(), 2u);
    EXPECT_FALSE(reads[0].contains(a_input));
}

/// A value that does not answer the same twice has to be carried instead of recomputed.
TEST(LazyFrontier, CarriesANonDeterministicValue)
{
    tryRegisterFunctions();
    const Block header{column("a"), column("b"), column("heavy")};

    TestPlan plan;
    auto & source = plan.addSource(header);

    ActionsDAG dag(header.getColumnsWithTypeAndName());
    const auto * a = dag.getOutputs()[0];
    const auto * heavy = dag.getOutputs()[2];
    const auto & random = addFunction(dag, "rand64", {});  /// NOLINT
    const auto & mixed = addFunction(dag, "plus", {a, &random});
    const auto & condition = addFunction(dag, "greater", {&mixed, a});
    dag.getOutputs() = {&condition, a, &dag.addAlias(mixed, "mixed"), heavy};

    auto & filter = plan.addStep(
        std::make_unique<FilterStep>(source.step->getOutputHeader(), std::move(dag), condition.result_name, true), source);

    const auto merged = buildMergedPlanDAG(filter);
    ASSERT_TRUE(merged.has_value());

    const auto frontier = chooseLazyFrontier(*merged, {outputPosition(*merged, "a")}, {true});

    /// The filter already drew a number from `rand64` and the result has to agree with it, so that number
    /// crosses the LIMIT as a column and is never drawn again. What is computed from it may then be
    /// recomputed above the LIMIT, since that gives the same answer.
    const auto * random_in_merged = findNodeContaining(*merged, "rand64");
    ASSERT_TRUE(random_in_merged != nullptr);
    EXPECT_EQ(frontier.at(random_in_merged).below, Placement::Below::ComputedAndCrossing);
    EXPECT_EQ(frontier.at(random_in_merged).above, Placement::Above::No);

    /// The heavy column has nothing to do with it and is still read for the surviving rows only.
    const auto * heavy_input = merged->sources.front().inputs[2];
    EXPECT_EQ(heavy_input->result_name, "heavy");
    EXPECT_TRUE(collectLazyReads(*merged, frontier)[0].contains(heavy_input));
}

/// Nothing can be deferred from a source that has no second read.
TEST(LazyFrontier, KeepsEverythingEagerWithoutALazySource)
{
    tryRegisterFunctions();
    const Block header{column("a"), column("b"), column("heavy")};

    TestPlan plan;
    auto & source = plan.addSource(header);

    const auto merged = buildMergedPlanDAG(source);
    ASSERT_TRUE(merged.has_value());

    const auto frontier = chooseLazyFrontier(*merged, {0}, {false});

    EXPECT_FALSE(frontier.defersAnything());
    EXPECT_TRUE(collectLazyReads(*merged, frontier)[0].empty());

    /// Every column crosses the LIMIT as a column of its own, computed below it as before.
    for (const auto * output : merged->getOutputs())
        EXPECT_EQ(frontier.at(output).below, Placement::Below::ComputedAndCrossing);
}

/// A value nothing below the LIMIT used is computed above it for the first time, so even a
/// non-deterministic one needs no column of its own.
TEST(LazyFrontier, ComputesAnUnusedNonDeterministicValueLate)
{
    tryRegisterFunctions();
    const Block header{column("a"), column("b"), column("heavy")};

    TestPlan plan;
    auto & source = plan.addSource(header);

    ActionsDAG dag(header.getColumnsWithTypeAndName());
    const auto * a = dag.getOutputs()[0];
    const auto * heavy = dag.getOutputs()[2];
    const auto & random = addFunction(dag, "rand64", {});  /// NOLINT
    dag.getOutputs() = {a, &dag.addAlias(random, "r"), heavy};

    auto & expression = plan.addStep(std::make_unique<ExpressionStep>(source.step->getOutputHeader(), std::move(dag)), source);

    const auto merged = buildMergedPlanDAG(expression);
    ASSERT_TRUE(merged.has_value());

    const auto frontier = chooseLazyFrontier(*merged, {outputPosition(*merged, "a")}, {true});

    const auto * random_in_merged = findNodeContaining(*merged, "rand64");
    ASSERT_TRUE(random_in_merged != nullptr);
    EXPECT_EQ(frontier.at(random_in_merged).above, Placement::Above::Recomputed);
    EXPECT_EQ(frontier.at(random_in_merged).below, Placement::Below::No);
}
