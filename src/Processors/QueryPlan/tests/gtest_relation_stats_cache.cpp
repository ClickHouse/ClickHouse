#include <gtest/gtest.h>

#include <Core/ProtocolDefines.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/JoinExpressionActions.h>
#include <Interpreters/JoinOperator.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/QueryPlanSerializationSettings.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

SharedHeader makeHeader(const String & column_name = "k")
{
    const auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

JoinExpressionActions makeJoinExpressionActions(const Block & left_header, const Block & right_header)
{
    JoinExpressionActions expression_actions(left_header, right_header);
    auto actions_dag = expression_actions.getActionsDAG();
    for (const auto * input : actions_dag->getInputs())
        actions_dag->getOutputs().push_back(input);
    return expression_actions;
}

std::unique_ptr<JoinStepLogical> makeLogicalJoinStep()
{
    auto left_header = makeHeader("l");
    auto right_header = makeHeader("r");
    QueryPlanSerializationSettings settings;
    return std::make_unique<JoinStepLogical>(
        left_header,
        right_header,
        JoinOperator(JoinKind::Inner, JoinStrictness::All),
        makeJoinExpressionActions(*left_header, *right_header),
        ActionsDAG::NodeRawConstPtrs{},
        JoinSettings(settings, DBMS_QUERY_PLAN_SERIALIZATION_VERSION),
        SortingStep::Settings(settings));
}

class TestSystemOneStep final : public ISourceStep
{
public:
    explicit TestSystemOneStep(SharedHeader header)
        : ISourceStep(std::move(header))
    {
    }

    String getName() const override { return "ReadFromSystemOne"; }
    QueryPlanStepPtr clone() const override { return std::make_unique<TestSystemOneStep>(getOutputHeader()); }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }
};

class CountingSourceStep final : public ISourceStep
{
public:
    CountingSourceStep(SharedHeader header, size_t & visits_)
        : ISourceStep(std::move(header))
        , visits(visits_)
    {
    }

    String getName() const override
    {
        ++visits;
        return "CountingSource";
    }

    QueryPlanStepPtr clone() const override { return std::make_unique<CountingSourceStep>(getOutputHeader(), visits); }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }

private:
    size_t & visits;
};

class CountingSystemOneStep final : public ISourceStep
{
public:
    CountingSystemOneStep(SharedHeader header, size_t & visits_)
        : ISourceStep(std::move(header))
        , visits(visits_)
    {
    }

    String getName() const override
    {
        ++visits;
        return "ReadFromSystemOne";
    }

    QueryPlanStepPtr clone() const override { return std::make_unique<CountingSystemOneStep>(getOutputHeader(), visits); }
    void initializePipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }

private:
    size_t & visits;
};

class PreservingTestStep final : public ITransformingStep
{
public:
    explicit PreservingTestStep(SharedHeader header)
        : ITransformingStep(
              header,
              header,
              Traits{
                  {.returns_single_stream = false, .preserves_number_of_streams = true, .preserves_sorting = true},
                  {.preserves_number_of_rows = true}})
    {
    }

    String getName() const override { return "PreservingTest"; }
    QueryPlanStepPtr clone() const override { return std::make_unique<PreservingTestStep>(getOutputHeader()); }
    void transformPipeline(QueryPipelineBuilder &, const BuildQueryPipelineSettings &) override { }

private:
    void updateOutputHeader() override { output_header = input_headers.front(); }
};

ActionsDAG makeFilterDAG(const SharedHeader & header)
{
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto filter_type = std::make_shared<DataTypeUInt8>();
    const auto & predicate = dag.addColumn(filter_type->createColumnConstWithDefaultValue(1), filter_type, "__filter");
    dag.addOrReplaceInOutputs(predicate);
    return dag;
}

}

TEST(RelationStatsCache, SharesOptionIndependentEntriesAndSupportsInvalidation)
{
    size_t visits = 0;
    QueryPlan::Node node;
    node.step = std::make_unique<CountingSourceStep>(makeHeader(), visits);
    RelationStatsCache cache;

    estimateReadRowsCount(node, nullptr, {}, &cache);
    estimateReadRowsCount(node, nullptr, RelationStatsOptions{.propagate_join_estimates = true}, &cache);
    EXPECT_EQ(visits, 1);

    cache.invalidate(node);
    estimateReadRowsCount(node, nullptr, {}, &cache);
    EXPECT_EQ(visits, 2);

    QueryPlan::Node join_node;
    auto join_step = makeLogicalJoinStep();
    join_step->setOptimized(100);
    join_node.step = std::move(join_step);

    const auto default_join_stats = estimateReadRowsCount(join_node, nullptr, {}, &cache);
    EXPECT_FALSE(default_join_stats.estimated_rows.has_value());
    const auto propagated_join_stats
        = estimateReadRowsCount(join_node, nullptr, RelationStatsOptions{.propagate_join_estimates = true}, &cache);
    EXPECT_EQ(propagated_join_stats.estimated_rows, 100);
}

#ifndef DEBUG_OR_SANITIZER_BUILD
TEST(RelationStatsCache, DetectsReplacedStepWithoutInvalidate)
{
    size_t visits = 0;
    QueryPlan::Node node;
    node.step = std::make_unique<CountingSourceStep>(makeHeader(), visits);
    RelationStatsCache cache;

    const auto original = estimateReadRowsCount(node, nullptr, {}, &cache);
    EXPECT_FALSE(original.estimated_rows.has_value());
    EXPECT_EQ(visits, 1);

    node.step = std::make_unique<TestSystemOneStep>(makeHeader());
    const auto replaced = estimateReadRowsCount(node, nullptr, {}, &cache);
    EXPECT_EQ(replaced.estimated_rows, 1);
}
#endif

TEST(RelationStatsCache, RebindsWrappedSubtreeAndSeparatesFilters)
{
    const auto header = makeHeader();
    size_t visits = 0;
    QueryPlan::Node node;
    node.step = std::make_unique<CountingSystemOneStep>(header, visits);
    RelationStatsCache cache;

    const auto original_stats = estimateReadRowsCount(node, nullptr, {}, &cache);
    ASSERT_EQ(original_stats.estimated_rows, 1);
    EXPECT_EQ(visits, 1);

    QueryPlan::Nodes nodes;
    auto & moved_subtree = nodes.emplace_back(std::move(node));
    node = QueryPlan::Node{std::make_unique<LimitStep>(header, 0, 0), {&moved_subtree}};
    cache.rebindNode(node, moved_subtree);

    const auto moved_stats = estimateReadRowsCount(moved_subtree, nullptr, {}, &cache);
    ASSERT_EQ(moved_stats.estimated_rows, 1);
    EXPECT_EQ(visits, 1);

    const auto wrapped_stats = estimateReadRowsCount(node, nullptr, {}, &cache);
    ASSERT_EQ(wrapped_stats.estimated_rows, 0);
    EXPECT_EQ(visits, 1);

    auto first_filter_dag = makeFilterDAG(header);
    const auto * first_filter = first_filter_dag.tryFindInOutputs("__filter");
    estimateReadRowsCount(moved_subtree, first_filter, {}, &cache);
    estimateReadRowsCount(moved_subtree, first_filter, {}, &cache);
    EXPECT_EQ(visits, 2);

    auto second_filter_dag = makeFilterDAG(header);
    const auto * second_filter = second_filter_dag.tryFindInOutputs("__filter");
    estimateReadRowsCount(moved_subtree, second_filter, {}, &cache);
    EXPECT_EQ(visits, 3);
}

TEST(RelationStatsCache, DoesNotPersistAcrossMutableJoins)
{
    QueryPlan::Node join_node;
    auto join_step = makeLogicalJoinStep();
    auto * mutable_join_step = join_step.get();
    const auto join_header = join_step->getOutputHeader();
    join_node.step = std::move(join_step);

    QueryPlan::Node root;
    root.step = std::make_unique<PreservingTestStep>(join_header);
    root.children = {&join_node};

    RelationStatsCache cache;
    const auto options = RelationStatsOptions{.propagate_join_estimates = true};
    EXPECT_FALSE(estimateReadRowsCount(root, nullptr, options, &cache).estimated_rows.has_value());

    mutable_join_step->setOptimized(100, {{"l", ColumnStats{.num_distinct_values = 50}}});
    cache.invalidate(join_node);
    const auto stats = estimateReadRowsCount(root, nullptr, options, &cache);
    ASSERT_EQ(stats.estimated_rows, 100);
    ASSERT_TRUE(stats.column_stats.contains("l"));
    EXPECT_EQ(stats.column_stats.at("l").num_distinct_values, 50);
}
