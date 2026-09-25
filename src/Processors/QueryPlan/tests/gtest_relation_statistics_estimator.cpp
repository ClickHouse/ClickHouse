#include <gtest/gtest.h>

#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FillingStep.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>

using namespace DB;
using namespace DB::QueryPlanOptimizations;

namespace
{

SharedHeader makeHeader(const String & column_name = "k")
{
    const auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, column_name)}));
}

SortDescription sortByKey()
{
    SortDescription result;
    result.emplace_back("k");
    return result;
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

void expectUnsupportedUnaryStepStopsBeforeChild(std::unique_ptr<IQueryPlanStep> step, const SharedHeader & child_header)
{
    size_t child_visits = 0;
    QueryPlan::Node child;
    child.step = std::make_unique<CountingSourceStep>(child_header, child_visits);
    QueryPlan::Node root;
    root.step = std::move(step);
    root.children = {&child};

    const auto result = estimateReadRowsCount(root);
    EXPECT_FALSE(result.estimated_rows.has_value());
    EXPECT_TRUE(result.column_stats.empty());
    EXPECT_EQ(child_visits, 0);
}

}

TEST(RelationStatisticsEstimator, ArrayJoinExpressionIsUnsupported)
{
    const auto element_type = std::make_shared<DataTypeUInt64>();
    const auto array_type = std::make_shared<DataTypeArray>(element_type);
    const auto header = std::make_shared<const Block>(Block({ColumnWithTypeAndName(array_type->createColumn(), array_type, "k")}));
    ActionsDAG dag(header->getColumnsWithTypeAndName());
    const auto & array_join = dag.addArrayJoin(*dag.getInputs().front(), "k");
    dag.addOrReplaceInOutputs(array_join);

    auto step = std::make_unique<ExpressionStep>(header, std::move(dag));
    EXPECT_FALSE(estimateUnaryStepStats(*step, RelationStats{}).has_value());

    size_t child_visits = 0;
    QueryPlan::Node child;
    child.step = std::make_unique<CountingSourceStep>(header, child_visits);
    QueryPlan::Node root;
    root.step = std::move(step);
    root.children.emplace_back(&child);

    const auto result = estimateReadRowsCount(root);
    EXPECT_FALSE(result.estimated_rows.has_value());
    EXPECT_TRUE(result.column_stats.empty());
    EXPECT_EQ(child_visits, 0);
}

TEST(RelationStatisticsEstimator, DistinctFillingAndUnionAreUnsupportedBoundaries)
{
    const auto header = makeHeader();

    expectUnsupportedUnaryStepStopsBeforeChild(
        std::make_unique<DistinctStep>(header, DistinctStep::Settings{}, 0, Names{"k"}, false), header);

    auto fill_sort = sortByKey();
    fill_sort.front().with_fill = true;
    expectUnsupportedUnaryStepStopsBeforeChild(std::make_unique<FillingStep>(header, std::move(fill_sort), nullptr, false), header);

    size_t left_visits = 0;
    size_t right_visits = 0;
    QueryPlan::Node left;
    left.step = std::make_unique<CountingSourceStep>(header, left_visits);
    QueryPlan::Node right;
    right.step = std::make_unique<CountingSourceStep>(header, right_visits);
    QueryPlan::Node union_node;
    union_node.step = std::make_unique<UnionStep>(SharedHeaders{header, header});
    union_node.children = {&left, &right};

    const auto result = estimateReadRowsCount(union_node);
    EXPECT_FALSE(result.estimated_rows.has_value());
    EXPECT_TRUE(result.column_stats.empty());
    EXPECT_EQ(left_visits, 0);
    EXPECT_EQ(right_visits, 0);
}

TEST(RelationStatisticsEstimator, RelationStatsTraversalIsIterative)
{
    constexpr size_t depth = 5000;
    const auto header = makeHeader();
    QueryPlan::Nodes nodes;

    auto & leaf = nodes.emplace_back();
    leaf.step = std::make_unique<TestSystemOneStep>(header);
    QueryPlan::Node * root = &leaf;
    for (size_t i = 0; i < depth; ++i)
    {
        auto & parent = nodes.emplace_back();
        parent.step = std::make_unique<PreservingTestStep>(header);
        parent.children = {root};
        root = &parent;
    }

    const auto stats = estimateReadRowsCount(*root);
    ASSERT_TRUE(stats.estimated_rows.has_value());
    EXPECT_EQ(*stats.estimated_rows, 1);
    EXPECT_EQ(stats.table_name, "system.one");
}
