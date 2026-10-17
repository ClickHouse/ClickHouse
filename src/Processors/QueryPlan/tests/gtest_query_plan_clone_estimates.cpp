#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/CostEstimationInfo.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>

using namespace DB;

namespace
{

SharedHeader makeHeader()
{
    auto type = std::make_shared<DataTypeUInt64>();
    return std::make_shared<const Block>(Block({ColumnWithTypeAndName(type->createColumn(), type, "k")}));
}

}

/// A cloned subplan keeps the row estimate of every node: `EXPLAIN estimates` and the processors
/// profile log read it from the node, not from the step.
TEST(QueryPlanClone, KeepsStepEstimates)
{
    auto header = makeHeader();
    QueryPlan source;
    source.addStep(std::make_unique<ReadNothingStep>(header));
    source.getRootNode()->cost_estimation = CostEstimationInfo{.rows = 42.0, .cost = std::nullopt, .source = RowEstimateSource::Statistics};
    source.addStep(std::make_unique<ExpressionStep>(header, ActionsDAG(header->getNamesAndTypesList())));
    source.getRootNode()->cost_estimation = CostEstimationInfo{.rows = 42.0, .cost = 7.0, .source = RowEstimateSource::Statistics};

    auto clone = QueryPlan::cloneSubtree(source.getRootNode(), source);

    const auto * root = clone.getRootNode();
    ASSERT_TRUE(root->cost_estimation.has_value());
    EXPECT_EQ(root->cost_estimation->rows, 42.0);
    EXPECT_EQ(root->cost_estimation->cost, 7.0);
    EXPECT_EQ(root->cost_estimation->source, RowEstimateSource::Statistics);
    ASSERT_EQ(root->children.size(), 1u);
    ASSERT_TRUE(root->children[0]->cost_estimation.has_value());
    EXPECT_EQ(root->children[0]->cost_estimation->rows, 42.0);
    EXPECT_EQ(root->children[0]->cost_estimation->cost, std::nullopt);
}
