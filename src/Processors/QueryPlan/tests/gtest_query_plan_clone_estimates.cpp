#include <gtest/gtest.h>

#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/CostEstimationInfo.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>

#include <magic_enum.hpp>

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

/// A serialized plan carries the source of each step estimate, and the reader rejects a value it does not know. Every
/// source must read back, and must have a name for the processors profile log.
TEST(RowEstimateSource, EveryValueReadsBack)
{
    for (const auto source : magic_enum::enum_values<RowEstimateSource>())
    {
        EXPECT_TRUE(isRowEstimateSource(static_cast<UInt8>(source))) << magic_enum::enum_name(source);
        if (source != RowEstimateSource::NoSource)
            EXPECT_FALSE(rowEstimateSourceName(source).empty()) << magic_enum::enum_name(source);
    }
    EXPECT_FALSE(isRowEstimateSource(static_cast<UInt8>(magic_enum::enum_count<RowEstimateSource>())));
}
