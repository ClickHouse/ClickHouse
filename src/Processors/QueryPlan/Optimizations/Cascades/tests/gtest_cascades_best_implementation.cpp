#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Core/SortDescription.h>

using namespace DB;

namespace
{

SortDescription sortByColumns(std::initializer_list<String> names)
{
    SortDescription description;
    for (const auto & name : names)
    {
        SortColumnDescription column;
        column.column_name = name;
        description.push_back(column);
    }
    return description;
}

ExpressionProperties propsAt(size_t node_count, SortDescription sorting = {})
{
    ExpressionProperties properties;
    properties.distribution.node_count = node_count;
    properties.sorting = std::move(sorting);
    return properties;
}

/// A physical expression with a fixed subtree cost and no plan step, used to exercise the
/// group's best-implementation bookkeeping in isolation.
GroupExpressionPtr costedExpr(ExpressionProperties properties, Float64 subtree_work)
{
    auto expression = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    expression->properties = std::move(properties);
    expression->cost = ExpressionCost{.cost = Cost{.work = subtree_work}, .subtree_cost = Cost{.work = subtree_work}};
    return expression;
}

}

/// Among implementations satisfying the same required properties, the cheapest one wins, and a
/// newly added cheaper implementation supersedes a costlier one with the same properties.
TEST(CascadesBestImplementation, CheapestWins)
{
    Group group(0);
    CostConfig cost_config;

    auto pricey = costedExpr(propsAt(4), 100);
    auto cheap = costedExpr(propsAt(4), 10);

    group.updateBestImplementation(pricey, cost_config);
    group.updateBestImplementation(cheap, cost_config);

    auto best = group.getBestImplementation(propsAt(4), cost_config);
    ASSERT_TRUE(best.expression != nullptr);
    EXPECT_EQ(best.expression, cheap);
    EXPECT_EQ(group.getBestCostForProperties(propsAt(4), cost_config), 10.0);
}

/// A stronger sorting satisfies a weaker requirement (prefix), but not vice versa.
TEST(CascadesBestImplementation, SortingSatisfactionSelectsImplementation)
{
    Group group(0);
    CostConfig cost_config;

    auto sorted_k_ts = costedExpr(propsAt(4, sortByColumns({"k", "ts"})), 50);
    group.updateBestImplementation(sorted_k_ts, cost_config);

    /// A requirement for ORDER BY k is satisfied by an implementation sorted by (k, ts).
    auto for_k = group.getBestImplementation(propsAt(4, sortByColumns({"k"})), cost_config);
    EXPECT_EQ(for_k.expression, sorted_k_ts);

    /// A requirement for ORDER BY (k, ts) is NOT satisfied by an implementation sorted only by k.
    Group only_k(1);
    only_k.updateBestImplementation(costedExpr(propsAt(4, sortByColumns({"k"})), 50), cost_config);
    auto for_k_ts = only_k.getBestImplementation(propsAt(4, sortByColumns({"k", "ts"})), cost_config);
    EXPECT_EQ(for_k_ts.expression, nullptr);
}

/// Implementations of different distribution shapes do not satisfy each other's requirements.
TEST(CascadesBestImplementation, DistributionShapeIsolation)
{
    Group group(0);
    CostConfig cost_config;

    auto four_nodes = costedExpr(propsAt(4), 40);
    auto one_node = costedExpr(propsAt(1), 10);
    group.updateBestImplementation(four_nodes, cost_config);
    group.updateBestImplementation(one_node, cost_config);

    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, four_nodes);
    EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, one_node);
    /// No implementation exists for a shape that was never added.
    EXPECT_EQ(group.getBestImplementation(propsAt(2), cost_config).expression, nullptr);
}
