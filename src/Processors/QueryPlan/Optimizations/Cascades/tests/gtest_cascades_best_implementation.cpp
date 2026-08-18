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

ExpressionProperties propsFull(size_t node_count, SortDescription sorting, DistributionColumns columns)
{
    ExpressionProperties properties;
    properties.distribution.node_count = node_count;
    properties.distribution.columns = std::move(columns);
    properties.sorting = std::move(sorting);
    return properties;
}

/// A self-referential enforcer candidate: produces `output`, requires `input_required` from its
/// own group (group 0 in these tests).
GroupExpressionPtr enforcerExpr(EnforcerAxis axis, ExpressionProperties output, Float64 cost, ExpressionProperties input_required)
{
    auto expression = costedExpr(std::move(output), cost);
    expression->enforcer_axis = axis;
    expression->inputs.push_back({.group_id = 0, .required_properties = std::move(input_required)});
    return expression;
}

}

/// Strategy B (Sort-per-node -> SortedGather) must compose, even over keyed data: a sorted gather's
/// self-input requires non-empty sort with empty distribution columns, and the productive
/// SortingEnforcer candidate carries the source's keyed columns. It must stay eligible (the
/// empty-columns guard is scoped to distribution-axis enforcers).
TEST(CascadesSelectInputImplementation, StrategyBComposesOverKeyedData)
{
    Group group(0);
    CostConfig cost_config;

    auto keyed = DistributionColumns{NameSet{"a"}};
    auto sort_enforcer = enforcerExpr(EnforcerAxis::Sorting,
        propsFull(4, sortByColumns({"k"}), keyed), 50,
        propsFull(4, {}, keyed));
    group.physical_expressions.push_back(sort_enforcer);

    /// Sorted gather input: {[k], 4 nodes, columns empty}.
    auto picked = group.selectInputImplementation(
        propsFull(4, sortByColumns({"k"}), {}), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, sort_enforcer);
}

/// Strategy A (Gather -> Sort) must compose: a sort enforcer's self-input {[], 1 node} is produced by
/// a regular gather (empty sort, empty columns).
TEST(CascadesSelectInputImplementation, StrategyAComposes)
{
    Group group(0);
    CostConfig cost_config;

    auto gather = enforcerExpr(EnforcerAxis::Distribution,
        propsAt(1), 30, propsAt(4));
    group.physical_expressions.push_back(gather);

    auto picked = group.selectInputImplementation(propsAt(1), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, gather);
}

/// A sort enforcer's empty-sort self-input must not be satisfied by another sorted enforcer
/// (over-providing on the relaxed sort axis); it falls back to the acyclic base.
TEST(CascadesSelectInputImplementation, SortSelfLoopBlocked)
{
    Group group(0);
    CostConfig cost_config;

    auto sort_enforcer = enforcerExpr(EnforcerAxis::Sorting,
        propsAt(4, sortByColumns({"k"})), 5, propsAt(4));
    group.physical_expressions.push_back(sort_enforcer);

    /// Only the cheap self-referential sort enforcer exists: the empty-sort input has no acyclic source.
    auto none = group.selectInputImplementation(propsAt(4), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(none.expression, nullptr);

    /// With an acyclic base present, it is chosen instead of the sort enforcer.
    auto base = costedExpr(propsAt(4), 100);
    group.physical_expressions.push_back(base);
    auto picked = group.selectInputImplementation(propsAt(4), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, base);
}

/// A keyed shuffle/scatter must not satisfy an empty-columns self-input by over-providing on the
/// relaxed distribution-columns axis; it falls back to the acyclic base.
TEST(CascadesSelectInputImplementation, ShuffleSelfLoopBlocked)
{
    Group group(0);
    CostConfig cost_config;

    auto keyed = DistributionColumns{NameSet{"a"}};
    auto shuffle = enforcerExpr(EnforcerAxis::Distribution,
        propsFull(4, {}, keyed), 5, propsAt(4));
    group.physical_expressions.push_back(shuffle);

    auto none = group.selectInputImplementation(propsAt(4), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(none.expression, nullptr);

    auto base = costedExpr(propsAt(4), 100);
    group.physical_expressions.push_back(base);
    auto picked = group.selectInputImplementation(propsAt(4), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, base);
}

/// Broadcast over gather must remain valid: a broadcast's {[], 1 node} self-input is produced by a
/// regular gather, which is a distribution enforcer with empty sort and empty columns.
TEST(CascadesSelectInputImplementation, BroadcastOverGatherValid)
{
    Group group(0);
    CostConfig cost_config;

    auto gather = enforcerExpr(EnforcerAxis::Distribution, propsAt(1), 30, propsAt(4));
    group.physical_expressions.push_back(gather);

    auto picked = group.selectInputImplementation(propsAt(1), cost_config, {}, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, gather);
}

/// The eviction guard keeps the acyclic base in the best cache even when a cheaper over-providing
/// sorted enforcer exists, and the resolver finds the base once the enforcer is on the active path.
TEST(CascadesSelectInputImplementation, EvictionGuardKeepsBase)
{
    Group group(0);
    CostConfig cost_config;

    auto base = costedExpr(propsAt(1), 100);
    auto sorted_gather = enforcerExpr(EnforcerAxis::Distribution,
        propsAt(1, sortByColumns({"k"})), 10, propsAt(4, sortByColumns({"k"})));

    group.updateBestImplementation(base, cost_config);
    group.updateBestImplementation(sorted_gather, cost_config);

    /// The cheaper sorted gather wins an unguarded empty-sort lookup ...
    EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, sorted_gather);
    /// ... but the base survived eviction and the resolver returns it when the gather is excluded.
    std::unordered_set<GroupExpression *> active_path{sorted_gather.get()};
    auto picked = group.selectInputImplementation(propsAt(1), cost_config, active_path, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, base);
}

/// The resolver finds a costed acyclic implementation that lives only in physical_expressions
/// (never promoted to the best cache).
TEST(CascadesSelectInputImplementation, FallbackScansPhysicalExpressions)
{
    Group group(0);
    CostConfig cost_config;

    auto sorted_gather = enforcerExpr(EnforcerAxis::Distribution,
        propsAt(1, sortByColumns({"k"})), 10, propsAt(4, sortByColumns({"k"})));
    group.updateBestImplementation(sorted_gather, cost_config);

    /// The acyclic base is present only in physical_expressions.
    auto base = costedExpr(propsAt(1), 100);
    group.physical_expressions.push_back(base);

    std::unordered_set<GroupExpression *> active_path{sorted_gather.get()};
    auto picked = group.selectInputImplementation(propsAt(1), cost_config, active_path, /*input_is_self_referential=*/true);
    EXPECT_EQ(picked.expression, base);
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
