#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Properties.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Rule.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Statistics.h>
#include <Processors/QueryPlan/ScatterExchangeStep.h>

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

/// A self-referential enforcer that an enforcer rule made for `made_for`: produces `output`,
/// requires `input_required` from its own group (group 0 in these tests).
GroupExpressionPtr enforcerExpr(ExpressionProperties made_for, ExpressionProperties output, Float64 cost, ExpressionProperties input_required)
{
    auto expression = costedExpr(std::move(output), cost);
    expression->enforced_for.push_back(std::move(made_for));
    expression->inputs.push_back({.group_id = 0, .required_properties = std::move(input_required)});
    return expression;
}

size_t bestImplementationCount(const Group & group)
{
    size_t count = 0;
    for (const auto & [_, bucket] : group.best_implementations)
        count += bucket.size();
    return count;
}

SharedHeader makeHeader()
{
    ColumnWithTypeAndName column;
    column.name = "x";
    column.type = std::make_shared<DataTypeUInt64>();
    column.column = column.type->createColumn();
    return std::make_shared<const Block>(Block{{column}});
}

/// Gives a test the helper that rules add their physical expressions with.
class TestEnforcerRule : public IOptimizationRule
{
public:
    String getName() const override { return "TestEnforcer"; }
    bool checkPattern(GroupExpressionPtr, const ExpressionProperties &, const Memo &) const override { return true; }
    Promise getPromise() const override { return 1; }
    bool isTransformation() const override { return false; }
    using IOptimizationRule::addPhysicalToMemo;

protected:
    std::vector<GroupExpressionPtr> applyImpl(GroupExpressionPtr, const ExpressionProperties &, Memo &) const override { return {}; }
};

}

/// Strategy B (Sort-per-node -> SortedGather) must compose, also over keyed data: the input of a
/// sorted gather requires a sort and no distribution columns. The enforcement of these required
/// properties makes the sort on each node from a keyed source, so the sort has the keyed columns
/// of its source and still is the answer.
TEST(CascadesEnforcerSelection, StrategyBComposesOverKeyedData)
{
    Group group(0);
    CostConfig cost_config;

    auto keyed = DistributionColumns{NameSet{"a"}};
    const auto gather_input = propsFull(4, sortByColumns({"k"}), {});
    auto sort_enforcer = enforcerExpr(gather_input,
        propsFull(4, sortByColumns({"k"}), keyed), 50,
        propsFull(4, {}, keyed));
    group.updateBestImplementation(sort_enforcer, cost_config);

    EXPECT_EQ(group.getBestImplementation(gather_input, cost_config).expression, sort_enforcer);
}

/// Strategy A (Gather -> Sort) must compose: the input {[], 1 node} of a sort is answered by a
/// regular gather. The gather answers it also when it was made as a step towards the sorted
/// required properties, because its own properties are equal to these required properties.
TEST(CascadesEnforcerSelection, StrategyAComposes)
{
    CostConfig cost_config;

    for (const auto & made_for : {propsAt(1), propsAt(1, sortByColumns({"k"}))})
    {
        Group group(0);
        auto gather = enforcerExpr(made_for, propsAt(1), 30, propsAt(4));
        group.updateBestImplementation(gather, cost_config);

        EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, gather);
    }
}

/// A sort satisfies the required properties of its own input, which has no sort. It is not an
/// answer to them, also when it is cheaper than the base.
TEST(CascadesEnforcerSelection, SortDoesNotAnswerItsInput)
{
    Group group(0);
    CostConfig cost_config;

    auto sort_enforcer = enforcerExpr(propsAt(4, sortByColumns({"k"})),
        propsAt(4, sortByColumns({"k"})), 5, propsAt(4));
    group.updateBestImplementation(sort_enforcer, cost_config);

    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, nullptr);
    EXPECT_EQ(group.getBestImplementation(propsAt(4, sortByColumns({"k"})), cost_config).expression, sort_enforcer);

    auto base = costedExpr(propsAt(4), 100);
    group.updateBestImplementation(base, cost_config);
    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, base);
}

/// A keyed shuffle or scatter satisfies the required properties of its own input, which has no
/// distribution columns. It is not an answer to them.
TEST(CascadesEnforcerSelection, ShuffleDoesNotAnswerItsInput)
{
    Group group(0);
    CostConfig cost_config;

    auto keyed = DistributionColumns{NameSet{"a"}};
    auto shuffle = enforcerExpr(propsFull(4, {}, keyed),
        propsFull(4, {}, keyed), 5, propsAt(4));
    group.updateBestImplementation(shuffle, cost_config);

    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, nullptr);
    EXPECT_EQ(group.getBestImplementation(propsFull(4, {}, keyed), cost_config).expression, shuffle);

    auto base = costedExpr(propsAt(4), 100);
    group.updateBestImplementation(base, cost_config);
    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, base);
}

/// An enforcer answers all the required properties that enforcer rules made it for: the group
/// keeps one expression when two rules make equal ones.
TEST(CascadesEnforcerSelection, EnforcerMadeForSeveralRequiredProperties)
{
    Group group(0);
    CostConfig cost_config;

    auto single_stream = propsAt(1, sortByColumns({"k"}));
    single_stream.stream_layout = StreamLayout::Single;

    /// A sorted gather gives one stream. It is made for required properties without a layout.
    auto sorted_gather = enforcerExpr(propsAt(1, sortByColumns({"k"})), single_stream, 10, propsAt(4, sortByColumns({"k"})));
    group.updateBestImplementation(sorted_gather, cost_config);

    EXPECT_EQ(group.getBestImplementation(propsAt(1, sortByColumns({"k"})), cost_config).expression, sorted_gather);
    /// It has no sort requirement to answer here: a plain gather is the answer to these.
    EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, nullptr);

    sorted_gather->enforced_for.push_back(propsAt(1));
    EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, sorted_gather);
}

/// The properties of an enforcer can be stronger than the required properties in the stream
/// layout or in the columns of an equivalence set. They satisfy the required properties then, but
/// are not equal to them, so the enforcer answers only if a rule made it for them.
TEST(CascadesEnforcerSelection, StrongerPropertiesNeedTheRequiredProperties)
{
    auto single_stream = propsAt(4);
    single_stream.stream_layout = StreamLayout::Single;
    auto disjoint_streams = propsAt(4);
    disjoint_streams.setDisjointStreams({NameSet{"k"}});

    /// A scatter gives one stream on each node. It is made for required properties without a layout.
    auto scatter = enforcerExpr(propsAt(4), single_stream, 10, propsAt(1));
    EXPECT_TRUE(scatter->answers(propsAt(4)));
    EXPECT_TRUE(scatter->answers(single_stream));
    ASSERT_TRUE(disjoint_streams.isSatisfiedBy(single_stream));
    EXPECT_FALSE(scatter->answers(disjoint_streams));

    /// A shuffle by a column that is equal to another one.
    const auto by_a_or_b = propsFull(4, {}, DistributionColumns{NameSet{"a", "b"}});
    const auto by_a = propsFull(4, {}, DistributionColumns{NameSet{"a"}});
    auto shuffle = enforcerExpr(by_a_or_b, by_a_or_b, 10, propsAt(4));
    ASSERT_TRUE(by_a.isSatisfiedBy(by_a_or_b));
    EXPECT_FALSE(shuffle->answers(by_a));
    shuffle->enforced_for.push_back(by_a);
    EXPECT_TRUE(shuffle->answers(by_a));
}

/// Two enforcer rules can make equal enforcers for different required properties: the distribution
/// enforcer makes a sorted gather over a sorted source also for required properties without a
/// sort. The group keeps one expression, and it answers both. It does not matter if the expression
/// gets its cost before or after the second rule.
TEST(CascadesEnforcerSelection, RuleAddsRequiredPropertiesToEqualEnforcer)
{
    const auto sorted = sortByColumns({"k"});
    auto gather_output = propsAt(1, sorted);
    gather_output.stream_layout = StreamLayout::Single;

    for (bool cost_before_second_rule : {true, false})
    {
        Memo memo(getLogger("gtest_cascades_best_implementation"));
        const auto & cost_config = memo.getContext().cost_config;
        TestEnforcerRule rule;

        auto source = costedExpr(propsAt(4, sorted), 100);
        auto group = memo.getGroup(memo.addGroup(source));
        group->addPhysicalExpression(source);

        auto make_sorted_gather = [&](const ExpressionProperties & required_properties)
        {
            auto gather = makeEnforcerExpression(source, QueryPlanStepPtr{}, propsAt(4, sorted), gather_output, required_properties);
            gather->cost = ExpressionCost{.cost = Cost{.work = 10}, .subtree_cost = Cost{.work = 110}};
            return gather;
        };

        auto gather = make_sorted_gather(propsAt(1, sorted));
        ASSERT_EQ(rule.addPhysicalToMemo(gather, propsAt(1, sorted), memo).size(), 1u);
        if (cost_before_second_rule)
        {
            group->updateBestImplementation(gather, cost_config);
            EXPECT_EQ(group->getBestImplementation(propsAt(1, sorted), cost_config).expression, gather);
            EXPECT_EQ(group->getBestImplementation(propsAt(1), cost_config).expression, nullptr);
        }

        /// The equal expression is dropped, and the rule has nothing new to cost.
        EXPECT_TRUE(rule.addPhysicalToMemo(make_sorted_gather(propsAt(1)), propsAt(1), memo).empty());
        EXPECT_EQ(group->physical_expressions.size(), 2u);
        EXPECT_EQ(gather->enforced_for.size(), 2u);

        if (!cost_before_second_rule)
            group->updateBestImplementation(gather, cost_config);
        EXPECT_EQ(group->getBestImplementation(propsAt(1, sorted), cost_config).expression, gather);
        EXPECT_EQ(group->getBestImplementation(propsAt(1), cost_config).expression, gather);
    }
}

/// A gather and a scatter of one group take each other's required properties. When exchanges
/// cost nothing, both cost the same as the plan below them. The plan below stays a best
/// implementation, so the chain that plan extraction builds always has an answer that is not on it.
TEST(CascadesEnforcerSelection, FreeExchangesKeepThePlanBelowThem)
{
    Memo memo(getLogger("gtest_cascades_best_implementation"));
    OptimizerContext context;
    context.cost_config.network_weight = 0;
    context.cost_config.sequential_weight = 0;
    memo.setContext(context);
    const auto & cost_config = memo.getContext().cost_config;
    CostEstimator estimator(memo);
    auto header = makeHeader();

    auto base = costedExpr(propsAt(4), 100);
    auto group = memo.getGroup(memo.addGroup(base));
    ExpressionStatistics statistics;
    statistics.estimated_row_count = 1000;
    statistics.estimated_bytes_per_row = 10;
    group->statistics = statistics;
    group->addPhysicalExpression(base);
    group->updateBestImplementation(base, cost_config);

    auto gather = makeEnforcerExpression(base, std::make_unique<GatherExchangeStep>(header, 4), propsAt(4), propsAt(1), propsAt(1));
    group->addPhysicalExpression(gather);
    gather->cost = estimator.estimateCost(gather);
    ASSERT_TRUE(gather->cost->buildable);
    group->updateBestImplementation(gather, cost_config);

    auto scatter_output = propsAt(4);
    scatter_output.stream_layout = StreamLayout::Single;
    auto scatter = makeEnforcerExpression(base, std::make_unique<ScatterExchangeStep>(header, Names{}, 4), propsAt(1), scatter_output, propsAt(4));
    group->addPhysicalExpression(scatter);
    scatter->cost = estimator.estimateCost(scatter);
    ASSERT_TRUE(scatter->cost->buildable);
    group->updateBestImplementation(scatter, cost_config);

    const Float64 base_cost = base->cost->subtree_cost.total(cost_config);
    EXPECT_EQ(gather->cost->subtree_cost.total(cost_config), base_cost);
    EXPECT_EQ(scatter->cost->subtree_cost.total(cost_config), base_cost);
    EXPECT_EQ(bestImplementationCount(*group), 3u);

    /// The gather is the only answer for one node. Below it the plan on four nodes is the first
    /// answer; the scatter costs the same and came later.
    EXPECT_EQ(group->getBestImplementation(propsAt(1), cost_config).expression, gather);
    EXPECT_EQ(group->getBestImplementation(propsAt(4), cost_config, {gather.get()}).expression, base);
    /// A chain that has the scatter and the gather on it has no further answer for one node.
    EXPECT_EQ(group->getBestImplementation(propsAt(1), cost_config, {scatter.get(), gather.get()}).expression, nullptr);
    /// For four nodes it still has the plan below.
    EXPECT_EQ(group->getBestImplementation(propsAt(4), cost_config, {scatter.get(), gather.get()}).expression, base);
}

/// An enforcer does not remove another implementation from the best implementations, also when
/// it is cheaper and has the same properties. Plan extraction excludes the enforcer when it is on
/// the chain that it builds, and then needs the other implementation.
TEST(CascadesEnforcerSelection, EnforcerKeepsOtherImplementations)
{
    CostConfig cost_config;

    for (bool base_first : {true, false})
    {
        Group group(0);
        auto base = costedExpr(propsAt(1), 100);
        auto gather = enforcerExpr(propsAt(1), propsAt(1), 10, propsAt(4));

        group.updateBestImplementation(base_first ? base : gather, cost_config);
        group.updateBestImplementation(base_first ? gather : base, cost_config);

        EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, gather);
        EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config, {gather.get()}).expression, base);
    }
}

/// An expression that is not an enforcer replaces a costlier enforcer with weaker properties:
/// it answers all the required properties that the enforcer answers.
TEST(CascadesEnforcerSelection, CheaperImplementationReplacesEnforcer)
{
    Group group(0);
    CostConfig cost_config;

    auto gather = enforcerExpr(propsAt(1), propsAt(1), 30, propsAt(4));
    auto base = costedExpr(propsAt(1), 10);
    group.updateBestImplementation(gather, cost_config);
    group.updateBestImplementation(base, cost_config);

    EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config).expression, base);
    EXPECT_EQ(group.getBestImplementation(propsAt(1), cost_config, {base.get()}).expression, nullptr);
}

/// An expression that gets a new cost stays in the best implementations once. With a lower cost
/// it can remove an entry that it now dominates.
TEST(CascadesBestImplementation, ExpressionWithNewCostStaysOnce)
{
    Group group(0);
    CostConfig cost_config;

    auto plain = costedExpr(propsAt(4), 50);
    auto sorted = costedExpr(propsAt(4, sortByColumns({"k"})), 60);
    group.updateBestImplementation(plain, cost_config);
    group.updateBestImplementation(sorted, cost_config);
    EXPECT_EQ(bestImplementationCount(group), 2u);
    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, plain);

    group.updateBestImplementation(sorted, cost_config);
    EXPECT_EQ(bestImplementationCount(group), 2u);

    sorted->cost = ExpressionCost{.cost = Cost{.work = 10}, .subtree_cost = Cost{.work = 10}};
    group.updateBestImplementation(sorted, cost_config);
    EXPECT_EQ(bestImplementationCount(group), 1u);
    EXPECT_EQ(group.getBestImplementation(propsAt(4), cost_config).expression, sorted);
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

/// A partitioned sort satisfies a disjoint-streams requirement at a strictly lower cost than
/// the plain sort (it pays no merge), so the cost comparison alone picks the plan that keeps
/// the streams split; a plain sort with its merge cost never suppresses it.
TEST(CascadesBestImplementation, CheaperDisjointWinsDisjointRequirement)
{
    Group group(0);
    CostConfig cost_config;

    auto single_props = propsAt(4, sortByColumns({"k"}));
    single_props.stream_layout = StreamLayout::Single;
    auto single_sort = costedExpr(single_props, 50);

    auto disjoint_props = propsAt(4, sortByColumns({"k"}));
    disjoint_props.setDisjointStreams({NameSet{"k"}});
    auto disjoint_sort = costedExpr(disjoint_props, 45);

    group.updateBestImplementation(single_sort, cost_config);
    group.updateBestImplementation(disjoint_sort, cost_config);

    auto disjoint_required = propsAt(4, sortByColumns({"k"}));
    disjoint_required.setDisjointStreams({NameSet{"k"}});
    EXPECT_EQ(group.getBestImplementation(disjoint_required, cost_config).expression, disjoint_sort);

    /// The requirement for one stream is served only by the single-stream sort.
    auto single_required = propsAt(4, sortByColumns({"k"}));
    single_required.stream_layout = StreamLayout::Single;
    EXPECT_EQ(group.getBestImplementation(single_required, cost_config).expression, single_sort);
}

/// Implementations of different distribution shapes do not satisfy each other's requirements.
/// A rule that runs for a requirement which accepts no exchange from `DistributionEnforcer` marks
/// the expressions that have the required distribution. Its other expressions (a variant for
/// another node count, or without the key columns) get no mark, and neither does anything that is
/// made for a requirement without distribution columns.
TEST(CascadesBestImplementation, MarkWithoutEnforcerOnlyOnTheRequiredDistribution)
{
    Memo memo(getLogger("gtest_cascades_best_implementation"));
    TestEnforcerRule rule;
    const GroupId group_id = memo.addGroup(costedExpr(propsAt(1), 1));

    auto physical = [&](ExpressionProperties properties)
    {
        auto expression = costedExpr(std::move(properties), 10);
        expression->group_id = group_id;
        return expression;
    };

    const auto keyed = propsFull(4, {}, DistributionColumns{NameSet{"k"}});
    auto keyed_without_enforcer = keyed;
    keyed_without_enforcer.distribution.without_enforcer = true;
    auto unkeyed_without_enforcer = propsAt(4);
    unkeyed_without_enforcer.distribution.without_enforcer = true;

    auto with_required_distribution = physical(keyed);
    rule.addPhysicalToMemo(with_required_distribution, keyed_without_enforcer, memo);
    EXPECT_TRUE(with_required_distribution->properties.distribution.without_enforcer);

    /// A rule that copies the required distribution into the properties copies the mark too.
    auto other_columns = physical(propsFull(4, {}, DistributionColumns{NameSet{"other"}}));
    other_columns->properties.distribution.without_enforcer = true;
    rule.addPhysicalToMemo(other_columns, keyed_without_enforcer, memo);
    EXPECT_FALSE(other_columns->properties.distribution.without_enforcer);

    auto without_columns = physical(propsAt(4));
    rule.addPhysicalToMemo(without_columns, keyed_without_enforcer, memo);
    EXPECT_FALSE(without_columns->properties.distribution.without_enforcer);

    auto other_node_count = physical(propsFull(8, {}, DistributionColumns{NameSet{"k"}}));
    rule.addPhysicalToMemo(other_node_count, keyed_without_enforcer, memo);
    EXPECT_FALSE(other_node_count->properties.distribution.without_enforcer);

    auto for_plain_requirement = physical(propsFull(4, sortByColumns({"s"}), DistributionColumns{NameSet{"k"}}));
    rule.addPhysicalToMemo(for_plain_requirement, keyed, memo);
    EXPECT_FALSE(for_plain_requirement->properties.distribution.without_enforcer);

    auto for_unkeyed_requirement = physical(propsAt(4, sortByColumns({"s"})));
    rule.addPhysicalToMemo(for_unkeyed_requirement, unkeyed_without_enforcer, memo);
    EXPECT_FALSE(for_unkeyed_requirement->properties.distribution.without_enforcer);
}

/// A cheaper implementation that has its distribution from an enforcer exchange does not replace a
/// costlier one that has it without, and is not selected for a requirement that accepts no exchange.
/// The order in which they are costed does not matter.
TEST(CascadesBestImplementation, RequirementWithoutEnforcer)
{
    CostConfig cost_config;
    const auto keyed = propsFull(4, {}, {NameSet{"k"}});
    auto keyed_without_enforcer = keyed;
    keyed_without_enforcer.distribution.without_enforcer = true;

    for (bool exchange_first : {true, false})
    {
        Group group(0);
        auto over_exchange = costedExpr(keyed, 10);
        auto without_exchange = costedExpr(keyed_without_enforcer, 100);
        group.updateBestImplementation(exchange_first ? over_exchange : without_exchange, cost_config);
        group.updateBestImplementation(exchange_first ? without_exchange : over_exchange, cost_config);

        EXPECT_EQ(group.getBestImplementation(keyed, cost_config).expression, over_exchange);
        EXPECT_EQ(group.getBestImplementation(keyed_without_enforcer, cost_config).expression, without_exchange);
    }

    /// Without such an implementation the requirement has no plan.
    Group only_exchange(0);
    only_exchange.updateBestImplementation(costedExpr(keyed, 10), cost_config);
    EXPECT_EQ(only_exchange.getBestImplementation(keyed_without_enforcer, cost_config).expression, nullptr);
}

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
