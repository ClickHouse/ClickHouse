#include <gtest/gtest.h>

#include <Columns/ColumnsNumber.h>
#include <Core/Block.h>
#include <DataTypes/DataTypesNumber.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/RuleUtils.h>
#include <Processors/QueryPlan/SortingStep.h>

using namespace DB;

/// A sorted gather over a partial top-N must be priced on the rows the partial physically
/// emits - min(input_rows, L * node_count) - not on the group statistics trimmed to the
/// final L. The partial records `physical_output_rows` when costed; the exchange reads it
/// from its selected child. Also covers the cost estimator marking an expression whose input
/// has no satisfying implementation as unbuildable.

namespace
{

SharedHeader makeHeader()
{
    ColumnWithTypeAndName column;
    column.name = "x";
    column.type = std::make_shared<DataTypeUInt64>();
    column.column = column.type->createColumn();
    return std::make_shared<const Block>(Block{{column}});
}

ExpressionStatistics makeStats(Float64 rows, Float64 bytes_per_row)
{
    ExpressionStatistics stats;
    stats.estimated_row_count = rows;
    stats.estimated_bytes_per_row = bytes_per_row;
    return stats;
}

SortDescription makeSortDescription()
{
    SortDescription description;
    description.emplace_back("x", 1, 1);
    return description;
}

}

TEST(CascadesPartialTopNCost, GatherPricedOnPhysicalRowsOfSelectedChild)
{
    constexpr Float64 input_rows = 10000;
    constexpr UInt64 limit = 100;
    constexpr size_t node_count = 4;
    constexpr Float64 bytes_per_row = 10;

    Memo memo(getLogger("gtest_cascades_partial_topn_cost"));
    CostEstimator estimator(memo);
    auto header = makeHeader();

    /// Raw input: 10000 rows across 4 nodes, pre-costed leaf.
    auto leaf = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    leaf->properties.distribution.node_count = node_count;
    leaf->cost = ExpressionCost{};
    auto leaf_group_id = memo.addGroup(leaf);
    auto leaf_group = memo.getGroup(leaf_group_id);
    leaf_group->statistics = makeStats(input_rows, bytes_per_row);
    leaf_group->updateBestImplementation(leaf, memo.getContext().cost_config);

    /// Partial top-N: bounded sort with limit L on each of the 4 nodes.
    auto partial_step = std::make_unique<SortingStep>(header, makeSortDescription(), limit, SortingStep::Settings(65000));
    partial_step->setPartialTopN();
    auto partial = std::make_shared<GroupExpression>(std::move(partial_step));
    partial->properties.distribution.node_count = node_count;
    partial->properties.sorting = makeSortDescription();
    partial->inputs.push_back({leaf_group_id, leaf->properties});
    auto partial_group_id = memo.addGroup(partial);
    auto partial_group = memo.getGroup(partial_group_id);
    /// Group statistics are logical: trimmed to the final L.
    partial_group->statistics = makeStats(limit, bytes_per_row);

    partial->cost = estimator.estimateCost(partial);
    /// Costing the partial records its physical output: min(10000, 100 * 4) = 400.
    ASSERT_TRUE(partial->physical_output_rows.has_value());
    EXPECT_DOUBLE_EQ(*partial->physical_output_rows, limit * node_count);
    partial_group->updateBestImplementation(partial, memo.getContext().cost_config);

    /// Sorted gather over the partial: its group statistics also say L, but the transfer
    /// must be priced on the 400 physical rows of the selected child.
    auto gather = std::make_shared<GroupExpression>(
        std::make_unique<GatherExchangeStep>(header, node_count, makeSortDescription()));
    gather->properties.distribution.node_count = 1;
    gather->inputs.push_back({partial_group_id, partial->properties});
    auto gather_group_id = memo.addGroup(gather);
    memo.getGroup(gather_group_id)->statistics = makeStats(limit, bytes_per_row);

    const auto gather_cost = estimator.estimateCost(gather);
    const Float64 physical_rows = limit * node_count;
    EXPECT_DOUBLE_EQ(gather_cost.cost.network, physical_rows * bytes_per_row);
    /// The limit above stops the receiver after the trimmed L rows, so the funnel and the
    /// receive merge are priced on L; the sender merge sees its node's share of the
    /// shipped rows.
    const auto & config = memo.getContext().cost_config;
    EXPECT_DOUBLE_EQ(gather_cost.cost.sequential,
        config.exchange_fixed_overhead
        + config.funnel_sequential_cost_per_row * limit
        + config.merge_sequential_cost_per_row * limit
        + config.merge_sequential_cost_per_row * physical_rows / node_count);
}

TEST(CascadesPartialTopNCost, PhysicalRowsClampedByInput)
{
    /// With fewer input rows than L * node_count the physical output cannot exceed the input.
    constexpr Float64 input_rows = 150;
    constexpr UInt64 limit = 100;
    constexpr size_t node_count = 4;

    Memo memo(getLogger("gtest_cascades_partial_topn_cost"));
    CostEstimator estimator(memo);
    auto header = makeHeader();

    auto leaf = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    leaf->properties.distribution.node_count = node_count;
    leaf->cost = ExpressionCost{};
    auto leaf_group_id = memo.addGroup(leaf);
    memo.getGroup(leaf_group_id)->statistics = makeStats(input_rows, 10);
    memo.getGroup(leaf_group_id)->updateBestImplementation(leaf, memo.getContext().cost_config);

    auto partial_step = std::make_unique<SortingStep>(header, makeSortDescription(), limit, SortingStep::Settings(65000));
    partial_step->setPartialTopN();
    auto partial = std::make_shared<GroupExpression>(std::move(partial_step));
    partial->properties.distribution.node_count = node_count;
    partial->inputs.push_back({leaf_group_id, leaf->properties});
    auto partial_group_id = memo.addGroup(partial);
    memo.getGroup(partial_group_id)->statistics = makeStats(limit, 10);

    partial->cost = estimator.estimateCost(partial);
    ASSERT_TRUE(partial->physical_output_rows.has_value());
    EXPECT_DOUBLE_EQ(*partial->physical_output_rows, input_rows);
}

/// An expression whose input has no implementation for the required properties cannot be
/// built into a plan; costing must mark it so it is never recorded as a group's best.
TEST(CascadesUnbuildableExpression, UnsatisfiableInputMarksCostUnbuildable)
{
    Memo memo(getLogger("gtest_cascades_unbuildable"));
    CostEstimator estimator(memo);
    auto header = makeHeader();

    /// Leaf group with a single-node implementation only.
    auto leaf = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    leaf->properties.distribution.node_count = 1;
    leaf->cost = ExpressionCost{};
    auto leaf_group_id = memo.addGroup(leaf);
    auto leaf_group = memo.getGroup(leaf_group_id);
    leaf_group->statistics = makeStats(100, 10);
    leaf_group->updateBestImplementation(leaf, memo.getContext().cost_config);

    /// A gather whose input demands the leaf at 4 nodes: no such implementation exists.
    auto unbuildable = std::make_shared<GroupExpression>(
        std::make_unique<GatherExchangeStep>(header, 4));
    unbuildable->properties.distribution.node_count = 1;
    ExpressionProperties four_nodes;
    four_nodes.distribution.node_count = 4;
    unbuildable->inputs.push_back({leaf_group_id, four_nodes});
    auto group_id = memo.addGroup(unbuildable);
    memo.getGroup(group_id)->statistics = makeStats(100, 10);

    const auto cost = estimator.estimateCost(unbuildable);
    EXPECT_FALSE(cost.buildable);

    /// A gather demanding the existing single-node input is buildable.
    auto buildable = std::make_shared<GroupExpression>(
        std::make_unique<GatherExchangeStep>(header, 1));
    buildable->properties.distribution.node_count = 1;
    ExpressionProperties one_node;
    one_node.distribution.node_count = 1;
    buildable->inputs.push_back({leaf_group_id, one_node});
    memo.getGroup(group_id)->addPhysicalExpression(buildable);
    buildable->group_id = group_id;

    EXPECT_TRUE(estimator.estimateCost(buildable).buildable);
}

/// A bounded partitioned sort keeps one stream per hash partition instead of merging to a
/// global top-N, so the top-N rules (which promise one sorted stream) must not take it.
TEST(CascadesTopNSort, PartitionedBoundedSortIsNotTopN)
{
    auto header = makeHeader();

    SortingStep plain_bounded(header, makeSortDescription(), /*limit_=*/10, SortingStep::Settings(65000));
    EXPECT_TRUE(isTopNSort(plain_bounded));

    SortingStep partitioned_bounded(
        header, makeSortDescription(), /*partition_by_description_=*/makeSortDescription(), /*limit_=*/10, SortingStep::Settings(65000));
    EXPECT_FALSE(isTopNSort(partitioned_bounded));
}

/// A sorted gather pays the receiver's merge of the bucket streams into one, and a sender
/// with several streams per node (disjoint layout) also pays its own merge before shipping.
TEST(CascadesSortedGatherCost, SendAndReceiveMergesPriced)
{
    constexpr Float64 rows = 10000;
    constexpr size_t node_count = 4;

    Memo memo(getLogger("gtest_cascades_partial_topn_cost"));
    CostEstimator estimator(memo);
    auto header = makeHeader();

    auto make_source = [&](StreamLayout layout)
    {
        auto source = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
        source->properties.distribution.node_count = node_count;
        source->properties.sorting = makeSortDescription();
        if (layout == StreamLayout::Disjoint)
            source->properties.setDisjointStreams({NameSet{"x"}});
        else
            source->properties.stream_layout = layout;
        source->cost = ExpressionCost{};
        auto group_id = memo.addGroup(source);
        memo.getGroup(group_id)->statistics = makeStats(rows, 10);
        memo.getGroup(group_id)->updateBestImplementation(source, memo.getContext().cost_config);
        return std::pair{group_id, source};
    };

    auto gather_cost_over = [&](StreamLayout layout, bool sorted)
    {
        auto [group_id, source] = make_source(layout);
        std::optional<SortDescription> maintain_sort;
        if (sorted)
            maintain_sort = makeSortDescription();
        auto gather = std::make_shared<GroupExpression>(
            std::make_unique<GatherExchangeStep>(header, node_count, std::move(maintain_sort)));
        gather->properties.distribution.node_count = 1;
        if (sorted)
            gather->properties.sorting = makeSortDescription();
        gather->inputs.push_back({group_id, source->properties});
        auto gather_group_id = memo.addGroup(gather);
        memo.getGroup(gather_group_id)->statistics = makeStats(rows, 10);
        return estimator.estimateCost(gather).cost.sequential;
    };

    const Float64 unordered = gather_cost_over(StreamLayout::Single, /*sorted=*/false);
    const Float64 over_single = gather_cost_over(StreamLayout::Single, /*sorted=*/true);
    const Float64 over_disjoint = gather_cost_over(StreamLayout::Disjoint, /*sorted=*/true);

    const auto & config = memo.getContext().cost_config;
    /// The receiver merges the sorted bucket streams into one: a full per-row merge.
    EXPECT_DOUBLE_EQ(over_single - unordered, config.merge_sequential_cost_per_row * rows);
    /// A sender with several streams per node merges its node's share before shipping.
    EXPECT_DOUBLE_EQ(over_disjoint - over_single, config.merge_sequential_cost_per_row * rows / node_count);
}
