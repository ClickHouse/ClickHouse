#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <DataTypes/DataTypesNumber.h>
#include <Common/logger_useful.h>
#include <Core/Block.h>

using namespace DB;

namespace
{

ExpressionStatistics makeStats(Float64 rows, Float64 bytes_per_row)
{
    ExpressionStatistics stats;
    stats.estimated_row_count = rows;
    stats.estimated_bytes_per_row = bytes_per_row;
    return stats;
}

}

/// The broadcast-vs-shuffle choice rests on this asymmetry: a broadcast join builds the full
/// hash table on every node, a shuffle join builds 1/N. The build is work (it parallelizes
/// across the threads of a node), not a sequential phase. Network is not part of the join
/// cost; the exchange steps model it.
TEST(CascadesOperatorCost, BroadcastJoinBuildsFullTablePerNode)
{
    CostConfig config;
    auto left = makeStats(1'000'000, 10);
    auto right = makeStats(1'000, 10);
    auto output = makeStats(1'000'000, 20);

    CostInputs inputs{
        .step = nullptr,
        .output_stats = output,
        .input_stats = {&left, &right},
        .parallelism = 4.0,
        .node_count = 4.0,
        .input_copies = {},
        .config = config,
    };

    const Cost broadcast = BroadcastJoinStrategy{}.estimateOperatorCost(inputs);
    const Cost shuffle = ShuffleJoinStrategy{}.estimateOperatorCost(inputs);

    EXPECT_GT(broadcast.work, shuffle.work);
    /// The build parts differ exactly by the node count; the probe and output parts are equal.
    const Float64 probe_and_output = (left.estimated_row_count + output.estimated_row_count) / inputs.parallelism;
    EXPECT_DOUBLE_EQ(broadcast.work - probe_and_output, (shuffle.work - probe_and_output) * inputs.parallelism);
    /// The build is not a serial phase: `parallel_hash` shards it across threads.
    EXPECT_DOUBLE_EQ(broadcast.sequential, 0.0);
    EXPECT_DOUBLE_EQ(shuffle.sequential, 0.0);
    EXPECT_DOUBLE_EQ(broadcast.network, 0.0);
    EXPECT_DOUBLE_EQ(shuffle.network, 0.0);
}

/// A gather of an unknown join result is priced on the rows it cannot exceed, capped at the largest
/// known leaf, whatever its search value; any other unknown result keeps its search value.
TEST(CascadesOperatorCost, UnknownResultGatherPricedOnBound)
{
    CostConfig config;
    auto unknown = makeStats(0.3, 10);
    unknown.rows_unknown = true;
    unknown.search_value_is_join_product = true;
    unknown.max_row_count = 1e12;
    auto header = std::make_shared<const Block>();
    GatherExchangeStep gather(header, 3);
    CostInputs inputs{
        .step = &gather,
        .output_stats = unknown,
        .input_stats = {},
        .parallelism = 1.0,
        .node_count = 3.0,
        .input_copies = {},
        .unknown_exchange_rows = 100000,
        .config = config,
    };
    const Cost cost = estimateOperatorCost(inputs, nullptr);
    EXPECT_DOUBLE_EQ(cost.sequential, config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * 100000);
    EXPECT_DOUBLE_EQ(cost.network, 100000 * 10.0);

    /// A bound below the largest known leaf is the tighter figure.
    unknown.max_row_count = 500;
    const Cost bounded = estimateOperatorCost(inputs, nullptr);
    EXPECT_DOUBLE_EQ(bounded.sequential, config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * 500);

    /// An unknown join result with a search value of whole rows is still priced on the cap: the
    /// product of stand-in selectivities says nothing about its size.
    auto unknown_join = makeStats(100, 10);
    unknown_join.rows_unknown = true;
    unknown_join.search_value_is_join_product = true;
    unknown_join.max_row_count = 1e12;
    CostInputs unknown_join_inputs{
        .step = &gather,
        .output_stats = unknown_join,
        .input_stats = {},
        .parallelism = 1.0,
        .node_count = 3.0,
        .input_copies = {},
        .unknown_exchange_rows = 100000,
        .config = config,
    };
    EXPECT_DOUBLE_EQ(estimateOperatorCost(unknown_join_inputs, nullptr).sequential, config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * 100000);

    /// An unknown aggregation keeps its search value, below one row or not: a partial aggregation
    /// is priced as the groups it is expected to have, not as its input.
    auto unknown_rows = makeStats(0.5, 10);
    unknown_rows.rows_unknown = true;
    unknown_rows.max_row_count = 1e12;
    CostInputs unknown_rows_inputs{
        .step = &gather,
        .output_stats = unknown_rows,
        .input_stats = {},
        .parallelism = 1.0,
        .node_count = 3.0,
        .input_copies = {},
        .unknown_exchange_rows = 100000,
        .config = config,
    };
    EXPECT_DOUBLE_EQ(estimateOperatorCost(unknown_rows_inputs, nullptr).sequential, config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * 0.5);

    /// A known result keeps its estimate.
    auto known = makeStats(0.3, 10);
    CostInputs known_inputs{
        .step = &gather,
        .output_stats = known,
        .input_stats = {},
        .parallelism = 1.0,
        .node_count = 3.0,
        .input_copies = {},
        .unknown_exchange_rows = 100000,
        .config = config,
    };
    EXPECT_DOUBLE_EQ(estimateOperatorCost(known_inputs, nullptr).sequential, config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * 0.3);
}

/// A `DISTINCT` derives a search value of its own, so its unknown result is no longer the product of
/// the inputs of the unknown join below. Without clearing that mark, a gather of the distinct rows
/// would be priced at the cap for a join product, as if it moved the raw join result.
TEST(CascadesOperatorCost, DistinctOverUnknownJoinGatheredOnItsOwnSearchValue)
{
    Memo memo(getLogger("gtest_cascades_operator_cost"));

    ColumnWithTypeAndName column;
    column.name = "x";
    column.type = std::make_shared<DataTypeUInt64>();
    column.column = column.type->createColumn();
    auto header = std::make_shared<const Block>(Block{{column}});

    auto join_result = std::make_shared<GroupExpression>(QueryPlanStepPtr{});
    const auto join_group_id = memo.addGroup(join_result);
    auto join_stats = makeStats(1000, 10);
    join_stats.rows_unknown = true;
    join_stats.search_value_is_join_product = true;
    join_stats.max_row_count = 1e12;
    memo.getGroup(join_group_id)->statistics = join_stats;

    auto distinct = std::make_shared<GroupExpression>(
        std::make_unique<DistinctStep>(header, DistinctStep::Settings{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/false));
    distinct->inputs.push_back({join_group_id, join_result->properties});
    const auto distinct_group_id = memo.addGroup(distinct);

    const auto lookup = createEmptyStatistics();
    StatisticsDerivation(memo, *lookup).deriveStatistics(distinct_group_id);
    const auto & distinct_stats = *memo.getGroup(distinct_group_id)->statistics;
    ASSERT_TRUE(distinct_stats.rows_unknown);
    EXPECT_FALSE(distinct_stats.search_value_is_join_product);

    CostConfig config;
    GatherExchangeStep gather(header, 3);
    CostInputs inputs{
        .step = &gather,
        .output_stats = distinct_stats,
        .input_stats = {},
        .parallelism = 1.0,
        .node_count = 3.0,
        .input_copies = {},
        .unknown_exchange_rows = 100000,
        .config = config,
    };
    EXPECT_DOUBLE_EQ(
        estimateOperatorCost(inputs, nullptr).sequential,
        config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * distinct_stats.estimated_row_count);
}

/// The copies of the logical rows an input emits (a partial aggregation emits a state per group on each node) are the
/// rows that the operators above it read and move: each cost below doubles its row terms with two copies.
TEST(CascadesOperatorCost, ConsumersPriceCopiedRows)
{
    CostConfig config;
    auto left = makeStats(1'000, 10);
    auto right = makeStats(100, 10);
    auto output = makeStats(1'000, 20);
    const auto with_copies = [&](std::vector<Float64> input_copies, Float64 output_copies)
    {
        return CostInputs{
            .step = nullptr,
            .output_stats = output,
            .input_stats = {&left, &right},
            .parallelism = 4.0,
            .node_count = 4.0,
            .input_copies = std::move(input_copies),
            .output_copies = output_copies,
            .config = config,
        };
    };

    /// A join reads its probe and build sides and emits its output physically.
    const Cost join = ShuffleJoinStrategy{}.estimateOperatorCost(with_copies({1.0, 1.0}, 1.0));
    const Cost copied_join = ShuffleJoinStrategy{}.estimateOperatorCost(with_copies({2.0, 2.0}, 2.0));
    EXPECT_DOUBLE_EQ(copied_join.work, 2 * join.work);

    /// A partial aggregation holds and emits its states, and reads its input physically.
    const Cost partial = PartialAggregationStrategy{}.estimateOperatorCost(with_copies({1.0}, 1.0));
    const Cost copied_partial = PartialAggregationStrategy{}.estimateOperatorCost(with_copies({2.0}, 2.0));
    EXPECT_DOUBLE_EQ(copied_partial.work, 2 * partial.work);

    /// A preliminary `DISTINCT` stores the copies it emits.
    auto header = std::make_shared<const Block>(Block{{DataTypeUInt64().createColumn(), std::make_shared<DataTypeUInt64>(), "x"}});
    DistinctStep preliminary(header, DistinctStep::Settings{}, /*limit_hint_=*/0, Names{"x"}, /*pre_distinct_=*/true);
    auto distinct_inputs = with_copies({2.0}, 2.0);
    distinct_inputs.step = &preliminary;
    auto single_distinct_inputs = with_copies({1.0}, 1.0);
    single_distinct_inputs.step = &preliminary;
    EXPECT_DOUBLE_EQ(estimateOperatorCost(distinct_inputs, nullptr).work, 2 * estimateOperatorCost(single_distinct_inputs, nullptr).work);

    /// An exchange moves the copies its input emits, which it passes on.
    GatherExchangeStep gather(header, 4);
    auto exchange_inputs = with_copies({1.0}, 1.0);
    exchange_inputs.step = &gather;
    auto copied_exchange_inputs = with_copies({2.0}, 2.0);
    copied_exchange_inputs.step = &gather;
    EXPECT_DOUBLE_EQ(estimateOperatorCost(copied_exchange_inputs, nullptr).network,
        2 * estimateOperatorCost(exchange_inputs, nullptr).network);
}
