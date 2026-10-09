#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>
#include <Processors/QueryPlan/GatherExchangeStep.h>
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
        .exchange_rows_override = {},
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
        .exchange_rows_override = {},
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
        .exchange_rows_override = {},
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
        .exchange_rows_override = {},
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
        .exchange_rows_override = {},
        .unknown_exchange_rows = 100000,
        .config = config,
    };
    EXPECT_DOUBLE_EQ(estimateOperatorCost(known_inputs, nullptr).sequential, config.exchange_fixed_overhead + config.funnel_sequential_cost_per_row * 0.3);
}
