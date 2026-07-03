#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Processors/QueryPlan/Optimizations/Cascades/ImplementationStrategy.h>

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
/// hash table on every node (work and serial build undivided), a shuffle join builds 1/N.
/// Network is not part of the join cost; the exchange steps model it.
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
    EXPECT_DOUBLE_EQ(broadcast.sequential, shuffle.sequential * inputs.parallelism);
    EXPECT_DOUBLE_EQ(broadcast.network, 0.0);
    EXPECT_DOUBLE_EQ(shuffle.network, 0.0);
}
