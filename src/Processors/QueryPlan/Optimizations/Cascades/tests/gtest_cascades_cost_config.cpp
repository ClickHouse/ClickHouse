#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/Cascades/Cost.h>
#include <Common/Exception.h>
#include <cmath>
#include <limits>

using namespace DB;

/// A valid config parses and applies the given weights.
TEST(CascadesCostConfig, ValidConfigApplies)
{
    auto config = parseCostConfig(R"({"work_weight":2,"network_weight":3,"sequential_weight":4,"exchange_fixed_overhead":5})");
    EXPECT_DOUBLE_EQ(config.work_weight, 2.0);
    EXPECT_DOUBLE_EQ(config.network_weight, 3.0);
    EXPECT_DOUBLE_EQ(config.sequential_weight, 4.0);
    EXPECT_DOUBLE_EQ(config.exchange_fixed_overhead, 5.0);
}

/// Zero weights are allowed on purpose (isolate/ignore a dimension in tests or tuning).
TEST(CascadesCostConfig, ZeroWeightAllowed)
{
    auto config = parseCostConfig(R"({"network_weight":0})");
    EXPECT_DOUBLE_EQ(config.network_weight, 0.0);
}

/// Negative and non-finite weights are rejected (they break cost ordering / non-negativity).
TEST(CascadesCostConfig, NegativeAndNonFiniteWeightsRejected)
{
    EXPECT_THROW(parseCostConfig(R"({"work_weight":-1})"), Exception);
    EXPECT_THROW(parseCostConfig(R"({"sequential_weight":-0.001})"), Exception);
    /// A non-object JSON value is not a valid config.
    EXPECT_THROW(parseCostConfig("42"), Exception);
    EXPECT_THROW(parseCostConfig("not json"), Exception);
}

/// An infinite cost component stays infinite under any weights, including a zero weight: `total` must
/// not produce NaN from `inf * 0`, otherwise an unsatisfiable subtree could compare as "not worse".
TEST(CascadesCostConfig, InfinityAbsorbedByTotal)
{
    Cost unsatisfiable = Cost::infinity();

    CostConfig zero_network = parseCostConfig(R"({"network_weight":0})");
    Float64 total = unsatisfiable.total(zero_network);
    EXPECT_FALSE(std::isnan(total));
    EXPECT_TRUE(std::isinf(total));

    /// A single infinite component is enough to make the whole cost infinite.
    Cost partial{.work = 1.0, .network = std::numeric_limits<Float64>::infinity(), .sequential = 1.0};
    EXPECT_TRUE(std::isinf(partial.total(CostConfig{})));

    /// Ordinary finite costs are unaffected.
    Cost finite{.work = 10.0, .network = 20.0, .sequential = 0.0};
    EXPECT_DOUBLE_EQ(finite.total(CostConfig{.work_weight = 1, .network_weight = 2, .sequential_weight = 1000}), 10.0 + 40.0);
}
