#include <gtest/gtest.h>

#include <limits>

#include <Processors/QueryPlan/RuntimeFilterBloomSizing.h>
#include <Common/Exception.h>

namespace DB
{
namespace
{

TEST(RuntimeFilterBloomSizing, ResolvesDefaultsAndValidatesMaxima)
{
    const auto defaults = resolveRuntimeBloomFilterDefaults({0, 0});
    EXPECT_EQ(defaults.bytes, 512 * 1024);
    EXPECT_EQ(defaults.hash_functions, 3);

    EXPECT_NO_THROW(validateRuntimeBloomFilterParameters({16 * 1024 * 1024, 10}));
    EXPECT_THROW(validateRuntimeBloomFilterParameters({16 * 1024 * 1024 + 1, 10}), Exception);
    EXPECT_THROW(validateRuntimeBloomFilterParameters({16 * 1024 * 1024, 11}), Exception);
}

TEST(RuntimeFilterBloomSizing, OccupancyHandlesEdgesAndIsMonotonic)
{
    const RuntimeBloomFilterParameters parameters{512 * 1024, 3};
    EXPECT_DOUBLE_EQ(estimateRuntimeBloomFilterSetBitsRatio(0, parameters), 0.0);

    const Float64 tiny = estimateRuntimeBloomFilterSetBitsRatio(1.0, RuntimeBloomFilterParameters{std::numeric_limits<UInt64>::max(), 1});
    EXPECT_GT(tiny, 0.0);
    EXPECT_LT(estimateRuntimeBloomFilterSetBitsRatio(100, parameters), estimateRuntimeBloomFilterSetBitsRatio(1000, parameters));
}

TEST(RuntimeFilterBloomSizing, StatisticsSizingRetainsCapAndConfiguredMinimum)
{
    EXPECT_EQ(growRuntimeBloomFilterBytesFromStats(1ULL << 40, 3, 512 * 1024, 0.5), 4 * 1024 * 1024);
    EXPECT_EQ(growRuntimeBloomFilterBytesFromStats(1ULL << 40, 3, 8 * 1024 * 1024, 0.5), 8 * 1024 * 1024);
}

}
}
