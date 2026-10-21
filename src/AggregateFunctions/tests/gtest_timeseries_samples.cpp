#include <AggregateFunctions/TimeSeries/AggregateFunctionTimeseriesSamples.h>

#include <gtest/gtest.h>

#include <array>
#include <cmath>
#include <limits>
#include <utility>
#include <vector>

namespace
{

using Samples = DB::AggregateFunctionTimeseriesSamples<UInt64, Float64>;
using Sample = std::pair<UInt64, Float64>;

template <size_t size>
void addBatch(Samples & samples, const std::array<UInt64, size> & timestamps, const std::array<Float64, size> & values)
{
    samples.addMany(timestamps.data(), values.data(), size);
}

std::vector<Sample> collectSamples(const Samples & samples)
{
    std::vector<Sample> result;
    samples.forEachSample([&result](UInt64 timestamp, Float64 value)
    {
        result.emplace_back(timestamp, value);
    });
    return result;
}

TEST(TimeseriesSamples, StrictlyIncreasingBatchesRemainInOrder)
{
    Samples samples;
    addBatch(samples, std::array<UInt64, 2>{10, 20}, std::array<Float64, 2>{1, 2});
    addBatch(samples, std::array<UInt64, 2>{30, 40}, std::array<Float64, 2>{3, 4});

    EXPECT_EQ(collectSamples(samples), (std::vector<Sample>{{10, 1}, {20, 2}, {30, 3}, {40, 4}}));
}

TEST(TimeseriesSamples, DuplicateAtBatchBoundaryKeepsLargestValue)
{
    Samples samples;
    addBatch(samples, std::array<UInt64, 2>{10, 20}, std::array<Float64, 2>{1, 3});
    addBatch(samples, std::array<UInt64, 2>{20, 30}, std::array<Float64, 2>{7, 4});

    EXPECT_EQ(collectSamples(samples), (std::vector<Sample>{{10, 1}, {20, 7}, {30, 4}}));
}

TEST(TimeseriesSamples, InternalDuplicateKeepsLargestValue)
{
    Samples samples;
    addBatch(samples, std::array<UInt64, 4>{10, 20, 20, 30}, std::array<Float64, 4>{1, 9, 4, 3});

    EXPECT_EQ(collectSamples(samples), (std::vector<Sample>{{10, 1}, {20, 9}, {30, 3}}));
}

TEST(TimeseriesSamples, OutOfOrderBatchIsSorted)
{
    Samples samples;
    addBatch(samples, std::array<UInt64, 4>{30, 10, 40, 20}, std::array<Float64, 4>{3, 1, 4, 2});

    EXPECT_EQ(collectSamples(samples), (std::vector<Sample>{{10, 1}, {20, 2}, {30, 3}, {40, 4}}));
}

TEST(TimeseriesSamples, DuplicateNaNLosesToLargestRealValue)
{
    const Float64 nan = std::numeric_limits<Float64>::quiet_NaN();
    Samples samples;
    addBatch(
        samples,
        std::array<UInt64, 7>{10, 10, 10, 20, 20, 30, 30},
        std::array<Float64, 7>{nan, 2, 5, 5, nan, nan, nan});

    const auto result = collectSamples(samples);
    ASSERT_EQ(result.size(), 3);
    EXPECT_EQ(result[0], (Sample{10, 5}));
    EXPECT_EQ(result[1], (Sample{20, 5}));
    EXPECT_EQ(result[2].first, 30);
    EXPECT_TRUE(std::isnan(result[2].second));
}

TEST(TimeseriesSamples, CounterResetIsRetainedAsSampleValue)
{
    Samples samples;
    addBatch(
        samples,
        std::array<UInt64, 4>{10, 20, 30, 40},
        std::array<Float64, 4>{100, 125, 3, 9});

    EXPECT_EQ(collectSamples(samples), (std::vector<Sample>{{10, 100}, {20, 125}, {30, 3}, {40, 9}}));
}

}
