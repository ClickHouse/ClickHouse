#include <Storages/TimeSeries/PromQLNativeAdmission.h>

#include <gtest/gtest.h>

#include <limits>


namespace DB
{
namespace
{

TEST(PromQLNativeAdmission, ExactSeriesAndGridBoundaries)
{
    const auto exact = evaluatePromQLNativeWorkloadAdmission(3, 3, 3, 9);
    EXPECT_EQ(exact.reason, PromQLNativeAdmissionReason::Admitted);
    ASSERT_TRUE(exact.grid_cells);
    EXPECT_EQ(*exact.grid_cells, 9);

    const auto series = evaluatePromQLNativeWorkloadAdmission(3, 3, 2, 9);
    EXPECT_EQ(series.reason, PromQLNativeAdmissionReason::SeriesLimit);
    EXPECT_FALSE(series.grid_cells);

    const auto grid = evaluatePromQLNativeWorkloadAdmission(3, 3, 3, 8);
    EXPECT_EQ(grid.reason, PromQLNativeAdmissionReason::GridCellLimit);
    ASSERT_TRUE(grid.grid_cells);
    EXPECT_EQ(*grid.grid_cells, 9);

    EXPECT_EQ(evaluatePromQLNativeWorkloadAdmission(3, 3, 4, 10).reason, PromQLNativeAdmissionReason::Admitted);
}

TEST(PromQLNativeAdmission, DisabledGridDoesNotMultiply)
{
    constexpr auto maximum = std::numeric_limits<UInt64>::max();
    const auto result = evaluatePromQLNativeWorkloadAdmission(maximum, maximum, maximum, 0);
    EXPECT_EQ(result.reason, PromQLNativeAdmissionReason::Admitted);
    EXPECT_FALSE(result.grid_cells);

    EXPECT_EQ(evaluatePromQLNativeWorkloadAdmission(3, maximum, 2, 0).reason, PromQLNativeAdmissionReason::SeriesLimit);
}

TEST(PromQLNativeAdmission, OverflowAndReasonPrecedence)
{
    constexpr auto maximum = std::numeric_limits<UInt64>::max();
    const auto overflow = evaluatePromQLNativeWorkloadAdmission(2, maximum, 2, maximum);
    EXPECT_EQ(overflow.reason, PromQLNativeAdmissionReason::GridCellOverflow);
    EXPECT_FALSE(overflow.grid_cells);

    const auto series = evaluatePromQLNativeWorkloadAdmission(2, maximum, 1, 1);
    EXPECT_EQ(series.reason, PromQLNativeAdmissionReason::SeriesLimit);
    EXPECT_FALSE(series.grid_cells);

    const auto exact = evaluatePromQLNativeWorkloadAdmission(1, maximum, 1, maximum);
    EXPECT_EQ(exact.reason, PromQLNativeAdmissionReason::Admitted);
    ASSERT_TRUE(exact.grid_cells);
    EXPECT_EQ(*exact.grid_cells, maximum);
}

TEST(PromQLNativeAdmission, EmptySelectorHasNoGridCells)
{
    const auto empty = evaluatePromQLNativeWorkloadAdmission(0, std::numeric_limits<UInt64>::max(), 0, 1);
    EXPECT_EQ(empty.reason, PromQLNativeAdmissionReason::Admitted);
    ASSERT_TRUE(empty.grid_cells);
    EXPECT_EQ(*empty.grid_cells, 0);

    const auto empty_grid = evaluatePromQLNativeWorkloadAdmission(3, 0, 3, 1);
    EXPECT_EQ(empty_grid.reason, PromQLNativeAdmissionReason::Admitted);
    ASSERT_TRUE(empty_grid.grid_cells);
    EXPECT_EQ(*empty_grid.grid_cells, 0);
}

}
}
