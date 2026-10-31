#include <gtest/gtest.h>

#include <algorithm>
#include <vector>

#include <Common/CurrentMetrics.h>

namespace CurrentMetrics
{
    extern const Metric IcebergCompactionCleanupTasks;
}

namespace
{

/// Nothing else in the unit tests changes this metric.
const auto metric = CurrentMetrics::IcebergCompactionCleanupTasks;

}

TEST(CurrentMetricsIncrement, MoveAssignmentReleasesHeldAmount)
{
    const auto before = CurrentMetrics::get(metric);
    {
        CurrentMetrics::Increment held(metric, 2);
        CurrentMetrics::Increment other(metric, 3);
        ASSERT_EQ(CurrentMetrics::get(metric), before + 5);

        held = std::move(other);
        EXPECT_EQ(CurrentMetrics::get(metric), before + 3);
    }
    EXPECT_EQ(CurrentMetrics::get(metric), before);
}

TEST(CurrentMetricsIncrement, EraseFromVector)
{
    const auto before = CurrentMetrics::get(metric);
    {
        std::vector<CurrentMetrics::Increment> increments;
        for (size_t i = 0; i < 10; ++i)
            increments.emplace_back(metric);
        ASSERT_EQ(CurrentMetrics::get(metric), before + 10);

        /// Erasing from the front move-assigns the remaining elements over the erased ones.
        size_t index = 0;
        std::erase_if(increments, [&](const CurrentMetrics::Increment &) { return index++ < 4; });
        EXPECT_EQ(increments.size(), 6u);
        EXPECT_EQ(CurrentMetrics::get(metric), before + 6);
    }
    EXPECT_EQ(CurrentMetrics::get(metric), before);
}
