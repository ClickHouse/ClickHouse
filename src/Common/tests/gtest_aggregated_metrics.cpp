#include <Common/AggregatedMetrics.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric Query;
}

TEST(AggregatedMetrics, SingleHandle)
{
    auto handle = std::make_unique<AggregatedMetrics::MetricHandle>(CurrentMetrics::Query);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 0);

    handle->add(5);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 5);

    handle->sub(2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 3);

    handle->set(42);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 42);

    handle.reset();
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 0);
}

TEST(AggregatedMetrics, MultipleHandles)
{
    auto handle1 = std::make_unique<AggregatedMetrics::MetricHandle>(CurrentMetrics::Query);
    auto handle2 = std::make_unique<AggregatedMetrics::MetricHandle>(CurrentMetrics::Query);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 0);

    handle1->add(5);
    handle2->add(10);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 15);

    handle1->sub(4);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 11);

    handle2->set(1);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 2);

    handle1.reset();
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 1);

    handle2.reset();
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::Query), 0);
}
