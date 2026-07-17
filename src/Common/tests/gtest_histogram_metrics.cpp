#include <gtest/gtest.h>
#include <Common/Histogram.h>

using namespace DB::Histogram;

const Buckets test_buckets = {1, 5, 10};

MetricFamily & family = Factory::instance().registerMetric(
    "keeper_response_time",
    "Keeper response time",
    test_buckets,
    {"operation"}
);
Metric & metric = family.withLabels({"create"});

/// (-Inf,1] bucket is index 0
TEST(HistogramMetricsTest, ObserveBelowFirstBucket)
{
    metric.observe(0);
    EXPECT_EQ(metric.getCounter(0), 1);
}

/// (1,5] bucket is index 1
TEST(HistogramMetricsTest, ObserveMultipleValues)
{
    metric.observe(3);
    metric.observe(4);
    metric.observe(5);
    EXPECT_EQ(metric.getCounter(1), 3);
}

/// (10, +Inf) bucket is the last one
TEST(HistogramMetricsTest, ObserveAboveAllBuckets)
{
    metric.observe(1000);
    EXPECT_EQ(metric.getCounter(test_buckets.size()), 1);
}
