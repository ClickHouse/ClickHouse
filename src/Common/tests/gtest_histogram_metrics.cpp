#include <gtest/gtest.h>
#include <Common/HistogramMetrics.h>
#include <Common/DimensionalMetrics.h>
#include <thread>

using namespace HistogramMetrics;

const Buckets test_buckets = {1, 5, 10};

MetricFamily & family = Factory::instance().registerMetric(
    "my_metrics",
    "My description",
    test_buckets,
    {"my_label"}
);
Metric & metric = family.withLabels({"my_label_value"});

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

/// Stress test: concurrent getOrCreate (emit) vs removeWhere (prune).
/// Under ASan or TSan the old unique_ptr<Metric> storage would produce a
/// use-after-free here; shared_ptr keeps the Metric alive until the last
/// observer releases it, so any write that lands on a just-pruned entry is
/// merely orphaned — no UB.
TEST(HistogramMetricsTest, ConcurrentEmitAndRemove)
{
    static HistogramMetrics::MetricFamily & race_family =
        HistogramMetrics::Factory::instance().registerMetric(
            "race_test_histogram",
            "Stress test for concurrent getOrCreate vs removeWhere",
            test_buckets,
            {"key"});

    static DimensionalMetrics::MetricFamily & race_dim_family =
        DimensionalMetrics::Factory::instance().registerMetric(
            "race_test_dimensional",
            "Stress test for concurrent getOrCreate vs removeWhere",
            {"key"});

    constexpr int kIterations = 50'000;

    auto emitter = std::thread([&]
    {
        for (int i = 0; i < kIterations; ++i)
        {
            race_family.getOrCreate({"val"})->observe(1.0);
            race_dim_family.getOrCreate({"val"})->increment();
        }
    });

    auto pruner = std::thread([&]
    {
        for (int i = 0; i < kIterations / 100; ++i)
        {
            race_family.removeWhere([](const auto &) { return true; });
            race_dim_family.removeWhere([](const auto &) { return true; });
        }
    });

    emitter.join();
    pruner.join();
    /// Reaching here without a crash or sanitizer report is the assertion.
}
