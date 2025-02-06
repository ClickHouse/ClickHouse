#include <atomic>
#include <gtest/gtest.h>

#include <Common/Labels.h>
#include <Common/HistogramMetrics.h>

constexpr HistogramMetrics::Metric metric = HistogramMetrics::KeeperResponseTime;
constexpr Labels::KeeperOperation label = Labels::KeeperOperation::Create;

const std::string metric_name = "KeeperResponseTime";
const std::string label_name = "KeeperOperation";
const std::string label_value = "Create";

using DataHolder = HistogramMetrics::MetricDataHolder<metric, label>;

class HistogramMetricsObserveTest : public ::testing::Test {
protected:
    void SetUp() override {
        std::fill(std::begin(DataHolder::counters), std::end(DataHolder::counters), 0);
        DataHolder::sum = 0;
    }
};

TEST_F(HistogramMetricsObserveTest, BelowOne) {
    HistogramMetrics::observe<metric, label>(0);
    EXPECT_EQ(DataHolder::counters[0], 1);
    EXPECT_EQ(DataHolder::sum, 0);
}

TEST_F(HistogramMetricsObserveTest, ExactlyOne) {
    HistogramMetrics::observe<metric, label>(1);
    EXPECT_EQ(DataHolder::counters[0], 1);
    EXPECT_EQ(DataHolder::sum, 1);
}

TEST_F(HistogramMetricsObserveTest, BelowAndEqualToFive) {
    HistogramMetrics::observe<metric, label>(3);
    HistogramMetrics::observe<metric, label>(4);
    HistogramMetrics::observe<metric, label>(5);
    EXPECT_EQ(DataHolder::counters[2], 3);
    EXPECT_EQ(DataHolder::sum, 3 + 4 + 5);
}

TEST_F(HistogramMetricsObserveTest, AboveAllBuckets) {
    HistogramMetrics::observe<metric, label>(1000);
    EXPECT_EQ(DataHolder::counters.back(), 1);
    EXPECT_EQ(DataHolder::sum, 1000);
}

TEST(HistogramMetricsCollect, Ok) {
    const auto & descriptors = HistogramMetrics::collect();
    
    const auto it = std::find_if(
        descriptors.begin(),
        descriptors.end(),
        [](const HistogramMetrics::MetricDescriptor& elem)
        {
            return elem.name == metric_name && elem.label == std::pair{label_name, label_value};
        }
    );
    EXPECT_NE(it, descriptors.end());
    const auto & descriptor = *it;

    HistogramMetrics::observe<metric, label>(1000);
    
    EXPECT_EQ(descriptor.sum->load(std::memory_order_relaxed), 1000);
    EXPECT_EQ(descriptor.counters.back(), 1);
}
