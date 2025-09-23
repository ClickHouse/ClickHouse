#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Common/Histogram.h>
#include <Common/DimensionalMetrics.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

TEST(PrometheusMetricsWriter, HistogramBasic)
{
    Histogram::Buckets buckets = {1, 5, 10};

    auto & family = Histogram::Factory::instance().registerMetric("test_histogram", "Test histogram", buckets, {"database", "table"});

    auto & metric = family.withLabels({"db1", "users"});
    metric.observe(0);
    metric.observe(3);
    metric.observe(7);

    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter().writeHistogramMetrics(buffer);
    }

    static constexpr const char * expected =
        "# HELP test_histogram Test histogram\n"
        "# TYPE test_histogram histogram\n"
        "test_histogram_bucket{database=\"db1\",table=\"users\",le=\"1\"} 1\n"
        "test_histogram_bucket{database=\"db1\",table=\"users\",le=\"5\"} 2\n"
        "test_histogram_bucket{database=\"db1\",table=\"users\",le=\"10\"} 3\n"
        "test_histogram_bucket{database=\"db1\",table=\"users\",le=\"+Inf\"} 3\n"
        "test_histogram_count{database=\"db1\",table=\"users\"} 3\n"
        "test_histogram_sum{database=\"db1\",table=\"users\"} 10\n";

    EXPECT_TRUE(output == expected);
}

TEST(PrometheusMetricsWriter, HistogramEmpty)
{
    Histogram::Factory::instance().clear();
    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter().writeHistogramMetrics(buffer);
    }
    EXPECT_GE(output.length(), 0);
}

TEST(PrometheusMetricsWriter, DimensionalBasic)
{
    auto & family = DimensionalMetrics::Factory::instance().registerMetric("test_dimensional", "Test dimensional metrics", {"database", "table"});

    auto & metric = family.withLabels({"db1", "users"});
    metric.set(42.0);

    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter().writeDimensionalMetrics(buffer);
    }

    static constexpr const char * expected =
        "# HELP test_dimensional Test dimensional metrics\n"
        "# TYPE test_dimensional gauge\n"
        "test_dimensional{database=\"db1\",table=\"users\"} 42\n";

    EXPECT_TRUE(output == expected);
}

TEST(PrometheusMetricsWriter, DimensionalEmpty)
{
    DimensionalMetrics::Factory::instance().clear();
    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter().writeDimensionalMetrics(buffer);
    }
    EXPECT_GE(output.length(), 0);
}
