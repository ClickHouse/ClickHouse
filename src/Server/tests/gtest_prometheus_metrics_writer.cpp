#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Common/Histogram.h>
#include <Common/DimensionalMetrics.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

TEST(PrometheusMetricsWriter, HistogramBasic)
{
    Histogram::MetricFamily family("test_histogram_gtest", "Test histogram", {1, 5, 10}, {"database", "table"});

    auto & metric1 = family.withLabels({"db1", "users"});
    metric1.observe(0);
    metric1.observe(3);
    metric1.observe(7);

    auto & metric2 = family.withLabels({"db2", "posts"});
    metric2.observe(2);
    metric2.observe(6);

    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter::writeHistogramMetric(buffer, family);
    }

    static constexpr const char * expected =
        "# HELP test_histogram_gtest Test histogram\n"
        "# TYPE test_histogram_gtest histogram\n"
        "test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"1\"} 1\n"
        "test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"5\"} 2\n"
        "test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"10\"} 3\n"
        "test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"+Inf\"} 3\n"
        "test_histogram_gtest_count{database=\"db1\",table=\"users\"} 3\n"
        "test_histogram_gtest_sum{database=\"db1\",table=\"users\"} 10\n"
        "test_histogram_gtest_bucket{database=\"db2\",table=\"posts\",le=\"1\"} 0\n"
        "test_histogram_gtest_bucket{database=\"db2\",table=\"posts\",le=\"5\"} 1\n"
        "test_histogram_gtest_bucket{database=\"db2\",table=\"posts\",le=\"10\"} 2\n"
        "test_histogram_gtest_bucket{database=\"db2\",table=\"posts\",le=\"+Inf\"} 2\n"
        "test_histogram_gtest_count{database=\"db2\",table=\"posts\"} 2\n"
        "test_histogram_gtest_sum{database=\"db2\",table=\"posts\"} 8\n";

    EXPECT_EQ(expected, output);
}

TEST(PrometheusMetricsWriter, DimensionalBasic)
{
    DimensionalMetrics::MetricFamily family("test_dimensional_gtest", "Test dimensional metrics", {"database", "table"});

    auto & metric1 = family.withLabels({"db1", "users"});
    metric1.set(42.0);

    auto & metric2 = family.withLabels({"db2", "posts"});
    metric2.set(17.5);

    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter::writeDimensionalMetric(buffer, family);
    }

    static constexpr const char * expected =
        "# HELP test_dimensional_gtest Test dimensional metrics\n"
        "# TYPE test_dimensional_gtest gauge\n"
        "test_dimensional_gtest{database=\"db1\",table=\"users\"} 42\n"
        "test_dimensional_gtest{database=\"db2\",table=\"posts\"} 17.5\n";

    EXPECT_EQ(expected, output);
}
