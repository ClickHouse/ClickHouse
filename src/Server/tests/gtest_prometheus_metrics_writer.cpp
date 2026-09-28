#include <base/scope_guard.h>
#include <gtest/gtest.h>
#include <Server/PrometheusMetricsWriter.h>
#include <Common/HistogramMetrics.h>
#include <Common/DimensionalMetrics.h>
#include <IO/WriteBufferFromString.h>

using namespace DB;

TEST(PrometheusMetricsWriter, HistogramBasic)
{
    HistogramMetrics::MetricFamily family("test_histogram_gtest", "Test histogram", {1, 5, 10}, {"database", "table"});

    auto & metric = family.withLabels({"db1", "users"});
    metric.observe(0);
    metric.observe(3);
    metric.observe(7);

    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter::writeHistogramMetric(buffer, family);
    }

    static constexpr const char * expected =
        "# HELP ClickHouseHistogramMetrics_test_histogram_gtest Test histogram\n"
        "# TYPE ClickHouseHistogramMetrics_test_histogram_gtest histogram\n"
        "ClickHouseHistogramMetrics_test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"1\"} 1\n"
        "ClickHouseHistogramMetrics_test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"5\"} 2\n"
        "ClickHouseHistogramMetrics_test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"10\"} 3\n"
        "ClickHouseHistogramMetrics_test_histogram_gtest_bucket{database=\"db1\",table=\"users\",le=\"+Inf\"} 3\n"
        "ClickHouseHistogramMetrics_test_histogram_gtest_count{database=\"db1\",table=\"users\"} 3\n"
        "ClickHouseHistogramMetrics_test_histogram_gtest_sum{database=\"db1\",table=\"users\"} 10\n";

    EXPECT_EQ(expected, output);
}

TEST(PrometheusMetricsWriter, DimensionalBasic)
{
    DimensionalMetrics::MetricFamily family("test_dimensional_gtest", "Test dimensional metrics", {"database", "table"});

    auto & metric = family.withLabels({"db1", "users"});
    metric.set(42.0);

    std::string output;
    {
        WriteBufferFromString buffer(output);
        PrometheusMetricsWriter::writeDimensionalMetric(buffer, family);
    }

    static constexpr const char * expected =
        "# HELP ClickHouseDimensionalMetrics_test_dimensional_gtest Test dimensional metrics\n"
        "# TYPE ClickHouseDimensionalMetrics_test_dimensional_gtest gauge\n"
        "ClickHouseDimensionalMetrics_test_dimensional_gtest{database=\"db1\",table=\"users\"} 42\n";

    EXPECT_EQ(expected, output);
}
