#include <gtest/gtest.h>
#include <Common/DimensionalMetrics.h>
#include <thread>
#include <vector>
#include <set>
#include <algorithm>

using namespace DB::DimensionalMetrics;

TEST(DimensionalMetrics, MetricOperations)
{
    Metric m;
    ASSERT_DOUBLE_EQ(m.get(), 0.0);

    m.set(42.5);
    ASSERT_DOUBLE_EQ(m.get(), 42.5);

    m.increment();
    ASSERT_DOUBLE_EQ(m.get(), 43.5);

    m.increment(1.5);
    ASSERT_DOUBLE_EQ(m.get(), 45.0);

    m.decrement();
    ASSERT_DOUBLE_EQ(m.get(), 44.0);

    m.decrement(4.0);
    ASSERT_DOUBLE_EQ(m.get(), 40.0);
}

TEST(DimensionalMetrics, MetricFamilyBasicLogic)
{
    MetricFamily family({"database", "table"});

    Metric & metric1 = family.withLabels({"db1", "t1"});
    ASSERT_DOUBLE_EQ(metric1.get(), 0.0);
    metric1.increment(15);
    ASSERT_DOUBLE_EQ(metric1.get(), 15.0);

    Metric & metric2 = family.withLabels({"db1", "t1"});
    ASSERT_EQ(&metric1, &metric2);
    ASSERT_DOUBLE_EQ(metric2.get(), 15.0);

    Metric & metric3 = family.withLabels({"db2", "t2"});
    ASSERT_NE(&metric1, &metric3);
    ASSERT_DOUBLE_EQ(metric3.get(), 0.0);
    metric3.set(100);

    auto metrics_map = family.getMetrics();
    ASSERT_EQ(metrics_map.size(), 2);
    ASSERT_TRUE(metrics_map.count({"db1", "t1"}));
    ASSERT_TRUE(metrics_map.count({"db2", "t2"}));
    ASSERT_DOUBLE_EQ(metrics_map.at({"db1", "t1"})->get(), 15.0);
    ASSERT_DOUBLE_EQ(metrics_map.at({"db2", "t2"})->get(), 100.0);
}

TEST(DimensionalMetrics, MetricFamilyConcurrency)
{
    static constexpr int num_tables = 16;
    static constexpr int iterations_per_table = 1000;
    MetricFamily family({"database", "table"});

    std::vector<std::thread> threads;
    threads.reserve(num_tables);
    for (int i = 0; i < num_tables; ++i)
    {
        threads.emplace_back([&family, i]()
        {
            for (int j = 0; j < iterations_per_table; ++j)
            {
                family.withLabels({"db", "t" + std::to_string(i)}).increment();
            }
        });
    }

    for (auto & t : threads)
    {
        t.join();
    }

    auto metrics_map = family.getMetrics();
    ASSERT_EQ(metrics_map.size(), num_tables);

    for (int i = 0; i < num_tables; ++i)
    {
        ASSERT_DOUBLE_EQ(metrics_map.at({"db", "t" + std::to_string(i)})->get(), iterations_per_table);
    }
}

TEST(DimensionalMetrics, FactoryRegistration)
{
    const size_t initial_size = Factory::instance().getRecords().size();

    static constexpr std::string metric_name = "failed_merges";
    MetricFamily & family = Factory::instance().registerMetric(
        metric_name,
        "Number of failed merges.",
        {"database", "table", "error_code_name"}
    );

    auto records = Factory::instance().getRecords();
    ASSERT_EQ(records.size(), initial_size + 1);

    auto it = std::find_if(
        records.begin(),
        records.end(),
        [&](const MetricRecordPtr& record) {
            return record->name == metric_name;
        }
    );
    ASSERT_NE(it, records.end());
    ASSERT_EQ((*it)->family.getLabels().size(), 3);
    ASSERT_EQ(&(*it)->family, &family);
}

TEST(DimensionalMetrics, MetricFamilyInitialization)
{
    const std::vector<LabelValues> initial_values = {
        {"db1", "t1"},
        {"db1", "t2"},
        {"db2", "t1"}
    };

    MetricFamily family({"database", "table"}, initial_values);

    auto metrics_map = family.getMetrics();
    ASSERT_EQ(metrics_map.size(), 3);

    for (const auto & labels : initial_values)
    {
        ASSERT_TRUE(metrics_map.count(labels));
        ASSERT_DOUBLE_EQ(metrics_map.at(labels)->get(), 0.0);
    }

    family.withLabels({"db1", "t2"}).increment(5);
    ASSERT_DOUBLE_EQ(metrics_map.at({"db1", "t2"})->get(), 5.0);
}
