#include <Common/AggregatedMetrics.h>

#include <gtest/gtest.h>

namespace CurrentMetrics
{
    extern const Metric Query;
    extern const Metric SharedMergeTreeMaxReplicas;
    extern const Metric SharedMergeTreeMinReplicas;
}

TEST(AggregatedMetrics, GlobalSumSingleHandle)
{
    auto handle = std::make_unique<AggregatedMetrics::GlobalSum>(CurrentMetrics::Query);
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

TEST(AggregatedMetrics, GlobalSumMultipleHandles)
{
    auto handle1 = std::make_unique<AggregatedMetrics::GlobalSum>(CurrentMetrics::Query);
    auto handle2 = std::make_unique<AggregatedMetrics::GlobalSum>(CurrentMetrics::Query);
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

TEST(AggregatedMetrics, GlobalQuantileSingleHandleMax)
{
    auto handle = std::make_unique<AggregatedMetrics::GlobalQuantile>(CurrentMetrics::SharedMergeTreeMaxReplicas);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 0);

    handle->set(5);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 5);

    handle->set(10);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 10);

    handle->set(3);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 3);

    handle.reset();
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 0);
}

TEST(AggregatedMetrics, GlobalQuantileSingleHandleMin)
{
    auto handle = std::make_unique<AggregatedMetrics::GlobalQuantile>(CurrentMetrics::SharedMergeTreeMinReplicas);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 0);

    handle->set(3);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 3);

    handle->set(10);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 10);

    handle->set(5);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 5);

    handle.reset();
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 0);
}

TEST(AggregatedMetrics, GlobalQuantileMultipleHandlesMax)
{
    auto handle1 = std::make_unique<AggregatedMetrics::GlobalQuantile>(CurrentMetrics::SharedMergeTreeMaxReplicas);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 0);

    handle1->set(5);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 5);

    auto handle2 = std::make_unique<AggregatedMetrics::GlobalQuantile>(CurrentMetrics::SharedMergeTreeMaxReplicas);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 5);

    handle2->set(3);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 5);

    handle1->set(2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 3);

    handle1->set(1);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 3);

    handle2->set(10);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 10);

    handle1->set(7);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMaxReplicas), 10);
}

TEST(AggregatedMetrics, GlobalQuantileMultipleHandlesMin)
{
    auto handle1 = std::make_unique<AggregatedMetrics::GlobalQuantile>(CurrentMetrics::SharedMergeTreeMinReplicas);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 0);

    handle1->set(5);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 5);

    auto handle2 = std::make_unique<AggregatedMetrics::GlobalQuantile>(CurrentMetrics::SharedMergeTreeMinReplicas);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 0);

    handle2->set(7);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 5);

    handle1->set(2);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 2);

    handle1->set(1);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 1);

    handle2->set(5);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 1);

    handle1->set(9);
    ASSERT_EQ(CurrentMetrics::get(CurrentMetrics::SharedMergeTreeMinReplicas), 5);
}
