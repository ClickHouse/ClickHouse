#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <random>

using namespace DB;

namespace
{

/// The cache is process-wide, so every test takes keys of its own.
UInt64 freshKey()
{
    static std::mt19937_64 generator{std::random_device{}()};
    UInt64 key = 0;
    while (key == 0)
        key = generator();
    return key;
}

/// A block whose destruction publishes statistics, as a collection that measured something does.
void measureSomething(RuntimeDataflowStatisticsBlock & block)
{
    auto & output = block.output_bytes_statistics[RuntimeDataflowStatisticsBlock::OutputChunk];
    std::lock_guard lock(output.mutex);
    output.bytes = 1000;
    output.sample_bytes = 1000;
    output.compressed_bytes = 100;
}

using Reason = RuntimeDataflowStatisticsCache::UndecidedReason;

}

/// A collection that a step could not take part in is remembered, so the plan is not built again for it.
TEST(RuntimeDataflowStatisticsUndecided, CollectorUnsupportedIsRemembered)
{
    const UInt64 undecided_key = freshKey();
    const UInt64 statistics_key = freshKey();
    {
        RuntimeDataflowStatisticsBlock failed(statistics_key, /*total_rows_to_read_=*/100, undecided_key);
        failed.unsupported_case = true;
    }

    const auto undecided = getRuntimeDataflowStatisticsCache().getUndecided(undecided_key);
    ASSERT_NE(undecided, nullptr);
    EXPECT_EQ(undecided->reason, Reason::CollectorUnsupported);
}

/// A collection that succeeds forgets what an earlier one could not collect.
TEST(RuntimeDataflowStatisticsUndecided, SuccessfulCollectionForgetsFailure)
{
    const UInt64 undecided_key = freshKey();
    const UInt64 statistics_key = freshKey();
    auto & cache = getRuntimeDataflowStatisticsCache();
    {
        RuntimeDataflowStatisticsBlock failed(statistics_key, /*total_rows_to_read_=*/100, undecided_key);
        failed.unsupported_case = true;
    }
    ASSERT_NE(cache.getUndecided(undecided_key), nullptr);

    {
        RuntimeDataflowStatisticsBlock succeeded(statistics_key, /*total_rows_to_read_=*/100, undecided_key);
        measureSomething(succeeded);
    }
    EXPECT_TRUE(cache.getStats(statistics_key));
    EXPECT_EQ(cache.getUndecided(undecided_key), nullptr);
}
