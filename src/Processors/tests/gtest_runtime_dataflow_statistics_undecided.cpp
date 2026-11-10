#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/RuntimeDataflowStatistics.h>

#include <random>

using namespace DB;

namespace ProfileEvents
{
extern const Event AutoParallelReplicasNoStatistics;
}

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

}

/// A collection that a step could not take part in is remembered, so that the plan is not built again for it, and a
/// collection that succeeds later forgets it. Both are recorded from the destructor of the block.
TEST(RuntimeDataflowStatisticsUndecided, FailedCollectionIsRememberedUntilOneSucceeds)
{
    const UInt64 undecided_key = freshKey();
    const UInt64 statistics_key = freshKey();
    auto & cache = getRuntimeDataflowStatisticsCache();
    {
        RuntimeDataflowStatisticsBlock failed(statistics_key, /*total_rows_to_read_=*/100, undecided_key);
        failed.unsupported_case = true;
    }
    const auto undecided = cache.getUndecided(undecided_key);
    ASSERT_NE(undecided, nullptr);
    EXPECT_EQ(undecided->outcome_event, ProfileEvents::AutoParallelReplicasNoStatistics);

    {
        RuntimeDataflowStatisticsBlock succeeded(statistics_key, /*total_rows_to_read_=*/100, undecided_key);
        measureSomething(succeeded);
    }
    EXPECT_TRUE(cache.getStats(statistics_key));
    EXPECT_EQ(cache.getUndecided(undecided_key), nullptr);
}
