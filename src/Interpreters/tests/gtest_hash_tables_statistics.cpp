#include <Interpreters/HashTablesStatistics.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

/// The statistics cache is a process-wide singleton, so every test uses its own key.
StatsCollectingParams makeParams(UInt64 key)
{
    return StatsCollectingParams(
        key, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/10000, /*max_size_to_preallocate_=*/1'000'000'000);
}

void record(const StatsCollectingParams & params, size_t sum_of_sizes, size_t median_size, size_t tables_cnt)
{
    getHashTablesStatistics<AggregationEntry>().update(
        {.sum_of_sizes = sum_of_sizes, .median_size = median_size, .tables_cnt = tables_cnt}, params);
}

}

TEST(HashTablesStatistics, SizeHintIsIndependentOfCurrentTablesCount)
{
    const auto params = makeParams(0xA6610F00ULL);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/8);

    for (size_t tables_cnt : {1ul, 2ul, 4ul, 8ul, 16ul, 64ul})
    {
        const auto hint = getSizeHint(params, tables_cnt);
        ASSERT_TRUE(hint.has_value()) << "tables_cnt=" << tables_cnt;
        EXPECT_EQ(hint->median_size, 1'000'000u) << "tables_cnt=" << tables_cnt;
    }
}

TEST(HashTablesStatistics, FewerThreadsDoNotInflateTheHint)
{
    const auto params = makeParams(0xA6610F01ULL);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/8);

    const auto hint = getSizeHint(params, /*tables_cnt=*/1);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->median_size, 1'000'000u);
}

TEST(HashTablesStatistics, AverageWinsOverAnUnrepresentativeMedian)
{
    const auto params = makeParams(0xA6610F02ULL);
    /// All but one table nearly empty: the median says 1000, the average says 1M.
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1000, /*tables_cnt=*/8);

    const auto hint = getSizeHint(params, /*tables_cnt=*/8);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->median_size, 1'000'000u);
}

TEST(HashTablesStatistics, NoHintWhenPreallocationLimitIsTooSmall)
{
    const StatsCollectingParams params(
        0xA6610F03ULL, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/10000, /*max_size_to_preallocate_=*/1000);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/8);

    EXPECT_FALSE(getSizeHint(params, /*tables_cnt=*/8).has_value());
}

TEST(HashTablesStatistics, NoHintForSmallAggregations)
{
    const auto params = makeParams(0xA6610F04ULL);
    record(params, /*sum_of_sizes=*/1000, /*median_size=*/125, /*tables_cnt=*/8);

    EXPECT_FALSE(getSizeHint(params, /*tables_cnt=*/8).has_value());
}

TEST(HashTablesStatistics, ZeroRecordedTablesCountIsSafe)
{
    const auto params = makeParams(0xA6610F05ULL);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/0);

    const auto hint = getSizeHint(params, /*tables_cnt=*/8);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->median_size, 8'000'000u);
}
