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

/// The preallocation hint describes the cardinality of the data, so it must not depend on how many
/// aggregation threads the current query happens to run with.
TEST(HashTablesStatistics, SizeHintIsIndependentOfCurrentTablesCount)
{
    const auto params = makeParams(0xA6610F00ULL);
    /// 8 threads observed ~1M keys each.
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/8);

    for (size_t tables_cnt : {1ul, 2ul, 4ul, 8ul, 16ul, 64ul})
    {
        const auto hint = getSizeHint(params, tables_cnt);
        ASSERT_TRUE(hint.has_value()) << "tables_cnt=" << tables_cnt;
        EXPECT_EQ(hint->median_size, 1'000'000u) << "tables_cnt=" << tables_cnt;
    }
}

/// Fewer threads than during the recording run used to inflate the hint by the ratio of the counts.
TEST(HashTablesStatistics, FewerThreadsDoNotInflateTheHint)
{
    const auto params = makeParams(0xA6610F01ULL);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/8);

    const auto hint = getSizeHint(params, /*tables_cnt=*/1);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->median_size, 1'000'000u);
}

/// The lower limit still guards against a median that under-represents the average, which happens
/// when the threads are loaded unevenly.
TEST(HashTablesStatistics, AverageWinsOverAnUnrepresentativeMedian)
{
    const auto params = makeParams(0xA6610F02ULL);
    /// 8 threads, all but one nearly empty: the median says 1000, the average says 1M.
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1000, /*tables_cnt=*/8);

    const auto hint = getSizeHint(params, /*tables_cnt=*/8);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->median_size, 1'000'000u);
}

/// Nothing is preallocated when `max_size_to_preallocate` cannot accommodate the hint.
TEST(HashTablesStatistics, NoHintWhenPreallocationLimitIsTooSmall)
{
    const StatsCollectingParams params(
        0xA6610F03ULL, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/10000, /*max_size_to_preallocate_=*/1000);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/8);

    EXPECT_FALSE(getSizeHint(params, /*tables_cnt=*/8).has_value());
}

/// Small aggregations are not worth preallocating for.
TEST(HashTablesStatistics, NoHintForSmallAggregations)
{
    const auto params = makeParams(0xA6610F04ULL);
    record(params, /*sum_of_sizes=*/1000, /*median_size=*/125, /*tables_cnt=*/8);

    EXPECT_FALSE(getSizeHint(params, /*tables_cnt=*/8).has_value());
}

/// A recorded entry that carries no table count (e.g. a single unaccounted table) must not divide by zero.
TEST(HashTablesStatistics, ZeroRecordedTablesCountIsSafe)
{
    const auto params = makeParams(0xA6610F05ULL);
    record(params, /*sum_of_sizes=*/8'000'000, /*median_size=*/1'000'000, /*tables_cnt=*/0);

    const auto hint = getSizeHint(params, /*tables_cnt=*/8);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->median_size, 8'000'000u);
}
