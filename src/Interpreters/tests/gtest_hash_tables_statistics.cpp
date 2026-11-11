#include <gtest/gtest.h>

#include <Interpreters/HashTablesStatistics.h>

using namespace DB;

namespace
{

template <typename Entry>
Entry makeEntry(size_t size);

template <>
AggregationEntry makeEntry<AggregationEntry>(size_t size)
{
    return {.sum_of_sizes = size, .median_size = size};
}

template <>
HashJoinEntry makeEntry<HashJoinEntry>(size_t size)
{
    return {.ht_size = size, .source_rows = size};
}

/// The capacity of the process-wide statistics cache is fixed when the first query that touches it
/// creates it. A caller that keeps no entries (`max_entries_for_hash_table_stats = 0`) must not be
/// the one to create it: a zero-capacity cache drops every insertion and misses every lookup, so it
/// would silently disable the statistics - and with them hash-table preallocation - for every later
/// query in the process, whatever the settings of that query.
template <typename Entry>
void checkZeroMaxEntriesKeepsStatisticsUsableForOtherQueries()
{
    HashTablesStatistics<Entry> stats;

    const StatsCollectingParams keeps_nothing{
        /*key_=*/1, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/0, /*max_size_to_preallocate_=*/100'000};
    stats.update(makeEntry<Entry>(1000), keeps_nothing);
    EXPECT_FALSE(stats.getSizeHint(keeps_nothing).has_value());
    EXPECT_FALSE(stats.getCacheStats().has_value()) << "the cache must not have been created with zero capacity";

    const StatsCollectingParams keeps_entries{
        /*key_=*/1, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/10, /*max_size_to_preallocate_=*/100'000};
    EXPECT_FALSE(stats.getSizeHint(keeps_entries).has_value());
    stats.update(makeEntry<Entry>(1000), keeps_entries);

    const auto hint = stats.getSizeHint(keeps_entries);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->dump(), makeEntry<Entry>(1000).dump());
}

}

TEST(HashTablesStatistics, ZeroMaxEntriesKeepsAggregationStatisticsUsable)
{
    checkZeroMaxEntriesKeepsStatisticsUsableForOtherQueries<AggregationEntry>();
}

TEST(HashTablesStatistics, ZeroMaxEntriesKeepsJoinStatisticsUsable)
{
    checkZeroMaxEntriesKeepsStatisticsUsableForOtherQueries<HashJoinEntry>();
}

/// A join whose build side an outside runtime filter reduces records its observed size but not an
/// unfiltered one. The unfiltered size recorded by an earlier run stays, and join ordering reads only it.
TEST(HashTablesStatistics, FilteredJoinRunKeepsUnfilteredSize)
{
    HashTablesStatistics<HashJoinEntry> stats;
    const StatsCollectingParams params{
        /*key_=*/1, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/10, /*max_size_to_preallocate_=*/100'000};

    auto unfiltered_mark = std::make_shared<JoinBuildInputMark>();
    auto filtered_mark = std::make_shared<JoinBuildInputMark>(JoinBuildInputMark{.filtered_from_outside = true});
    const HashJoinStatsCollectingParams unfiltered_run{.build = params, .match = {}, .build_input_mark = unfiltered_mark};
    const HashJoinStatsCollectingParams filtered_run{.build = params, .match = {}, .build_input_mark = filtered_mark};

    stats.update(makeHashJoinEntry(1000, 1000, unfiltered_run), params);
    stats.update(makeHashJoinEntry(50, 50, filtered_run), params);

    auto hint = stats.getSizeHint(params);
    ASSERT_TRUE(hint.has_value());
    EXPECT_EQ(hint->source_rows, 50);
    EXPECT_EQ(hint->unfiltered_source_rows, 1000);

    /// A first run under a filter leaves no unfiltered size, and the next unfiltered run sets it even
    /// though its hash table is about the same size.
    const StatsCollectingParams other_key{
        /*key_=*/2, /*enable_=*/true, /*max_entries_for_hash_table_stats_=*/10, /*max_size_to_preallocate_=*/100'000};
    const HashJoinStatsCollectingParams filtered_other{.build = other_key, .match = {}, .build_input_mark = filtered_mark};
    const HashJoinStatsCollectingParams unfiltered_other{.build = other_key, .match = {}, .build_input_mark = unfiltered_mark};
    stats.update(makeHashJoinEntry(100, 100, filtered_other), other_key);
    EXPECT_FALSE(stats.getSizeHint(other_key)->unfiltered_source_rows.has_value());
    stats.update(makeHashJoinEntry(90, 90, unfiltered_other), other_key);
    EXPECT_EQ(stats.getSizeHint(other_key)->unfiltered_source_rows, 90);
}
