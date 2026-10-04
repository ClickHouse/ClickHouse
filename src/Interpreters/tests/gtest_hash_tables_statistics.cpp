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
